// 本文件测试实时 K 线中枢：差分规则、轮询共享与扇出、退订清理、上限与慢消费者处理。
package live

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedFetcher 按脚本依次返回结果，脚本耗尽后重复最后一项，并统计调用次数。
type scriptedFetcher struct {
	mu    sync.Mutex
	steps []fetchStep
	calls atomic.Int32
}

// fetchStep 一次拉取的返回值。
type fetchStep struct {
	bars []Bar
	err  error
}

// fetch 实现 FetchFunc。
func (f *scriptedFetcher) fetch(context.Context) ([]Bar, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	step := f.steps[0]
	if len(f.steps) > 1 {
		f.steps = f.steps[1:]
	}
	return step.bars, step.err
}

// bar 构造测试 K 线。
func bar(ts int64, close float64) Bar {
	return Bar{Timestamp: ts, Open: 10, High: close + 1, Low: 9, Close: close}
}

// testConfig 返回毫秒级节拍的测试参数。
func testConfig() Config {
	return Config{
		OpenInterval:     5 * time.Millisecond,
		ClosedInterval:   time.Hour,
		ErrorInterval:    5 * time.Millisecond,
		MaxSubscriptions: 8,
		MaxSeries:        4,
		SnapshotSize:     2,
		BufferSize:       16,
	}
}

// nextEvent 在超时内读取一条事件。
func nextEvent(t *testing.T, sub *Subscription) Event {
	t.Helper()
	select {
	case ev := <-sub.Events():
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

// expectNoEvent 确认短时间内没有新事件。
func expectNoEvent(t *testing.T, sub *Subscription, wait time.Duration) {
	t.Helper()
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event %#v", ev)
	case <-time.After(wait):
	}
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

// 验证同一根 K 线数值变化输出 forming，数值不变不输出。
func TestDiffTailFormingOnlyOnChange(t *testing.T) {
	prev := []Bar{bar(1, 10), bar(2, 11)}
	if got := diffTail(prev, []Bar{bar(1, 10), bar(2, 11)}, 2); len(got) != 0 {
		t.Fatalf("unchanged tail produced %#v", got)
	}
	got := diffTail(prev, []Bar{bar(1, 10), bar(2, 12)}, 2)
	if len(got) != 1 || got[0].Type != EventForming || got[0].Bar.Close != 12 {
		t.Fatalf("changed tail = %#v, want one forming close=12", got)
	}
}

// 验证可选成交量变化同样视为变化。
func TestDiffTailDetectsVolumeChange(t *testing.T) {
	v1, v2 := 100.0, 120.0
	a, b := bar(2, 11), bar(2, 11)
	a.Volume, b.Volume = &v1, &v2
	got := diffTail([]Bar{a}, []Bar{b}, 2)
	if len(got) != 1 || got[0].Type != EventForming {
		t.Fatalf("volume change = %#v, want forming", got)
	}
}

// 验证恰好跨过一根时先输出上一根的终值 closed，再输出新一根 forming。
func TestDiffTailClosedTransition(t *testing.T) {
	prev := []Bar{bar(1, 10), bar(2, 11)}
	got := diffTail(prev, []Bar{bar(1, 10), bar(2, 11.5), bar(3, 12)}, 2)
	if len(got) != 2 || got[0].Type != EventClosed || got[1].Type != EventForming {
		t.Fatalf("transition = %#v, want closed+forming", got)
	}
	if got[0].Bar.Timestamp != 2 || got[0].Bar.Close != 11.5 {
		t.Fatalf("closed bar = %#v, want final value of ts=2", got[0].Bar)
	}
	if got[1].Bar.Timestamp != 3 {
		t.Fatalf("forming bar = %#v, want ts=3", got[1].Bar)
	}
}

// 验证跳过多根或时间回退时改发快照，避免前端只暂存一根 closed 造成缺口。
func TestDiffTailGapOrRewindSendsSnapshot(t *testing.T) {
	prev := []Bar{bar(1, 10), bar(2, 11)}
	gap := diffTail(prev, []Bar{bar(3, 10), bar(4, 12), bar(5, 13)}, 2)
	if len(gap) != 1 || gap[0].Type != EventSnapshot || len(gap[0].Bars) != 2 || gap[0].Bars[1].Timestamp != 5 {
		t.Fatalf("gap = %#v, want snapshot of last 2", gap)
	}
	rewind := diffTail(prev, []Bar{bar(1, 10)}, 2)
	if len(rewind) != 1 || rewind[0].Type != EventSnapshot {
		t.Fatalf("rewind = %#v, want snapshot", rewind)
	}
}

// 验证首个订阅者先收到尾部快照与 live 状态，随后按变化收到 forming、closed+forming。
func TestHubEmitsSnapshotThenChanges(t *testing.T) {
	f := &scriptedFetcher{steps: []fetchStep{
		{bars: []Bar{bar(1, 10), bar(2, 11), bar(3, 12)}},
		{bars: []Bar{bar(1, 10), bar(2, 11), bar(3, 12)}},
		{bars: []Bar{bar(1, 10), bar(2, 11), bar(3, 13)}},
		{bars: []Bar{bar(2, 11), bar(3, 13.5), bar(4, 14)}},
	}}
	hub := NewHub(testConfig())
	sub, err := hub.Subscribe("k", Source{Fetch: f.fetch})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	snap := nextEvent(t, sub)
	if snap.Type != EventSnapshot || len(snap.Bars) != 2 || snap.Bars[0].Timestamp != 2 {
		t.Fatalf("first event = %#v, want snapshot of last 2 bars", snap)
	}
	if st := nextEvent(t, sub); st.Type != EventStatus || st.Status != StatusLive {
		t.Fatalf("second event = %#v, want live status", st)
	}
	if ev := nextEvent(t, sub); ev.Type != EventForming || ev.Bar.Close != 13 {
		t.Fatalf("third event = %#v, want forming close=13 (unchanged poll skipped)", ev)
	}
	closed, forming := nextEvent(t, sub), nextEvent(t, sub)
	if closed.Type != EventClosed || closed.Bar.Timestamp != 3 || closed.Bar.Close != 13.5 {
		t.Fatalf("closed = %#v, want final ts=3 close=13.5", closed)
	}
	if forming.Type != EventForming || forming.Bar.Timestamp != 4 {
		t.Fatalf("forming = %#v, want ts=4", forming)
	}
	expectNoEvent(t, sub, 30*time.Millisecond)
}

// 验证同一 key 的多个订阅者共享一个轮询器，后来者立即收到缓存快照，事件扇出到所有人。
func TestHubSharesPollerAcrossSubscribers(t *testing.T) {
	f := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 10), bar(2, 11)}}}}
	other := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 99)}}}}
	hub := NewHub(testConfig())
	first, err := hub.Subscribe("k", Source{Fetch: f.fetch})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	nextEvent(t, first) // snapshot
	nextEvent(t, first) // status

	second, err := hub.Subscribe("k", Source{Fetch: other.fetch})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	snap := nextEvent(t, second)
	if snap.Type != EventSnapshot || snap.Bars[len(snap.Bars)-1].Close != 11 {
		t.Fatalf("late subscriber snapshot = %#v, want cached tail of shared poller", snap)
	}
	if series, subs := hub.Stats(); series != 1 || subs != 2 {
		t.Fatalf("stats = %d series %d subs, want 1/2", series, subs)
	}
	if other.calls.Load() != 0 {
		t.Fatalf("second source fetched %d times, want shared poller only", other.calls.Load())
	}

	f.mu.Lock()
	f.steps = []fetchStep{{bars: []Bar{bar(1, 10), bar(2, 15)}}}
	f.mu.Unlock()
	nextEvent(t, second) // status
	for _, sub := range []*Subscription{first, second} {
		if ev := nextEvent(t, sub); ev.Type != EventForming || ev.Bar.Close != 15 {
			t.Fatalf("fan-out event = %#v, want forming close=15", ev)
		}
	}
}

// 验证最后一个订阅者退订后轮询器停止并从中枢移除，且不再访问上游。
func TestHubStopsPollerWhenLastSubscriberLeaves(t *testing.T) {
	f := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 10)}}}}
	hub := NewHub(testConfig())
	a, _ := hub.Subscribe("k", Source{Fetch: f.fetch})
	b, _ := hub.Subscribe("k", Source{Fetch: f.fetch})
	nextEvent(t, a)
	p := a.poller

	a.Close()
	a.Close() // 重复退订无副作用
	if series, subs := hub.Stats(); series != 1 || subs != 1 {
		t.Fatalf("after first close stats = %d/%d, want 1/1", series, subs)
	}
	b.Close()
	if series, subs := hub.Stats(); series != 0 || subs != 0 {
		t.Fatalf("after last close stats = %d/%d, want 0/0", series, subs)
	}
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller goroutine did not exit")
	}
	calls := f.calls.Load()
	time.Sleep(20 * time.Millisecond)
	if f.calls.Load() != calls {
		t.Fatal("upstream fetched after poller stopped")
	}
}

// 验证上游失败只广播一次 upstream-error，恢复后补发快照并回到 live。
func TestHubUpstreamErrorThenRecovery(t *testing.T) {
	boom := errors.New("tdx down")
	f := &scriptedFetcher{steps: []fetchStep{
		{err: boom}, {err: boom}, {err: boom},
		{bars: []Bar{bar(1, 10), bar(2, 11)}},
	}}
	hub := NewHub(testConfig())
	sub, _ := hub.Subscribe("k", Source{Fetch: f.fetch})
	defer sub.Close()

	ev := nextEvent(t, sub)
	if ev.Type != EventStatus || ev.Status != StatusUpstreamError || ev.Detail != "tdx down" {
		t.Fatalf("first event = %#v, want upstream-error", ev)
	}
	if snap := nextEvent(t, sub); snap.Type != EventSnapshot {
		t.Fatalf("after recovery = %#v, want snapshot (error not repeated)", snap)
	}
	if st := nextEvent(t, sub); st.Status != StatusLive {
		t.Fatalf("status = %#v, want live", st)
	}
}

// 验证休市时发 market-closed 状态并按休市间隔降频，不在每个节拍访问上游。
func TestHubBacksOffWhenMarketClosed(t *testing.T) {
	f := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 10)}}}}
	hub := NewHub(testConfig())
	closed := func(time.Time, []Bar) (bool, string) { return false, "weekend" }
	sub, _ := hub.Subscribe("k", Source{Fetch: f.fetch, Session: closed})
	defer sub.Close()

	nextEvent(t, sub) // snapshot
	st := nextEvent(t, sub)
	if st.Status != StatusMarketClosed || st.Detail != "weekend" {
		t.Fatalf("status = %#v, want market-closed weekend", st)
	}
	time.Sleep(40 * time.Millisecond)
	if calls := f.calls.Load(); calls != 1 {
		t.Fatalf("fetch calls while closed = %d, want 1", calls)
	}
}

// 验证休市转开市时立即拉取并广播 live，无需等待休市间隔。
func TestHubPollsImmediatelyWhenMarketOpens(t *testing.T) {
	f := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 10)}}}}
	var open atomic.Bool
	session := func(time.Time, []Bar) (bool, string) {
		if open.Load() {
			return true, ""
		}
		return false, "closed"
	}
	hub := NewHub(testConfig())
	sub, _ := hub.Subscribe("k", Source{Fetch: f.fetch, Session: session})
	defer sub.Close()
	nextEvent(t, sub)
	nextEvent(t, sub)

	open.Store(true)
	if st := nextEvent(t, sub); st.Status != StatusLive {
		t.Fatalf("status = %#v, want live after open", st)
	}
	waitFor(t, func() bool { return f.calls.Load() >= 2 })
}

// 验证全局订阅数与序列数上限。
func TestHubBoundsSubscriptionsAndSeries(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions, cfg.MaxSeries = 3, 2
	f := &scriptedFetcher{steps: []fetchStep{{bars: []Bar{bar(1, 10)}}}}
	hub := NewHub(cfg)
	a, _ := hub.Subscribe("a", Source{Fetch: f.fetch})
	b, _ := hub.Subscribe("b", Source{Fetch: f.fetch})
	defer a.Close()
	defer b.Close()
	if _, err := hub.Subscribe("c", Source{Fetch: f.fetch}); !errors.Is(err, ErrTooManySeries) {
		t.Fatalf("third series err = %v, want ErrTooManySeries", err)
	}
	c, err := hub.Subscribe("a", Source{Fetch: f.fetch})
	if err != nil {
		t.Fatalf("joining existing series: %v", err)
	}
	defer c.Close()
	if _, err := hub.Subscribe("a", Source{Fetch: f.fetch}); !errors.Is(err, ErrTooManySubscriptions) {
		t.Fatalf("fourth subscription err = %v, want ErrTooManySubscriptions", err)
	}
}

// 验证缓冲写满的慢消费者被标记丢失，其余订阅者不受影响。
func TestHubDropsSlowConsumer(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 2
	var n atomic.Int64
	fetch := func(context.Context) ([]Bar, error) {
		return []Bar{bar(1, float64(n.Add(1)))}, nil
	}
	hub := NewHub(cfg)
	slow, _ := hub.Subscribe("k", Source{Fetch: fetch})
	defer slow.Close()
	select {
	case <-slow.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("slow consumer was not dropped")
	}
	if _, subs := hub.Stats(); subs != 1 {
		t.Fatalf("subs = %d, want lost subscriber counted until Close", subs)
	}
	slow.Close()
	if series, subs := hub.Stats(); series != 0 || subs != 0 {
		t.Fatalf("stats after close = %d/%d, want 0/0", series, subs)
	}
}

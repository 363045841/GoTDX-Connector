// 实时 K 线轮询中枢：同一 (品种, 周期, 复权) 只保留一个上游轮询器，按变化向全部订阅者扇出事件。
// 本包不依赖 HTTP 与 gotdx，帧编码与上游拉取由调用方注入。
package live

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Bar 实时 K 线载荷，JSON 字段与 KCQ 前端 LiveBar 对齐（timestamp 为 UTC 毫秒）。
type Bar struct {
	Timestamp int64    `json:"timestamp"`
	Open      float64  `json:"open"`
	High      float64  `json:"high"`
	Low       float64  `json:"low"`
	Close     float64  `json:"close"`
	Volume    *float64 `json:"volume,omitempty"`
	Turnover  *float64 `json:"turnover,omitempty"`
	// TradingDay 交易所本地日期 yyyymmdd，仅供会话判断（节假日/停牌识别），不输出。
	TradingDay int `json:"-"`
}

// sameValues 判断两根 K 线时间与数值是否完全一致，用于只在变化时推送。
func (b Bar) sameValues(o Bar) bool {
	return b.Timestamp == o.Timestamp && b.Open == o.Open && b.High == o.High &&
		b.Low == o.Low && b.Close == o.Close &&
		sameOptional(b.Volume, o.Volume) && sameOptional(b.Turnover, o.Turnover)
}

// sameOptional 比较两个可选数值。
func sameOptional(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// EventType 实时事件类型，与前端 LiveBarsFrame.type 一致。
type EventType string

// 实时事件类型。
const (
	EventSnapshot EventType = "snapshot"
	EventForming  EventType = "forming"
	EventClosed   EventType = "closed"
	EventStatus   EventType = "status"
)

// 状态事件取值。
const (
	StatusLive          = "live"
	StatusMarketClosed  = "market-closed"
	StatusUpstreamError = "upstream-error"
	StatusNoData        = "no-data"
)

// Event 轮询器向订阅者发出的一条事件；symbol/period 由各订阅者在编码时自行填入。
type Event struct {
	Type   EventType
	Bars   []Bar // snapshot
	Bar    Bar   // forming / closed
	Status string
	Detail string
}

// FetchFunc 拉取最近若干根升序 K 线（含当前形成中的一根）。
type FetchFunc func(ctx context.Context) ([]Bar, error)

// SessionFunc 根据当前时间与最近 K 线判断是否处于交易时段；detail 说明休市原因。
type SessionFunc func(now time.Time, tail []Bar) (open bool, detail string)

// Source 一个轮询序列的上游拉取与会话判定。
type Source struct {
	Fetch   FetchFunc
	Session SessionFunc
}

// Config 轮询中枢参数。
type Config struct {
	// OpenInterval 交易时段的轮询间隔，同时是会话检查的节拍。
	OpenInterval time.Duration
	// ClosedInterval 休市时的轮询间隔。
	ClosedInterval time.Duration
	// ErrorInterval 上游失败后的重试间隔。
	ErrorInterval time.Duration
	// MaxSubscriptions 全局订阅（SSE 连接）上限。
	MaxSubscriptions int
	// MaxSeries 全局并发轮询序列上限。
	MaxSeries int
	// SnapshotSize 快照帧携带的尾部 K 线根数。
	SnapshotSize int
	// BufferSize 单个订阅者的事件缓冲；写满视为慢消费者并断开。
	BufferSize int
	// Now 时钟注入点，默认 time.Now。
	Now func() time.Time
}

// DefaultConfig 返回生产默认参数。
func DefaultConfig() Config {
	return Config{
		OpenInterval:     2 * time.Second,
		ClosedInterval:   60 * time.Second,
		ErrorInterval:    5 * time.Second,
		MaxSubscriptions: 256,
		MaxSeries:        32,
		SnapshotSize:     2,
		BufferSize:       64,
		Now:              time.Now,
	}
}

// withDefaults 用默认值补齐未设置的参数。
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.OpenInterval <= 0 {
		c.OpenInterval = d.OpenInterval
	}
	if c.ClosedInterval <= 0 {
		c.ClosedInterval = d.ClosedInterval
	}
	if c.ErrorInterval <= 0 {
		c.ErrorInterval = d.ErrorInterval
	}
	if c.MaxSubscriptions <= 0 {
		c.MaxSubscriptions = d.MaxSubscriptions
	}
	if c.MaxSeries <= 0 {
		c.MaxSeries = d.MaxSeries
	}
	if c.SnapshotSize <= 0 {
		c.SnapshotSize = d.SnapshotSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = d.BufferSize
	}
	if c.Now == nil {
		c.Now = d.Now
	}
	return c
}

// 订阅上限错误。
var (
	ErrTooManySubscriptions = errors.New("live bars: too many subscriptions")
	ErrTooManySeries        = errors.New("live bars: too many distinct series")
)

// Hub 实时 K 线中枢：按 key 共享轮询器并统计全局订阅数。
type Hub struct {
	cfg     Config
	mu      sync.Mutex
	pollers map[string]*poller
	subs    int
}

// NewHub 创建实时 K 线中枢。
func NewHub(cfg Config) *Hub {
	return &Hub{cfg: cfg.withDefaults(), pollers: make(map[string]*poller)}
}

// Stats 返回当前轮询序列数与订阅数，供测试与日志使用。
func (h *Hub) Stats() (series, subscriptions int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pollers), h.subs
}

// Subscribe 订阅 key 对应的序列；首个订阅者的 src 用于创建轮询器，后续订阅者共享该轮询器。
func (h *Hub) Subscribe(key string, src Source) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs >= h.cfg.MaxSubscriptions {
		return nil, ErrTooManySubscriptions
	}
	p, ok := h.pollers[key]
	if !ok {
		if len(h.pollers) >= h.cfg.MaxSeries {
			return nil, ErrTooManySeries
		}
		p = newPoller(h.cfg, src)
		h.pollers[key] = p
		go p.run()
	}
	sub := &Subscription{
		hub:    h,
		key:    key,
		poller: p,
		events: make(chan Event, h.cfg.BufferSize),
		lost:   make(chan struct{}),
	}
	h.subs++
	p.add(sub)
	return sub, nil
}

// release 移除订阅；序列无人订阅时停止并移除其轮询器。
func (h *Hub) release(sub *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs--
	if sub.poller.remove(sub) == 0 && h.pollers[sub.key] == sub.poller {
		delete(h.pollers, sub.key)
		sub.poller.cancel()
	}
}

// Subscription 单个订阅者的事件流。
type Subscription struct {
	hub       *Hub
	key       string
	poller    *poller
	events    chan Event
	lost      chan struct{}
	lostOnce  sync.Once
	closeOnce sync.Once
}

// Events 返回事件通道；通道不会被关闭，结束信号见 Lost 与调用方上下文。
func (s *Subscription) Events() <-chan Event { return s.events }

// Lost 在订阅者因缓冲写满被中枢丢弃时关闭；调用方应断开连接让客户端重连取快照。
func (s *Subscription) Lost() <-chan struct{} { return s.lost }

// Close 退订；可重复调用。
func (s *Subscription) Close() {
	s.closeOnce.Do(func() { s.hub.release(s) })
}

// markLost 标记订阅丢失（仅关闭一次）。
func (s *Subscription) markLost() {
	s.lostOnce.Do(func() { close(s.lost) })
}

// poller 单个序列的上游轮询器与尾部状态。
type poller struct {
	cfg    Config
	src    Source
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	subs      map[*Subscription]struct{}
	tail      []Bar
	ready     bool
	open      bool
	status    string
	detail    string
	nextFetch time.Time
}

// newPoller 创建轮询器（未启动）。
func newPoller(cfg Config, src Source) *poller {
	ctx, cancel := context.WithCancel(context.Background())
	return &poller{
		cfg: cfg, src: src, ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
		subs: make(map[*Subscription]struct{}),
	}
}

// add 加入订阅者；已有尾部状态时立即补发快照与当前状态，使后来者无需等待下一次轮询。
func (p *poller) add(sub *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subs[sub] = struct{}{}
	if p.ready {
		p.sendLocked(sub, Event{Type: EventSnapshot, Bars: lastN(p.tail, p.cfg.SnapshotSize)})
	}
	if p.status != "" {
		p.sendLocked(sub, Event{Type: EventStatus, Status: p.status, Detail: p.detail})
	}
}

// remove 移除订阅者并返回剩余订阅数。
func (p *poller) remove(sub *Subscription) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.subs, sub)
	return len(p.subs)
}

// run 轮询主循环：按 OpenInterval 节拍检查会话，到期才访问上游；上下文取消即退出。
func (p *poller) run() {
	defer close(p.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
		}
		p.tick()
		timer.Reset(p.cfg.OpenInterval)
	}
}

// tick 一个节拍：休市转开市时立即拉取，否则仅在到期时拉取；随后刷新会话状态。
func (p *poller) tick() {
	now := p.cfg.Now()
	p.mu.Lock()
	tail := append([]Bar(nil), p.tail...)
	wasOpen, due := p.open, !now.Before(p.nextFetch)
	p.mu.Unlock()

	open, _ := p.session(now, tail)
	if due || (open && !wasOpen) {
		p.poll(now)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applySessionLocked(now)
}

// session 调用注入的会话判定；未注入时视为始终开市。
func (p *poller) session(now time.Time, tail []Bar) (bool, string) {
	if p.src.Session == nil {
		return true, ""
	}
	return p.src.Session(now, tail)
}

// poll 拉取上游并把变化转成事件广播。
func (p *poller) poll(now time.Time) {
	bars, err := p.src.Fetch(p.ctx)
	if p.ctx.Err() != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.nextFetch = now.Add(p.cfg.ErrorInterval)
		p.setStatusLocked(StatusUpstreamError, err.Error())
		return
	}
	if len(bars) == 0 {
		p.nextFetch = now.Add(p.cfg.ClosedInterval)
		p.setStatusLocked(StatusNoData, "upstream returned no bars")
		return
	}
	var events []Event
	if p.ready {
		events = diffTail(p.tail, bars, p.cfg.SnapshotSize)
	} else {
		events = []Event{{Type: EventSnapshot, Bars: lastN(bars, p.cfg.SnapshotSize)}}
	}
	p.tail = append([]Bar(nil), bars...)
	p.ready = true
	for _, ev := range events {
		p.broadcastLocked(ev)
	}
	p.applySessionLocked(now)
	if p.open {
		p.nextFetch = now.Add(p.cfg.OpenInterval)
	} else {
		p.nextFetch = now.Add(p.cfg.ClosedInterval)
	}
}

// applySessionLocked 按会话判定更新开市标记，并在状态变化时广播 status 帧。
func (p *poller) applySessionLocked(now time.Time) {
	if !p.ready {
		return
	}
	open, detail := p.session(now, p.tail)
	p.open = open
	if open {
		p.setStatusLocked(StatusLive, "")
	} else {
		p.setStatusLocked(StatusMarketClosed, detail)
	}
}

// setStatusLocked 仅在状态取值变化时广播，避免同类错误反复刷屏。
func (p *poller) setStatusLocked(status, detail string) {
	if p.status == status {
		p.detail = detail
		return
	}
	p.status, p.detail = status, detail
	p.broadcastLocked(Event{Type: EventStatus, Status: status, Detail: detail})
}

// broadcastLocked 向全部订阅者投递事件。
func (p *poller) broadcastLocked(ev Event) {
	for sub := range p.subs {
		p.sendLocked(sub, ev)
	}
}

// sendLocked 非阻塞投递；缓冲写满的慢消费者被移出并标记丢失，由其连接层断开后重连取快照。
func (p *poller) sendLocked(sub *Subscription, ev Event) {
	select {
	case sub.events <- ev:
	default:
		delete(p.subs, sub)
		sub.markLost()
	}
}

// diffTail 比较前后两次尾部，生成最小事件序列：
// 同一根变化 → forming；恰好跨过一根 → closed(上一根终值) + forming(新一根)；其余（跳根、回退）→ snapshot。
func diffTail(prev, next []Bar, snapshotSize int) []Event {
	if len(prev) == 0 {
		return []Event{{Type: EventSnapshot, Bars: lastN(next, snapshotSize)}}
	}
	pl, nl := prev[len(prev)-1], next[len(next)-1]
	switch {
	case nl.Timestamp == pl.Timestamp:
		if nl.sameValues(pl) {
			return nil
		}
		return []Event{{Type: EventForming, Bar: nl}}
	case nl.Timestamp > pl.Timestamp && len(next) >= 2 && next[len(next)-2].Timestamp == pl.Timestamp:
		return []Event{
			{Type: EventClosed, Bar: next[len(next)-2]},
			{Type: EventForming, Bar: nl},
		}
	default:
		return []Event{{Type: EventSnapshot, Bars: lastN(next, snapshotSize)}}
	}
}

// lastN 返回切片末尾 n 个元素的副本。
func lastN(bars []Bar, n int) []Bar {
	if len(bars) > n {
		bars = bars[len(bars)-n:]
	}
	return append([]Bar(nil), bars...)
}

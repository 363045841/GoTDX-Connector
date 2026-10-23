// 本文件测试 V1 实时 K 线 SSE：帧编码、品种解析、错误响应、端到端推送、心跳、断连清理与能力声明。
package v1

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/live"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
)

// liveTestCache 构造含同码股票/指数（000001）与唯一代码（600519）的证券目录。
func liveTestCache() *directory.Cache {
	return directory.NewCache(&fakeDirectoryLoader{stocks: map[uint8][]proto.Security{
		0: {{Code: "000001", Name: "平安银行"}},
		1: {{Code: "000001", Name: "上证指数"}, {Code: "600519", Name: "贵州茅台"}},
	}}, nil, 24*time.Hour)
}

// floatPtr 返回数值指针。
func floatPtr(v float64) *float64 { return &v }

// 验证四类帧按 KCQ LiveBarsFrame 编码，并原样回显客户端的 symbol/period。
func TestEncodeLiveFrame(t *testing.T) {
	b := live.Bar{Timestamp: 1700000000000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: floatPtr(10), TradingDay: 20231114}
	cases := []struct {
		ev   live.Event
		want string
	}{
		{live.Event{Type: live.EventSnapshot, Bars: []live.Bar{b}},
			`{"type":"snapshot","symbol":"600519","period":"5min","bars":[{"timestamp":1700000000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}]}`},
		{live.Event{Type: live.EventSnapshot},
			`{"type":"snapshot","symbol":"600519","period":"5min","bars":[]}`},
		{live.Event{Type: live.EventForming, Bar: b},
			`{"type":"forming","symbol":"600519","period":"5min","bar":{"timestamp":1700000000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}}`},
		{live.Event{Type: live.EventClosed, Bar: b},
			`{"type":"closed","symbol":"600519","period":"5min","bar":{"timestamp":1700000000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}}`},
		{live.Event{Type: live.EventStatus, Status: live.StatusMarketClosed, Detail: "weekend"},
			`{"type":"status","symbol":"600519","period":"5min","status":"market-closed","detail":"weekend"}`},
		{live.Event{Type: live.EventStatus, Status: live.StatusLive},
			`{"type":"status","symbol":"600519","period":"5min","status":"live"}`},
	}
	for _, tc := range cases {
		got, err := encodeLiveFrame("600519", "5min", tc.ev)
		if err != nil || string(got) != tc.want {
			t.Fatalf("encode %v = %s (%v), want %s", tc.ev.Type, got, err, tc.want)
		}
	}
	var sb strings.Builder
	if err := writeLiveFrame(&sb, "600519", "5min", cases[5].ev); err != nil || sb.String() != "data: "+cases[5].want+"\n\n" {
		t.Fatalf("sse message = %q, want unnamed data line terminated by blank line", sb.String())
	}
}

// 验证品种解析的各条路径：instrumentId、symbol 即 ID、后缀、exchange 参数、目录唯一匹配、多义、未找到与扩展行情。
func TestResolveLiveTarget(t *testing.T) {
	cache := liveTestCache()
	ok := []struct {
		symbol, id, exchange string
		want                 liveTarget
	}{
		{"000001", "gotdx:index:1:000001", "", liveTarget{Kind: "index", Market: 1, Code: "000001"}},
		{"gotdx:stock:0:000001", "", "", liveTarget{Kind: "stock", Market: 0, Code: "000001"}},
		{"000001.SZ", "", "", liveTarget{Kind: "stock", Market: 0, Code: "000001"}},
		{"000001", "", "sh", liveTarget{Kind: "index", Market: 1, Code: "000001"}},
		{"600519", "", "", liveTarget{Kind: "stock", Market: 1, Code: "600519"}},
	}
	for _, tc := range ok {
		got, apiErr, _ := resolveLiveTarget(cache, tc.symbol, tc.id, tc.exchange)
		if apiErr != nil || got != tc.want {
			t.Fatalf("resolve(%q,%q,%q) = %#v %#v, want %#v", tc.symbol, tc.id, tc.exchange, got, apiErr, tc.want)
		}
	}

	_, apiErr, status := resolveLiveTarget(cache, "000001", "", "")
	if apiErr == nil || status != http.StatusBadRequest || apiErr.Details["candidates"] == nil {
		t.Fatalf("ambiguous = %#v status=%d, want 400 with candidates", apiErr, status)
	}
	if _, apiErr, status = resolveLiveTarget(cache, "999999", "", ""); apiErr == nil || status != http.StatusNotFound || apiErr.Code != v1CodeInstrumentNotFound {
		t.Fatalf("missing = %#v status=%d, want 404 INSTRUMENT_NOT_FOUND", apiErr, status)
	}
	if _, apiErr, _ = resolveLiveTarget(cache, "IFL8", "gotdx:ex:47:IFL8", ""); apiErr == nil || apiErr.Code != v1CodeUnsupportedCapability {
		t.Fatalf("ex = %#v, want UNSUPPORTED_CAPABILITY", apiErr)
	}
	if _, apiErr, _ = resolveLiveTarget(cache, "", "", ""); apiErr == nil || apiErr.Code != v1CodeInvalidRequest {
		t.Fatalf("empty = %#v, want INVALID_REQUEST", apiErr)
	}
	if _, apiErr, status = resolveLiveTarget(nil, "600519", "", ""); apiErr == nil || status != http.StatusServiceUnavailable {
		t.Fatalf("no directory = %#v status=%d, want 503", apiErr, status)
	}
}

// liveFakeFetch 可在测试中切换返回尾部的拉取替身，并记录构造参数。
type liveFakeFetch struct {
	mu      sync.Mutex
	bars    []live.Bar
	targets []liveTarget
}

// build 实现 liveFetchBuilder。
func (f *liveFakeFetch) build(target liveTarget, category, adjust uint16) live.FetchFunc {
	f.mu.Lock()
	f.targets = append(f.targets, target)
	f.mu.Unlock()
	return func(context.Context) ([]live.Bar, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return append([]live.Bar(nil), f.bars...), nil
	}
}

// set 替换后续拉取返回的尾部。
func (f *liveFakeFetch) set(bars ...live.Bar) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bars = bars
}

// newLiveTestServer 启动挂载实时流处理器的测试服务。
func newLiveTestServer(t *testing.T, fetch *liveFakeFetch, heartbeat time.Duration) (*httptest.Server, *live.Hub) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	hub := live.NewHub(live.Config{
		OpenInterval: 5 * time.Millisecond, ClosedInterval: time.Hour, ErrorInterval: 5 * time.Millisecond,
		MaxSubscriptions: 4, MaxSeries: 4, SnapshotSize: 2, BufferSize: 16,
	})
	router := gin.New()
	router.GET("/api/v1/market-data/sources/:sourceId/stream", handleV1StreamWithDeps(liveStreamDeps{
		hub: hub, cache: liveTestCache(), fetch: fetch.build,
		session:   func(time.Time, []live.Bar) (bool, string) { return true, "" },
		heartbeat: heartbeat,
	}))
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return srv, hub
}

// sseReader 按空行切分 SSE 消息。
type sseReader struct{ r *bufio.Reader }

// next 读取下一条完整消息（不含结尾空行）。
func (s sseReader) next(t *testing.T) string {
	t.Helper()
	var lines []string
	for {
		line, err := s.r.ReadString('\n')
		if err != nil {
			t.Fatalf("read sse: %v (partial %q)", err, lines)
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			return strings.Join(lines, "\n")
		}
		lines = append(lines, line)
	}
}

// nextData 跳过心跳注释，返回下一条 data 帧的 JSON 解码结果。
func (s sseReader) nextData(t *testing.T) map[string]any {
	t.Helper()
	for {
		msg := s.next(t)
		if strings.HasPrefix(msg, ":") {
			continue
		}
		if !strings.HasPrefix(msg, "data: ") || strings.Contains(msg, "\n") {
			t.Fatalf("message %q is not a single unnamed data line", msg)
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(msg, "data: ")), &frame); err != nil {
			t.Fatalf("decode %q: %v", msg, err)
		}
		return frame
	}
}

// openStream 发起 SSE 请求并返回读取器与取消函数。
func openStream(t *testing.T, srv *httptest.Server, query string) (*http.Response, sseReader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/market-data/sources/gotdx/stream?"+query, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp, sseReader{bufio.NewReader(resp.Body)}, cancel
}

// waitHubEmpty 等待中枢回收全部订阅与轮询器。
func waitHubEmpty(t *testing.T, hub *live.Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		series, subs := hub.Stats()
		if series == 0 && subs == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hub not empty: %d series %d subs", series, subs)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// 验证端到端：SSE 响应头、首包、快照、状态、forming、closed+forming 与心跳，断连后中枢回收轮询器。
func TestV1StreamEndToEnd(t *testing.T) {
	fetch := &liveFakeFetch{}
	fetch.set(live.Bar{Timestamp: 1000, Close: 10}, live.Bar{Timestamp: 2000, Close: 11}, live.Bar{Timestamp: 3000, Close: 12})
	srv, hub := newLiveTestServer(t, fetch, 30*time.Millisecond)

	resp, reader, cancel := openStream(t, srv, "symbol=600519&period=5min&barAggregation=original")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for header, want := range map[string]string{
		"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
	} {
		if got := resp.Header.Get(header); got != want {
			t.Fatalf("%s = %q, want %q", header, got, want)
		}
	}
	if first := reader.next(t); !strings.HasPrefix(first, "retry: ") || !strings.Contains(first, ": connected gotdx:stock:1:600519") {
		t.Fatalf("first message = %q, want retry + connected comment", first)
	}

	snap := reader.nextData(t)
	bars, _ := snap["bars"].([]any)
	if snap["type"] != "snapshot" || snap["symbol"] != "600519" || snap["period"] != "5min" || len(bars) != 2 {
		t.Fatalf("snapshot = %#v, want 2 tail bars echoing symbol/period", snap)
	}
	if st := reader.nextData(t); st["type"] != "status" || st["status"] != "live" {
		t.Fatalf("status = %#v, want live", st)
	}

	fetch.set(live.Bar{Timestamp: 2000, Close: 11}, live.Bar{Timestamp: 3000, Close: 12.5})
	if f := reader.nextData(t); f["type"] != "forming" || f["bar"].(map[string]any)["close"] != 12.5 {
		t.Fatalf("forming = %#v, want close 12.5", f)
	}
	fetch.set(live.Bar{Timestamp: 3000, Close: 12.8}, live.Bar{Timestamp: 4000, Close: 13})
	closed, forming := reader.nextData(t), reader.nextData(t)
	if closed["type"] != "closed" || closed["bar"].(map[string]any)["timestamp"] != float64(3000) || closed["bar"].(map[string]any)["close"] != 12.8 {
		t.Fatalf("closed = %#v, want final ts=3000 close=12.8", closed)
	}
	if forming["type"] != "forming" || forming["bar"].(map[string]any)["timestamp"] != float64(4000) {
		t.Fatalf("forming = %#v, want ts=4000", forming)
	}
	if msg := reader.next(t); msg != ": ping" {
		t.Fatalf("idle message = %q, want heartbeat", msg)
	}

	if series, subs := hub.Stats(); series != 1 || subs != 1 {
		t.Fatalf("stats = %d/%d, want 1/1 while connected", series, subs)
	}
	cancel()
	waitHubEmpty(t, hub)
}

// 验证同一品种以不同写法（代码/带后缀）连接时共享一个轮询器，且各自回显自己的 symbol。
func TestV1StreamSharesSeriesAcrossConnections(t *testing.T) {
	fetch := &liveFakeFetch{}
	fetch.set(live.Bar{Timestamp: 1000, Close: 10})
	srv, hub := newLiveTestServer(t, fetch, time.Hour)

	_, a, cancelA := openStream(t, srv, "symbol=600519&period=daily")
	_, b, cancelB := openStream(t, srv, "symbol=600519.SH&period=daily")
	a.next(t)
	b.next(t)
	if snap := a.nextData(t); snap["symbol"] != "600519" {
		t.Fatalf("a snapshot = %#v", snap)
	}
	if snap := b.nextData(t); snap["symbol"] != "600519.SH" {
		t.Fatalf("b snapshot = %#v, want own symbol echoed", snap)
	}
	if series, subs := hub.Stats(); series != 1 || subs != 2 {
		t.Fatalf("stats = %d/%d, want 1 shared series 2 subs", series, subs)
	}
	cancelA()
	cancelB()
	waitHubEmpty(t, hub)
}

// 验证建流前的请求错误返回 V1 错误 envelope（非 200，EventSource 不会无限重连）。
func TestV1StreamRejectsBadRequests(t *testing.T) {
	srv, hub := newLiveTestServer(t, &liveFakeFetch{}, time.Hour)
	cases := []struct {
		query string
		code  int
		want  string
	}{
		{"symbol=600519&period=2min", http.StatusBadRequest, v1CodeUnsupportedCapability},
		{"symbol=000001&period=5min", http.StatusBadRequest, `"candidates":["gotdx:stock:0:000001","gotdx:index:1:000001"]`},
		{"symbol=999999&period=5min", http.StatusNotFound, v1CodeInstrumentNotFound},
		{"symbol=000001&period=5min&instrumentId=gotdx:index:1:000001&adjustment=qfq", http.StatusBadRequest, v1CodeUnsupportedCapability},
	}
	for _, tc := range cases {
		resp, err := http.Get(srv.URL + "/api/v1/market-data/sources/gotdx/stream?" + tc.query)
		if err != nil {
			t.Fatal(err)
		}
		var body strings.Builder
		_, _ = bufio.NewReader(resp.Body).WriteTo(&body)
		resp.Body.Close()
		if resp.StatusCode != tc.code || !strings.Contains(body.String(), tc.want) {
			t.Fatalf("%s → %d %s, want %d containing %s", tc.query, resp.StatusCode, body.String(), tc.code, tc.want)
		}
	}
	resp, _ := http.Get(srv.URL + "/api/v1/market-data/sources/other/stream?symbol=600519&period=5min")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown source status = %d, want 404", resp.StatusCode)
	}
	if series, subs := hub.Stats(); series != 0 || subs != 0 {
		t.Fatalf("rejected requests leaked %d/%d", series, subs)
	}
}

// 验证 gotdx 拉取函数复用 bars 接口映射（时间戳一致）、按 kind 路由并把 qfq 归一为 none。
func TestGotdxLiveFetchMapsLikeBars(t *testing.T) {
	originalStock, originalIndex := domain.FetchStockKLinePage, domain.FetchIndexBarsPage
	t.Cleanup(func() {
		domain.FetchStockKLinePage = originalStock
		domain.FetchIndexBarsPage = originalIndex
	})
	loc := time.FixedZone("CST", 8*60*60)
	bars := []proto.SecurityBar{
		{DateTime: time.Date(2026, 10, 9, 10, 0, 0, 0, loc), Open: 10, High: 11, Low: 9, Close: 10.5, Vol: 100, Amount: 1000},
		{DateTime: time.Date(2026, 10, 9, 10, 5, 0, 0, loc), Open: 10.5, High: 11.2, Low: 10.4, Close: 11, Vol: 50, Amount: 550},
	}
	var gotCount, gotAdjust uint16
	domain.FetchStockKLinePage = func(category uint16, market uint8, code string, start, count, times, adjust uint16) ([]proto.SecurityBar, error) {
		if category != 0 || market != 1 || code != "600519" || start != 0 {
			t.Fatalf("StockKLine cat=%d m=%d c=%s start=%d", category, market, code, start)
		}
		gotCount, gotAdjust = count, adjust
		return bars, nil
	}
	got, err := gotdxLiveFetch(liveTarget{Kind: "stock", Market: 1, Code: "600519"}, 0, 0)(context.Background())
	if err != nil || len(got) != 2 {
		t.Fatalf("fetch = %#v %v", got, err)
	}
	if gotCount != liveFetchSize || gotAdjust != 0 {
		t.Fatalf("count=%d adjust=%d, want %d/0", gotCount, gotAdjust, liveFetchSize)
	}
	want := securityBarToV1KLine(bars[1])
	if got[1].Timestamp != want.Timestamp || got[1].Close != 11 || *got[1].Volume != 50 || *got[1].Turnover != 550 || got[1].TradingDay != 20261009 {
		t.Fatalf("bar = %#v, want bars-endpoint mapping ts=%d", got[1], want.Timestamp)
	}

	domain.FetchIndexBarsPage = func(uint16, uint8, string, uint16, uint16) ([]proto.SecurityBar, error) {
		return nil, nil
	}
	got, err = gotdxLiveFetch(liveTarget{Kind: "index", Market: 1, Code: "000001"}, 0, 0)(context.Background())
	if err != nil || got != nil {
		t.Fatalf("empty index = %#v %v, want nil,nil (no-data status)", got, err)
	}
}

// 验证 A 股会话判定：周末、午休、盘中、节假日识别（09:35 后最近一根不是今天）。
func TestCNLiveSession(t *testing.T) {
	at := func(day, hour, minute int) time.Time {
		return time.Date(2026, 10, day, hour, minute, 0, 0, cnShanghai)
	}
	today := []live.Bar{{TradingDay: 20261009}}
	stale := []live.Bar{{TradingDay: 20261008}}
	cases := []struct {
		now  time.Time
		tail []live.Bar
		open bool
	}{
		{at(10, 10, 0), today, false}, // 周六
		{at(9, 9, 0), today, false},   // 开盘前
		{at(9, 9, 20), stale, true},   // 集合竞价，尚无今日 K 线
		{at(9, 10, 0), today, true},   // 盘中
		{at(9, 12, 0), today, false},  // 午休
		{at(9, 15, 1), today, true},   // 收盘余量
		{at(9, 15, 5), today, false},  // 收盘后
		{at(9, 10, 0), stale, false},  // 节假日或停牌
	}
	for _, tc := range cases {
		if open, detail := cnLiveSession(tc.now, tc.tail); open != tc.open {
			t.Fatalf("session(%s) = %v %q, want %v", tc.now, open, detail, tc.open)
		}
	}
	if open, _ := cnLiveSession(time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC), today); !open {
		t.Fatal("02:00 UTC is 10:00 Shanghai, want open regardless of server timezone")
	}
}

// 验证探测源级能力与搜索品种级能力声明 liveBars（主市场为 true）。
func TestV1CapabilitiesDeclareLiveBars(t *testing.T) {
	router := newV1TestRouter(liveTestCache(), func() client.Status { return client.Status{Ready: true} })
	router.GET("/probe-live", handleV1ProbeWithDeps(
		func() client.Status { return client.Status{Ready: true} },
		func() error { return nil },
	))
	probe := v1Request(router, http.MethodGet, "/probe-live", "")
	if !strings.Contains(probe.Body.String(), `"liveBars":true`) {
		t.Fatalf("probe = %s, want liveBars true", probe.Body.String())
	}
	search := v1Request(router, http.MethodPost, "/api/v1/market-data/instruments/search", `{"sourceId":"gotdx","keyword":"600519"}`)
	if !strings.Contains(search.Body.String(), `"liveBars":true`) {
		t.Fatalf("search = %s, want instrument liveBars true", search.Body.String())
	}
	if caps := v1InstrumentCapabilitiesFor(domain.KindEx); caps.LiveBars == nil || *caps.LiveBars {
		t.Fatalf("ex caps = %#v, want explicit liveBars false", caps)
	}
}

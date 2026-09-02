// 本文件测试 V1 协议路由：envelope 包装、探测、品种搜索、K 线与分时映射。
package v1

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
)

// fakeDirectoryLoader 为 V1 搜索测试提供可注入的证券目录加载器。
type fakeDirectoryLoader struct {
	stocks map[uint8][]proto.Security
}

func (l *fakeDirectoryLoader) StockAll(market uint8) ([]proto.Security, error) {
	return l.stocks[market], nil
}

func (l *fakeDirectoryLoader) ExCount() (uint32, error) {
	return 0, nil
}

func (l *fakeDirectoryLoader) ExList(uint32, uint16) ([]proto.ExListItem, error) {
	return nil, nil
}

func newV1TestRouter(cache *directory.Cache, status func() client.Status) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterRoutes(router, cache, status)
	return router
}

func v1Request(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

// 验证探测心跳成功且全域就绪时返回在线，并携带源级能力声明。
func TestV1ProbeOnline(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	router.GET("/api/v1/market-data/sources/gotdx/probe", handleV1ProbeWithDeps(
		func() client.Status { return client.Status{Ready: true} },
		func() error { return nil },
	))
	resp := v1Request(router, http.MethodGet, "/api/v1/market-data/sources/gotdx/probe", "")

	var body struct {
		Data struct {
			Status       string `json:"status"`
			CheckedAt    int64  `json:"checkedAt"`
			LatencyMs    int64  `json:"latencyMs"`
			Capabilities struct {
				AssetClasses []string `json:"assetClasses"`
				Bars         struct {
					Periods     []string `json:"periods"`
					Adjustments []string `json:"adjustments"`
				} `json:"bars"`
				TimeShare      bool `json:"timeShare"`
				TimeShareRange struct {
					MaxTradingDays int `json:"maxTradingDays"`
				} `json:"timeShareRange"`
				Depth bool `json:"depth"`
			} `json:"capabilities"`
		} `json:"data"`
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if body.Data.Status != "online" || body.RequestID == "" {
		t.Fatalf("probe = %#v, want online with requestId", body)
	}
	caps := body.Data.Capabilities
	if len(caps.AssetClasses) != 6 || len(caps.Bars.Periods) != 10 {
		t.Fatalf("capabilities = %#v, want 6 assetClasses and 10 periods", caps)
	}
	if !caps.TimeShare || caps.Depth {
		t.Fatalf("capabilities = %#v, want timeShare true depth false", caps)
	}
	if caps.TimeShareRange.MaxTradingDays != v1MaxTimeShareRangeDays {
		t.Fatalf("timeShareRange = %#v, want maxTradingDays=%d", caps.TimeShareRange, v1MaxTimeShareRangeDays)
	}
}

// 验证探测声明历史覆盖上界（historyCoverage.to），供前端流转筛选候选源。
func TestV1ProbeDeclaresHistoryCoverage(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	router.GET("/api/v1/market-data/sources/gotdx/probe", handleV1ProbeWithDeps(
		func() client.Status { return client.Status{Ready: true} },
		func() error { return nil },
	))
	resp := v1Request(router, http.MethodGet, "/api/v1/market-data/sources/gotdx/probe", "")

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"historyCoverage"`)) {
		t.Fatalf("response = %s, want historyCoverage", resp.Body.String())
	}
}

// 验证心跳成功但全域未就绪时返回降级。
func TestV1ProbeDegraded(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: false} })
	router.GET("/api/v1/market-data/sources/gotdx/probe", handleV1ProbeWithDeps(
		func() client.Status { return client.Status{Ready: false} },
		func() error { return nil },
	))
	resp := v1Request(router, http.MethodGet, "/api/v1/market-data/sources/gotdx/probe", "")

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"status":"degraded"`)) {
		t.Fatalf("response = %s, want degraded", resp.Body.String())
	}
}

// 验证心跳失败时返回离线并携带错误消息。
func TestV1ProbeOffline(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	router.GET("/api/v1/market-data/sources/gotdx/probe", handleV1ProbeWithDeps(
		func() client.Status { return client.Status{Ready: true} },
		func() error { return &errV1ProbeDown{} },
	))
	resp := v1Request(router, http.MethodGet, "/api/v1/market-data/sources/gotdx/probe", "")

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"status":"offline"`)) {
		t.Fatalf("response = %s, want offline", resp.Body.String())
	}
}

type errV1ProbeDown struct{}

func (e *errV1ProbeDown) Error() string { return "heartbeat refused" }

// 验证品种搜索映射为 V1 品种描述并包装在 envelope 中。
func TestV1SearchMapsInstruments(t *testing.T) {
	loader := &fakeDirectoryLoader{
		stocks: map[uint8][]proto.Security{
			0: {{Code: "000001", Name: "平安银行"}},
		},
	}
	cache := directory.NewCache(loader, nil, 24*time.Hour)
	router := newV1TestRouter(cache, func() client.Status { return client.Status{Ready: true} })

	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/instruments/search",
		`{"sourceId":"gotdx","keyword":"000001","limit":10}`)

	var body struct {
		Data struct {
			Items []struct {
				ID           string `json:"id"`
				SourceID     string `json:"sourceId"`
				Symbol       string `json:"symbol"`
				Name         string `json:"name"`
				AssetClass   string `json:"assetClass"`
				Exchange     string `json:"exchange"`
				SessionID    string `json:"sessionId"`
				Capabilities struct {
					Bars struct {
						Periods     []string `json:"periods"`
						Adjustments []string `json:"adjustments"`
					} `json:"bars"`
					TimeShare bool `json:"timeShare"`
				} `json:"capabilities"`
			} `json:"items"`
		} `json:"data"`
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if len(body.Data.Items) != 1 || body.RequestID == "" {
		t.Fatalf("items = %#v requestId=%q, want one", body.Data.Items, body.RequestID)
	}
	item := body.Data.Items[0]
	if item.ID != "gotdx:stock:0:000001" || item.AssetClass != "stock" || item.SessionID != "CN" {
		t.Fatalf("item = %#v, want gotdx:stock:0:000001 stock CN", item)
	}
	if len(item.Capabilities.Bars.Periods) != 10 {
		t.Fatalf("periods = %v, want 10 standard periods", item.Capabilities.Bars.Periods)
	}
	if len(item.Capabilities.Bars.Adjustments) != 3 || item.Capabilities.Bars.Adjustments[2] != "none" {
		t.Fatalf("adjustments = %v, want qfq/hfq/none", item.Capabilities.Bars.Adjustments)
	}
}

// 验证搜索空关键字返回 INVALID_REQUEST 错误 envelope。
func TestV1SearchRejectsEmptyKeyword(t *testing.T) {
	router := newV1TestRouter(directory.NewCache(&fakeDirectoryLoader{}, nil, 24*time.Hour), func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/instruments/search",
		`{"sourceId":"gotdx","keyword":"","limit":10}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INVALID_REQUEST"`)) {
		t.Fatalf("response = %s, want INVALID_REQUEST", resp.Body.String())
	}
}

// 验证股票 K 线请求按 kind=stock 路由、映射复权参数并输出 V1 条目。
func TestV1BarsStock(t *testing.T) {
	original := domain.FetchStockKLinePage
	t.Cleanup(func() { domain.FetchStockKLinePage = original })
	loc := time.FixedZone("CST", 8*60*60)
	day := time.Date(2026, 7, 24, 15, 0, 0, 0, loc)
	domain.FetchStockKLinePage = func(category uint16, market uint8, code string, start, count uint16, times, adjust uint16) ([]proto.SecurityBar, error) {
		if category != 4 || market != 1 || code != "600519" || times != 1 || adjust != 1 {
			t.Fatalf("StockKLine = cat=%d m=%d c=%s times=%d adjust=%d", category, market, code, times, adjust)
		}
		// 首页返回单根 K 线，后续页为空，终止分页循环，避免无限追加
		if start > 0 {
			return nil, nil
		}
		return []proto.SecurityBar{{DateTime: day, Open: 10, High: 11, Low: 9, Close: 10.5, Vol: 1000, Amount: 500000, RisePrice: 0.5, RiseRate: 5}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"daily","adjustment":"qfq","limit":1}`)

	var body struct {
		Data struct {
			InstrumentID string `json:"instrumentId"`
			Period       string `json:"period"`
			Adjustment   string `json:"adjustment"`
			Timezone     string `json:"timezone"`
			Items        []struct {
				Timestamp     int64    `json:"timestamp"`
				Close         float64  `json:"close"`
				Volume        *float64 `json:"volume"`
				ChangePercent *float64 `json:"changePercent"`
			} `json:"items"`
		} `json:"data"`
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if body.Data.InstrumentID != "gotdx:stock:1:600519" || body.Data.Period != "daily" || body.Data.Timezone != "Asia/Shanghai" || body.RequestID == "" {
		t.Fatalf("series = %#v", body.Data)
	}
	if len(body.Data.Items) != 1 || body.Data.Items[0].Timestamp != day.UnixMilli() || body.Data.Items[0].Close != 10.5 {
		t.Fatalf("items = %#v, want one 10.5 bar", body.Data.Items)
	}
	if body.Data.Items[0].Volume == nil || *body.Data.Items[0].Volume != 1000 {
		t.Fatalf("volume = %v, want 1000", body.Data.Items[0].Volume)
	}
	if body.Data.Items[0].ChangePercent == nil || *body.Data.Items[0].ChangePercent != 5 {
		t.Fatalf("changePercent = %v, want 5", body.Data.Items[0].ChangePercent)
	}
}

// 验证 before 为排他 UTC 毫秒游标，返回最新匹配页且条目保持时间升序。
func TestV1BarsStockBeforeCursor(t *testing.T) {
	original := domain.FetchStockKLinePage
	t.Cleanup(func() { domain.FetchStockKLinePage = original })
	loc := time.FixedZone("CST", 8*60*60)
	newest := time.Date(2026, 7, 26, 15, 0, 0, 0, loc)
	cursor := time.Date(2026, 7, 25, 15, 0, 0, 0, loc)
	older := time.Date(2026, 7, 24, 15, 0, 0, 0, loc)
	oldest := time.Date(2026, 7, 23, 15, 0, 0, 0, loc)
	domain.FetchStockKLinePage = func(category uint16, market uint8, code string, start, count uint16, times, adjust uint16) ([]proto.SecurityBar, error) {
		switch start {
		case 0:
			return []proto.SecurityBar{{DateTime: newest}, {DateTime: cursor}}, nil
		case 2:
			return []proto.SecurityBar{{DateTime: older}, {DateTime: oldest}}, nil
		default:
			return nil, nil
		}
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"daily","adjustment":"none","limit":2,"beforeTimestamp":`+strconv.FormatInt(cursor.UnixMilli(), 10)+`}`)

	var body struct {
		Data struct {
			Items []struct {
				Timestamp int64 `json:"timestamp"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if len(body.Data.Items) != 2 || body.Data.Items[0].Timestamp != oldest.UnixMilli() || body.Data.Items[1].Timestamp != older.UnixMilli() {
		t.Fatalf("items = %#v, want oldest then older", body.Data.Items)
	}
}

// 验证指数 K 线请求按 kind=index 路由到指数接口。
func TestV1BarsIndex(t *testing.T) {
	original := domain.FetchIndexBarsPage
	t.Cleanup(func() { domain.FetchIndexBarsPage = original })
	loc := time.FixedZone("CST", 8*60*60)
	day := time.Date(2026, 7, 24, 15, 0, 0, 0, loc)
	domain.FetchIndexBarsPage = func(category uint16, market uint8, code string, start, count uint16) ([]proto.SecurityBar, error) {
		if category != 4 || market != 1 || code != "000001" {
			t.Fatalf("GetIndexBars = cat=%d m=%d c=%s", category, market, code)
		}
		// 首页返回单根 K 线，后续页为空，终止分页循环，避免无限追加
		if start > 0 {
			return nil, nil
		}
		return []proto.SecurityBar{{DateTime: day, Open: 3500, High: 3510, Low: 3490, Close: 3505, Vol: 1}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:index:1:000001","symbol":"000001","exchange":"SH","providerRef":{"market":1,"kind":"index"}},"period":"daily","adjustment":"none","limit":1}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"close":3505`)) {
		t.Fatalf("response = %s, want index close 3505", resp.Body.String())
	}
}

// 验证扩展行情 K 线请求按 kind=ex 路由到扩展接口。
func TestV1BarsEx(t *testing.T) {
	original := domain.FetchExKLinePage
	t.Cleanup(func() { domain.FetchExKLinePage = original })
	loc := time.FixedZone("CST", 8*60*60)
	day := time.Date(2026, 7, 24, 15, 0, 0, 0, loc)
	domain.FetchExKLinePage = func(category uint8, code string, period uint16, start uint32, count uint16, times uint16) ([]proto.ExKLineItem, error) {
		if category != 31 || code != "HSI" || period != 4 {
			t.Fatalf("ExKLine = cat=%d c=%s period=%d", category, code, period)
		}
		// 首页返回单根 K 线，后续页为空，模拟真实 safeExKLine 的翻页终止行为
		if start > 0 {
			return nil, nil
		}
		return []proto.ExKLineItem{{DateTime: day, Open: 18000, High: 18100, Low: 17900, Close: 18050, Vol: 100, Amount: 200000}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:ex:31:HSI","symbol":"HSI","exchange":"HK","providerRef":{"category":31,"kind":"ex"}},"period":"daily","adjustment":"none","limit":1}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"close":18050`)) {
		t.Fatalf("response = %s, want ex close 18050", resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte(`"timezone":"Asia/Hong_Kong"`)) {
		t.Fatalf("response = %s, want HK timezone", resp.Body.String())
	}
}

// 验证股票 K 线缺少 providerRef.market 时返回 INVALID_REQUEST。
func TestV1BarsRejectsMissingMarket(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"kind":"stock"}},"period":"daily","adjustment":"none","limit":1}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INVALID_REQUEST"`)) {
		t.Fatalf("response = %s, want INVALID_REQUEST", resp.Body.String())
	}
}

// 验证请求未声明周期的周期时返回 UNSUPPORTED_CAPABILITY，而非静默回退到日线。
func TestV1BarsRejectsUnknownPeriod(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"2min","adjustment":"none","limit":1}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"UNSUPPORTED_CAPABILITY"`)) {
		t.Fatalf("response = %s, want UNSUPPORTED_CAPABILITY", resp.Body.String())
	}
}

// 验证无数据品种（如已到期期货）返回 INSTRUMENT_NOT_FOUND，触发前端请求流转。
func TestV1BarsNoDataReturnsInstrumentNotFound(t *testing.T) {
	original := domain.FetchStockKLinePage
	t.Cleanup(func() { domain.FetchStockKLinePage = original })
	domain.FetchStockKLinePage = func(uint16, uint8, string, uint16, uint16, uint16, uint16) ([]proto.SecurityBar, error) {
		return nil, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"daily","adjustment":"none","limit":1}`)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", resp.Code, resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INSTRUMENT_NOT_FOUND"`)) {
		t.Fatalf("response = %s, want INSTRUMENT_NOT_FOUND", resp.Body.String())
	}
}

// 验证已有 K 线后的游标耗尽是成功空页，不会被前端误判为加载失败。
func TestV1BarsCursorExhaustedReturnsSuccess(t *testing.T) {
	original := domain.FetchStockKLinePage
	t.Cleanup(func() { domain.FetchStockKLinePage = original })
	domain.FetchStockKLinePage = func(uint16, uint8, string, uint16, uint16, uint16, uint16) ([]proto.SecurityBar, error) {
		return nil, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"daily","adjustment":"none","limit":1,"beforeTimestamp":1}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte(`"olderData":"exhausted"`)) {
		t.Fatalf("response = %s, want exhausted cursor page", resp.Body.String())
	}
}

// 验证扩展行情请求不支持的复权方式时返回 UNSUPPORTED_CAPABILITY。
func TestV1BarsRejectsUnsupportedAdjustment(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:ex:31:HSI","symbol":"HSI","exchange":"HK","providerRef":{"category":31,"kind":"ex"}},"period":"daily","adjustment":"qfq","limit":1}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"UNSUPPORTED_CAPABILITY"`)) {
		t.Fatalf("response = %s, want UNSUPPORTED_CAPABILITY", resp.Body.String())
	}
}

// 验证 bars limit 缺失、非正数或超过上游单页上限时返回 INVALID_REQUEST。
func TestV1BarsRejectsInvalidLimit(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	for _, limit := range []string{"", `,"limit":0`, `,"limit":799`} {
		resp := v1Request(router, http.MethodPost, "/api/v1/market-data/bars",
			`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"period":"daily","adjustment":"none"`+limit+`}`)
		if resp.Code != http.StatusBadRequest || !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INVALID_REQUEST"`)) {
			t.Fatalf("limit suffix %q response = %d %s, want INVALID_REQUEST", limit, resp.Code, resp.Body.String())
		}
	}
}

// 验证股票分时输出 unix 毫秒点与昨收。
func TestV1TimeShareStock(t *testing.T) {
	originalTick := domain.FetchStockHistoryTick
	originalOpening := domain.FetchOpeningTrade
	originalK := domain.FetchStockKLinePage
	t.Cleanup(func() {
		domain.FetchStockHistoryTick = originalTick
		domain.FetchOpeningTrade = originalOpening
		domain.FetchStockKLinePage = originalK
	})
	loc := time.FixedZone("CST", 8*60*60)
	domain.FetchStockHistoryTick = func(date uint32, market uint8, code string) ([]proto.HistoryMinuteTimeData, error) {
		return []proto.HistoryMinuteTimeData{{Price: 8.5, Avg: 8.5, Vol: 100}}, nil
	}
	domain.FetchOpeningTrade = func(uint32, uint8, string, time.Time) (domain.OpeningTrade, bool) {
		return domain.OpeningTrade{}, false
	}
	domain.FetchStockKLinePage = func(category uint16, market uint8, code string, start, count uint16, times, adjust uint16) ([]proto.SecurityBar, error) {
		return []proto.SecurityBar{{DateTime: time.Date(2026, 7, 24, 15, 0, 0, 0, loc), PreClose: 8.4}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/timeshare",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"tradingDate":"2026-07-24"}`)

	var body struct {
		Data struct {
			InstrumentID string  `json:"instrumentId"`
			TradingDate  string  `json:"tradingDate"`
			Timezone     string  `json:"timezone"`
			PreClose     float64 `json:"preClose"`
			Items        []struct {
				Timestamp int64   `json:"timestamp"`
				Price     float64 `json:"price"`
				Average   float64 `json:"average"`
				Volume    *int    `json:"volume"`
			} `json:"items"`
		} `json:"data"`
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if body.Data.PreClose != 8.4 || body.Data.Timezone != "Asia/Shanghai" || body.RequestID == "" {
		t.Fatalf("series = %#v", body.Data)
	}
	if len(body.Data.Items) != 1 {
		t.Fatalf("items = %#v, want one point", body.Data.Items)
	}
	item := body.Data.Items[0]
	wantTS := time.Date(2026, 7, 24, 9, 31, 0, 0, loc).UnixMilli()
	if item.Timestamp != wantTS || item.Price != 8.5 || item.Volume == nil || *item.Volume != 100 {
		t.Fatalf("item = %#v, want 09:31 price 8.5 volume 100", item)
	}
}

// 验证多日分时按最近实际交易日聚合，并为每个交易日保留独立昨收。
func TestV1TimeShareRangeStock(t *testing.T) {
	originalTick := domain.FetchStockHistoryTick
	originalOpening := domain.FetchOpeningTrade
	originalK := domain.FetchStockKLinePage
	t.Cleanup(func() {
		domain.FetchStockHistoryTick = originalTick
		domain.FetchOpeningTrade = originalOpening
		domain.FetchStockKLinePage = originalK
	})
	loc := time.FixedZone("CST", 8*60*60)
	day1 := time.Date(2026, 7, 23, 15, 0, 0, 0, loc)
	day2 := time.Date(2026, 7, 24, 15, 0, 0, 0, loc)
	domain.FetchStockKLinePage = func(uint16, uint8, string, uint16, uint16, uint16, uint16) ([]proto.SecurityBar, error) {
		return []proto.SecurityBar{
			{DateTime: day2, PreClose: 8.5},
			{DateTime: day1, PreClose: 8.4},
		}, nil
	}
	domain.FetchStockHistoryTick = func(date uint32, market uint8, code string) ([]proto.HistoryMinuteTimeData, error) {
		if market != 1 || code != "600519" {
			t.Fatalf("history tick request = date=%d market=%d code=%s", date, market, code)
		}
		return []proto.HistoryMinuteTimeData{{Price: float64(date), Avg: float64(date), Vol: 100}}, nil
	}
	domain.FetchOpeningTrade = func(uint32, uint8, string, time.Time) (domain.OpeningTrade, bool) {
		return domain.OpeningTrade{}, false
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/timeshare/range",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"endTradingDate":"2026-07-26","days":2}`)

	var body struct {
		Data struct {
			InstrumentID  string `json:"instrumentId"`
			RequestedDays int    `json:"requestedDays"`
			Days          []struct {
				TradingDate string  `json:"tradingDate"`
				PreClose    float64 `json:"preClose"`
				Items       []struct {
					Price float64 `json:"price"`
				} `json:"items"`
			} `json:"days"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if resp.Code != http.StatusOK || body.Data.InstrumentID != "gotdx:stock:1:600519" || body.Data.RequestedDays != 2 {
		t.Fatalf("response = %d %#v, want successful two-day range", resp.Code, body.Data)
	}
	if len(body.Data.Days) != 2 || body.Data.Days[0].TradingDate != "2026-07-23" || body.Data.Days[1].TradingDate != "2026-07-24" {
		t.Fatalf("days = %#v, want ascending trading dates", body.Data.Days)
	}
	if body.Data.Days[0].PreClose != 8.4 || body.Data.Days[1].PreClose != 8.5 || len(body.Data.Days[1].Items) != 1 {
		t.Fatalf("days = %#v, want per-day preClose and points", body.Data.Days)
	}
}

// 验证多日分时拒绝超过服务端保护上限的 days。
func TestV1TimeShareRangeRejectsInvalidDays(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	for _, days := range []int{0, v1MaxTimeShareRangeDays + 1} {
		resp := v1Request(router, http.MethodPost, "/api/v1/market-data/timeshare/range",
			`{"sourceId":"gotdx","instrument":{"id":"gotdx:stock:1:600519","symbol":"600519","exchange":"SH","providerRef":{"market":1,"kind":"stock"}},"endTradingDate":"2026-07-24","days":`+strconv.Itoa(days)+`}`)
		if resp.Code != http.StatusBadRequest || !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INVALID_REQUEST"`)) {
			t.Fatalf("days %d response = %d %s, want INVALID_REQUEST", days, resp.Code, resp.Body.String())
		}
	}
}

// 验证扩展行情分时输出点与昨收。
func TestV1TimeShareEx(t *testing.T) {
	originalTick := domain.FetchExHistoryTick
	originalK := domain.FetchExKLinePage
	t.Cleanup(func() {
		domain.FetchExHistoryTick = originalTick
		domain.FetchExKLinePage = originalK
	})
	loc := time.FixedZone("CST", 8*60*60)
	domain.FetchExHistoryTick = func(date uint32, category uint8, code string) ([]proto.ExTickChartData, error) {
		return []proto.ExTickChartData{{Time: "09:31", Price: 10.5, Avg: 10.5, Vol: 100}}, nil
	}
	domain.FetchExKLinePage = func(category uint8, code string, period uint16, start uint32, count uint16, times uint16) ([]proto.ExKLineItem, error) {
		return []proto.ExKLineItem{{DateTime: time.Date(2026, 7, 24, 15, 0, 0, 0, loc), PreClose: 10.4}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/timeshare",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:ex:31:HSI","symbol":"HSI","exchange":"HK","providerRef":{"category":31,"kind":"ex"}},"tradingDate":"2026-07-24"}`)

	var body struct {
		Data struct {
			PreClose float64 `json:"preClose"`
			Timezone string  `json:"timezone"`
			Items    []struct {
				Timestamp int64   `json:"timestamp"`
				Price     float64 `json:"price"`
				Average   float64 `json:"average"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.Body.String())
	}
	if body.Data.PreClose != 10.4 || body.Data.Timezone != "Asia/Hong_Kong" || len(body.Data.Items) != 1 || body.Data.Items[0].Price != 10.5 {
		t.Fatalf("series = %#v", body.Data)
	}
}

// 验证扩展行情分时无数据时返回 INSTRUMENT_NOT_FOUND。
func TestV1TimeShareExNoData(t *testing.T) {
	originalTick := domain.FetchExHistoryTick
	t.Cleanup(func() { domain.FetchExHistoryTick = originalTick })
	domain.FetchExHistoryTick = func(uint32, uint8, string) ([]proto.ExTickChartData, error) {
		return []proto.ExTickChartData{{Time: "09:31", Price: 0, Avg: 0, Vol: 0}}, nil
	}

	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/timeshare",
		`{"sourceId":"gotdx","instrument":{"id":"gotdx:ex:31:HSI","symbol":"HSI","exchange":"HK","providerRef":{"category":31,"kind":"ex"}},"tradingDate":"2026-07-24"}`)

	if !bytes.Contains(resp.Body.Bytes(), []byte(`"code":"INSTRUMENT_NOT_FOUND"`)) {
		t.Fatalf("response = %s, want INSTRUMENT_NOT_FOUND", resp.Body.String())
	}
}

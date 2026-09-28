// 验证 V1 交易日历直接读取连接器内置的 gotdx 日历，并限制能力范围。
package v1

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
)

// TestTradingCalendarHoliday 验证春节休市由数据库处理而非工作日推导。
func TestTradingCalendarHoliday(t *testing.T) {
	zone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, 2, 13, 15, 0, 0, 0, zone).UnixMilli()
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	body, err := json.Marshal(v1TradingCalendarRequest{
		SourceID: "gotdx", Instrument: v1InstrumentReference{Symbol: "rb2610", Exchange: "FUTURES", ProviderRef: map[string]any{"kind": "ex"}},
		Period: "daily", Adjustment: "none", BarAggregation: "original", AnchorTimestamp: anchor, Count: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/trading-calendar", string(body))
	if resp.Code != http.StatusOK {
		t.Fatalf("response %d: %s", resp.Code, resp.Body.String())
	}
	var result struct {
		Data v1TradingCalendarResult `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data.FutureTimestamps) != 2 || result.Data.FutureTimestamps[0] != time.Date(2026, 2, 24, 15, 0, 0, 0, zone).UnixMilli() {
		t.Fatalf("unexpected calendar: %+v", result.Data)
	}
}

// TestTradingCalendarRejectsUnsupportedSeries 验证没有真实日历覆盖的序列不能提供预测标签。
func TestTradingCalendarRejectsUnsupportedSeries(t *testing.T) {
	router := newV1TestRouter(nil, func() client.Status { return client.Status{Ready: true} })
	resp := v1Request(router, http.MethodPost, "/api/v1/market-data/trading-calendar", `{"sourceId":"gotdx","instrument":{"symbol":"600519","exchange":"SH","providerRef":{"kind":"stock"}},"period":"daily","adjustment":"none","barAggregation":"original","anchorTimestamp":1760000000000,"count":3}`)
	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unexpected response %d: %s", resp.Code, resp.Body.String())
	}
}

// TestTradingCalendarCapabilities 验证品种能力只开放给数据库覆盖的期货合约。
func TestTradingCalendarCapabilities(t *testing.T) {
	if _, ok := futuresCalendarExchange("FUTURES", "rb2610"); !ok {
		t.Fatal("SHFE futures must be supported")
	}
	if _, ok := futuresCalendarExchange("SH", "600519"); ok {
		t.Fatal("stocks must not be advertised")
	}
	if _, ok := futuresCalendarExchange("FUTURES", "UNKNOWN2610"); ok {
		t.Fatal("unknown futures must not be advertised")
	}
}

// TestFuturesCalendarVenues 校验前缀表与注释一致：格式合法、逐个前缀可命中、互不串所。
func TestFuturesCalendarVenues(t *testing.T) {
	wantVenues := map[string]bool{"SHFE": true, "INE": true, "DCE": true, "CZCE": true, "CFFEX": true}
	seen := map[string]int{}
	for _, entry := range futuresCalendarVenues {
		if !wantVenues[entry.venue] {
			t.Fatalf("unexpected venue %q; 数据库 exchange 列仅含 %v", entry.venue, wantVenues)
		}
		if !strings.HasPrefix(entry.codes, " ") || !strings.HasSuffix(entry.codes, " ") {
			t.Fatalf("%s codes must be space padded: %q", entry.venue, entry.codes)
		}
		if strings.Contains(entry.codes, "  ") {
			t.Fatalf("%s codes must not contain empty prefix: %q", entry.venue, entry.codes)
		}
		for _, prefix := range strings.Fields(entry.codes) {
			seen[prefix]++
			venue, ok := futuresCalendarExchange("FUTURES", prefix+"2610")
			if !ok || venue != entry.venue {
				t.Fatalf("prefix %q resolved to %q/%v, want %q", prefix, venue, ok, entry.venue)
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("venue table is empty")
	}
}

// TestFuturesCalendarPrefixIsolation 验证短前缀不会误配其他交易所的更长品种代码。
func TestFuturesCalendarPrefixIsolation(t *testing.T) {
	cases := map[string]string{
		"I2610":  "DCE",   // 铁矿石，不能命中 CFFEX 的 IC/IF/IH/IM
		"IC2610": "CFFEX", // 沪深 300 股指
		"L2610":  "DCE",   // 塑料，不能命中 INE 的 LU
		"LU2610": "INE",   // 液化天然气
		"P2610":  "DCE",   // 棕榈油，不能命中 CZCE 的 PF/PK/PM 或 DCE 自身的 PG/PP
		"TA2610": "CZCE",  // PTA
		"T2610":  "CFFEX", // 10 年期国债，不能命中 CZCE 的 TA
		"2601":   "",      // 纯数字无品种前缀
	}
	for symbol, want := range cases {
		venue, _ := futuresCalendarExchange("FUTURES", symbol)
		if venue != want {
			t.Fatalf("%s resolved to %q, want %q", symbol, venue, want)
		}
	}
}

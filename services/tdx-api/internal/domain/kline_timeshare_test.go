// 本文件测试股票/指数/扩展行情 K 线与分时昨收解析的领域逻辑。
package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/bensema/gotdx/proto"
)

// 验证指数日K按日期排序并去除重复K线。
func TestFilterKLineByDateSortsAndDeduplicatesIndexBars(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	klines := []proto.SecurityBar{
		{Year: 2026, Month: 7, Day: 24, Hour: 15, Minute: 0, DateTime: time.Date(2026, 7, 24, 15, 0, 0, 0, loc)},
		{Year: 2026, Month: 7, Day: 23, Hour: 15, Minute: 0, DateTime: time.Date(2026, 7, 23, 15, 0, 0, 0, loc)},
		{Year: 2026, Month: 7, Day: 23, Hour: 15, Minute: 0, DateTime: time.Date(2026, 7, 23, 15, 0, 0, 0, loc)},
		{Year: 2026, Month: 7, Day: 22, Hour: 15, Minute: 0, DateTime: time.Date(2026, 7, 22, 15, 0, 0, 0, loc)},
	}

	got := filterKLineByDate(
		klines,
		time.Date(2026, 7, 23, 0, 0, 0, 0, loc),
		time.Date(2026, 7, 24, 0, 0, 0, 0, loc),
		true,
	)

	if len(got) != 2 {
		t.Fatalf("filtered bars = %d, want 2", len(got))
	}
	if got[0].DateTime.Day() != 23 || got[1].DateTime.Day() != 24 {
		t.Fatalf("dates = %s, %s; want ascending 23, 24", got[0].DateTime, got[1].DateTime)
	}
}

// 验证指数日K分页大小不超过上游接口限制。
func TestIndexPageSizeStaysBelowGetIndexBarsLimit(t *testing.T) {
	if indexPageSize != 798 {
		t.Fatalf("index page size = %d, want 798", indexPageSize)
	}
	if indexPageSize >= 801 {
		t.Fatalf("index page size = %d exceeds the verified GetIndexBars limit", indexPageSize)
	}
	if indexFallbackPageSize == 0 || indexFallbackPageSize >= indexPageSize {
		t.Fatalf("indexFallbackPageSize = %d invalid relative to indexPageSize", indexFallbackPageSize)
	}
}

// 验证历史股票分时使用目标日K线昨收。
func TestResolveStockPreCloseUsesTargetDailyBarForHistoricalDate(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	source := StockPreCloseSource{
		Now: func() time.Time { return time.Date(2026, 7, 27, 10, 0, 0, 0, loc) },
		Quote: func(uint8, string) (float64, error) {
			return 0, errors.New("realtime quote must not be called for a historical date")
		},
		DailyBars: func(market uint8, code string, date time.Time) ([]proto.SecurityBar, error) {
			if market != 1 || code != "600519" || date.Format("20060102") != "20260724" {
				t.Fatalf("daily bar request = %d/%s/%s", market, code, date.Format("20060102"))
			}
			return []proto.SecurityBar{{
				DateTime: date,
				PreClose: 1418.5,
				Close:    1432.8,
			}}, nil
		},
	}

	got, err := ResolveStockPreClose(1, "600519", 20260724, source)
	if err != nil {
		t.Fatalf("resolve historical preClose: %v", err)
	}
	if got != 1418.5 {
		t.Fatalf("preClose = %v, want 1418.5", got)
	}
}

// 验证当日实时昨收为零时拒绝无效基准。
func TestResolveStockPreCloseRejectsZeroRealtimeBaseline(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	source := StockPreCloseSource{
		Now: func() time.Time { return time.Date(2026, 7, 27, 10, 0, 0, 0, loc) },
		Quote: func(uint8, string) (float64, error) {
			return 0, nil
		},
		DailyBars: func(uint8, string, time.Time) ([]proto.SecurityBar, error) {
			return nil, errors.New("daily bars must not be called for the current date")
		},
	}

	if _, err := ResolveStockPreClose(0, "000001", 20260727, source); err == nil {
		t.Fatal("resolve current preClose succeeded with zero baseline")
	}
}

// 验证扩展行情日K按日期区间过滤、去重和排序。
func TestFilterExKLineByDateKeepsBarsInRange(t *testing.T) {
	loc := time.Local
	klines := []proto.ExKLineItem{
		{DateTime: time.Date(2026, 7, 22, 15, 0, 0, 0, loc), Open: 1, High: 1, Low: 1, Close: 1},
		{DateTime: time.Date(2026, 7, 23, 15, 0, 0, 0, loc), Open: 2, High: 2, Low: 2, Close: 2},
		{DateTime: time.Date(2026, 7, 23, 15, 0, 0, 0, loc), Open: 2, High: 2, Low: 2, Close: 2}, // dup
		{DateTime: time.Date(2026, 7, 24, 15, 0, 0, 0, loc), Open: 3, High: 3, Low: 3, Close: 3},
		{DateTime: time.Date(2026, 7, 25, 15, 0, 0, 0, loc), Open: 4, High: 4, Low: 4, Close: 4},
	}

	got := filterExKLineByDate(
		klines,
		time.Date(2026, 7, 23, 0, 0, 0, 0, loc),
		time.Date(2026, 7, 24, 0, 0, 0, 0, loc),
	)

	if len(got) != 2 {
		t.Fatalf("filtered bars = %d, want 2; got %#v", len(got), got)
	}
	if got[0].DateTime.Day() != 23 || got[1].DateTime.Day() != 24 {
		t.Fatalf("dates = %s, %s; want ascending 23 then 24", got[0].DateTime, got[1].DateTime)
	}
}

// 验证扩展行情当日分时优先目标日线昨收（实时 ExQuote.PreClose 对美股不可靠）。
func TestResolveExPreClosePrefersDailyBarOnCurrentDate(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	source := ExPreCloseSource{
		Now: func() time.Time { return time.Date(2026, 7, 28, 10, 0, 0, 0, loc) },
		Quote: func(category uint8, code string) (float64, error) {
			t.Fatalf("quote must not be called when daily bar is available: %d/%s", category, code)
			return 0, nil
		},
		DailyBars: func(category uint8, code string, date time.Time) ([]proto.ExKLineItem, error) {
			if category != 31 || code != "01810" || date.Format("20060102") != "20260728" {
				t.Fatalf("daily bar request = %d/%s/%s", category, code, date.Format("20060102"))
			}
			return []proto.ExKLineItem{{
				DateTime: time.Date(2026, 7, 28, 15, 0, 0, 0, loc),
				PreClose: 18.5,
				Open:     18.6,
				Close:    18.7,
			}}, nil
		},
	}

	got, err := ResolveExPreClose(31, "01810", 20260728, source)
	if err != nil {
		t.Fatalf("resolve current preClose: %v", err)
	}
	if got != 18.5 {
		t.Fatalf("preClose = %v, want 18.5", got)
	}
}

// 验证扩展行情当日日线缺失（如盘前尚无当日 bar）时回退实时行情昨收。
func TestResolveExPreCloseFallsBackToQuoteWhenCurrentDailyBarMissing(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	source := ExPreCloseSource{
		Now: func() time.Time { return time.Date(2026, 7, 29, 12, 0, 0, 0, loc) },
		Quote: func(category uint8, code string) (float64, error) {
			if category != 31 || code != "00700" {
				t.Fatalf("quote request = %d/%s", category, code)
			}
			return 447.2, nil
		},
		DailyBars: func(uint8, string, time.Time) ([]proto.ExKLineItem, error) {
			// 当日日线尚未形成，返回空
			return nil, nil
		},
	}

	got, err := ResolveExPreClose(31, "00700", 20260729, source)
	if err != nil {
		t.Fatalf("resolve current preClose via quote fallback: %v", err)
	}
	if got != 447.2 {
		t.Fatalf("preClose = %v, want quote 447.2", got)
	}
}

// 验证扩展行情历史分时使用目标日K线昨收。
func TestResolveExPreCloseUsesTargetDailyBarForHistoricalDate(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	source := ExPreCloseSource{
		Now: func() time.Time { return time.Date(2026, 7, 28, 10, 0, 0, 0, loc) },
		Quote: func(uint8, string) (float64, error) {
			return 0, errors.New("realtime quote must not be called for a historical date")
		},
		DailyBars: func(category uint8, code string, date time.Time) ([]proto.ExKLineItem, error) {
			if category != 31 || code != "01810" || date.Format("20060102") != "20260724" {
				t.Fatalf("daily bar request = %d/%s/%s", category, code, date.Format("20060102"))
			}
			return []proto.ExKLineItem{{
				DateTime: time.Date(2026, 7, 24, 15, 0, 0, 0, loc),
				PreClose: 18.0,
				Open:     18.2,
				Close:    18.8,
			}}, nil
		},
	}

	got, err := ResolveExPreClose(31, "01810", 20260724, source)
	if err != nil {
		t.Fatalf("resolve historical preClose: %v", err)
	}
	if got != 18.0 {
		t.Fatalf("preClose = %v, want 18.0", got)
	}
}

// 验证新三板、基金和 REITs 日K透传对应复权参数。
func TestStockKLineRangePassesAdjustForSpecifiedSecurityTypes(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	startDate := time.Date(2026, 7, 24, 0, 0, 0, 0, loc)
	endDate := startDate
	tests := []struct {
		name   string
		code   string
		adjust uint16
	}{
		// 东海证券（832970）：新三板日K前复权。
		{name: "new third board forward adjusted", code: "832970", adjust: 0},
		// 国泰中证同业存单AAA指数7天持有期（015825）：基金日K不复权。
		{name: "fund unadjusted", code: "015825", adjust: 1},
		// 国泰海通砂之船商业REIT（508602）：REITs日K后复权。
		{name: "REIT backward adjusted", code: "508602", adjust: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetch := Fetcher{StockPage: func(category uint16, market uint8, code string, start, count uint16, times, adjust uint16) ([]proto.SecurityBar, error) {
				if category != 4 || market != 1 || code != tt.code || times != 1 || adjust != tt.adjust {
					t.Fatalf("StockKLine request = category=%d market=%d code=%s times=%d adjust=%d", category, market, code, times, adjust)
				}
				return []proto.SecurityBar{{DateTime: startDate, Close: 10}}, nil
			}}

			bars, err := StockKLineRange(fetch, 4, 1, tt.code, 1, tt.adjust, startDate, endDate)
			if err != nil {
				t.Fatalf("StockKLineRange: %v", err)
			}
			if len(bars) != 1 || bars[0].Close != 10 {
				t.Fatalf("bars = %#v, want one daily bar", bars)
			}
		})
	}
}

// 验证上证指数和深证成指使用对应市场的指数日K接口。
func TestIndexKLineRangeUsesSpecifiedIndexCodes(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	startDate := time.Date(2026, 7, 24, 0, 0, 0, 0, loc)
	endDate := startDate
	tests := []struct {
		name   string
		market uint8
		code   string
	}{
		// 上证指数（000001）使用上海市场指数日K接口。
		{name: "Shanghai Composite", market: 1, code: "000001"},
		// 深证成指（399001）使用深圳市场指数日K接口。
		{name: "Shenzhen Component", market: 0, code: "399001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetch := Fetcher{IndexPage: func(category uint16, market uint8, code string, start, count uint16) ([]proto.SecurityBar, error) {
				if category != 4 || market != tt.market || code != tt.code {
					t.Fatalf("GetIndexBars request = category=%d market=%d code=%s", category, market, code)
				}
				return []proto.SecurityBar{{DateTime: startDate, Close: 3000}}, nil
			}}

			bars, err := IndexKLineRange(fetch, 4, tt.market, tt.code, startDate, endDate)
			if err != nil {
				t.Fatalf("IndexKLineRange: %v", err)
			}
			if len(bars) != 1 || bars[0].Close != 3000 {
				t.Fatalf("bars = %#v, want one daily bar", bars)
			}
		})
	}
}

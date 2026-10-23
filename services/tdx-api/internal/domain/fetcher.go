// 上游行情拉取的装配层：把 Manager 绑定成一组函数，供领域逻辑与 V1 使用。
// 测试构造自己的 Fetcher 替换实现，不再依赖包级可变变量。
package domain

import (
	"errors"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"github.com/bensema/gotdx/proto"
)

// StockPageFetcher 拉取 A 股/指数单页 K 线。
type StockPageFetcher func(category uint16, market uint8, code string, start, count, times, adjust uint16) ([]proto.SecurityBar, error)

// IndexPageFetcher 拉取指数单页 K 线并映射为 SecurityBar。
type IndexPageFetcher func(category uint16, market uint8, code string, start, count uint16) ([]proto.SecurityBar, error)

// ExPageFetcher 拉取扩展行情单页 K 线。
type ExPageFetcher func(category uint8, code string, period uint16, start uint32, count uint16, times uint16) ([]proto.ExKLineItem, error)

// OpeningTradeFetcher 拉取指定日期的开盘首笔。
type OpeningTradeFetcher func(date uint32, market uint8, code string, now time.Time) (OpeningTrade, bool)

// Fetcher 一次装配的全部行情拉取入口。
type Fetcher struct {
	StockPage     StockPageFetcher
	IndexPage     IndexPageFetcher
	ExPage        ExPageFetcher
	StockTick     StockHistoryTickFetcher
	ExTick        ExHistoryTickFetcher
	StockPreClose func(market uint8, code string) (float64, error)
	ExPreClose    func(category uint8, code string) (float64, error)
	OpeningTrade  OpeningTradeFetcher
}

// NewFetcher 用管理器装配生产用拉取函数。
func NewFetcher(manager *client.Manager) Fetcher {
	return Fetcher{
		StockPage: func(category uint16, market uint8, code string, start, count, times, adjust uint16) ([]proto.SecurityBar, error) {
			return client.QueryMain(manager, func(c client.MainQuerier) ([]proto.SecurityBar, error) {
				return c.StockKLine(category, market, code, start, count, times, adjust)
			})
		},
		IndexPage: func(category uint16, market uint8, code string, start, count uint16) ([]proto.SecurityBar, error) {
			return fetchIndexPage(manager, category, market, code, start, count)
		},
		ExPage: func(category uint8, code string, period uint16, start uint32, count uint16, times uint16) ([]proto.ExKLineItem, error) {
			return client.QueryEx(manager, func(c client.ExQuerier) ([]proto.ExKLineItem, error) {
				return c.ExKLine(category, code, period, start, count, times)
			})
		},
		StockTick: func(date uint32, market uint8, code string) ([]proto.HistoryMinuteTimeData, error) {
			return client.QueryMain(manager, func(c client.MainQuerier) ([]proto.HistoryMinuteTimeData, error) {
				return c.StockHistoryTickChart(date, market, code)
			})
		},
		ExTick: func(date uint32, category uint8, code string) ([]proto.ExTickChartData, error) {
			return client.QueryEx(manager, func(c client.ExQuerier) ([]proto.ExTickChartData, error) {
				return c.ExTickChart(category, code, date)
			})
		},
		StockPreClose: func(market uint8, code string) (float64, error) {
			quotes, err := client.QueryMain(manager, func(c client.MainQuerier) ([]proto.SecurityQuote, error) {
				return c.StockQuotesDetail([]uint8{market}, []string{code})
			})
			if err != nil {
				return 0, err
			}
			if len(quotes) == 0 {
				return 0, errors.New("empty realtime quote response")
			}
			return quotes[0].PreClose, nil
		},
		ExPreClose: func(category uint8, code string) (float64, error) {
			quote, err := client.QueryEx(manager, func(c client.ExQuerier) (*proto.ExQuoteItem, error) {
				return c.ExQuote(category, code)
			})
			if err != nil {
				return 0, err
			}
			if quote == nil {
				return 0, errors.New("empty ex quote response")
			}
			return quote.PreClose, nil
		},
		OpeningTrade: func(date uint32, market uint8, code string, now time.Time) (OpeningTrade, bool) {
			return fetchOpeningTrade(manager, date, market, code, now)
		},
	}
}

// fetchIndexPage 拉取指数 K 线并映射为与股票接口一致的 SecurityBar。
func fetchIndexPage(manager *client.Manager, category uint16, market uint8, code string, start, count uint16) ([]proto.SecurityBar, error) {
	reply, err := client.QueryMain(manager, func(c client.MainQuerier) (*proto.GetIndexBarsReply, error) {
		return c.GetIndexBars(category, market, code, start, count)
	})
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return []proto.SecurityBar{}, nil
	}
	out := make([]proto.SecurityBar, 0, len(reply.List))
	loc := time.FixedZone("CST", 8*60*60)
	for _, b := range reply.List {
		dt := b.DateTime
		if dt.IsZero() {
			dt = time.Date(b.Year, time.Month(b.Month), b.Day, b.Hour, b.Minute, 0, 0, loc)
		}
		out = append(out, proto.SecurityBar{
			PreClose:  b.PreClose,
			LastClose: b.LastClose,
			Open:      b.Open,
			Close:     b.Close,
			High:      b.High,
			Low:       b.Low,
			Vol:       b.Vol,
			Amount:    b.Amount,
			RisePrice: b.RisePrice,
			RiseRate:  b.RiseRate,
			Year:      b.Year,
			Month:     b.Month,
			Day:       b.Day,
			Hour:      b.Hour,
			Minute:    b.Minute,
			DateTime:  dt,
			UpCount:   b.UpCount,
			DownCount: b.DownCount,
		})
	}
	return out, nil
}

// fetchOpeningTrade 按日期选逐笔源：当日用实时逐笔，历史日用历史逐笔。
func fetchOpeningTrade(manager *client.Manager, date uint32, market uint8, code string, now time.Time) (OpeningTrade, bool) {
	loc := time.FixedZone("CST", 8*60*60)
	now = now.In(loc)
	currentDate := uint32(now.Year()*10000 + int(now.Month())*100 + now.Day())
	if date == currentDate {
		trans, err := client.QueryMain(manager, func(c client.MainQuerier) ([]proto.TransactionData, error) {
			return c.StockFullTransaction(market, code)
		})
		if err != nil || len(trans) == 0 {
			return OpeningTrade{}, false
		}
		return OpeningTrade{Price: trans[0].Price, Vol: trans[0].Vol}, true
	}
	trans, err := client.QueryMain(manager, func(c client.MainQuerier) ([]proto.HistoryTransactionData, error) {
		return c.StockHistoryFullTransaction(date, market, code)
	})
	if err != nil || len(trans) == 0 {
		return OpeningTrade{}, false
	}
	return OpeningTrade{Price: trans[0].Price, Vol: trans[0].Vol}, true
}

// NewStockPreCloseSource 构造 A 股/指数分时昨收依赖；日线按 kind 选择指数或股票接口。
func NewStockPreCloseSource(f Fetcher, market uint8, code string, isIndex bool) StockPreCloseSource {
	return StockPreCloseSource{
		Now:   time.Now,
		Quote: f.StockPreClose,
		DailyBars: func(_ uint8, _ string, date time.Time) ([]proto.SecurityBar, error) {
			if isIndex {
				return IndexKLineRange(f, 4, market, code, date, date)
			}
			return StockKLineRange(f, 4, market, code, 1, 0, date, date)
		},
	}
}

// NewExPreCloseSource 构造扩展行情分时昨收依赖。
func NewExPreCloseSource(f Fetcher) ExPreCloseSource {
	return ExPreCloseSource{
		Now:   time.Now,
		Quote: f.ExPreClose,
		DailyBars: func(category uint8, code string, date time.Time) ([]proto.ExKLineItem, error) {
			return ExKLineRange(f, category, code, 4, 1, date, date)
		},
	}
}

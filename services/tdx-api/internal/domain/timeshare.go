// 分时点构建与昨收解析领域逻辑，A 股/指数/扩展行情共用，api/v1 两层复用。
package domain

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bensema/gotdx/proto"
)

// ErrNoTimeShareData 指定日期无历史分时数据（扩展行情全零点）。
var ErrNoTimeShareData = errors.New("该日期暂无历史分时数据")

// TimeSharePoint 分时点，时间保留为 time.Time 供不同序列化端（ISO 串 / unix 毫秒）使用。
type TimeSharePoint struct {
	At     time.Time
	Price  float64
	Avg    float64
	Volume *int
	Amount *int64
}

// OpeningTrade 开盘首笔，用于补 09:30 分时点。
type OpeningTrade struct {
	Price float64
	Vol   int
}

// StockTimeShareRequest A 股/指数分时查询参数，与 HTTP 请求体解耦。
type StockTimeShareRequest struct {
	Date   uint32
	Market uint8
	Code   string
	Kind   string
}

// ExTimeShareRequest 扩展行情分时查询参数，与 HTTP 请求体解耦。
type ExTimeShareRequest struct {
	Date     uint32
	Category uint8
	Code     string
}

// StockHistoryTickFetcher A 股/指数历史分时拉取签名。
type StockHistoryTickFetcher func(date uint32, market uint8, code string) ([]proto.HistoryMinuteTimeData, error)

// ExHistoryTickFetcher 扩展行情历史分时拉取签名。
type ExHistoryTickFetcher func(date uint32, category uint8, code string) ([]proto.ExTickChartData, error)

// StockPreCloseSource 分时昨收依赖，now/quote/dailyBars 可测注入。
type StockPreCloseSource struct {
	Now       func() time.Time
	Quote     func(market uint8, code string) (float64, error)
	DailyBars func(market uint8, code string, date time.Time) ([]proto.SecurityBar, error)
}

// ResolveStockPreClose 当日读取实时行情，历史日读取目标日线的前收价。
func ResolveStockPreClose(market uint8, code string, date uint32, source StockPreCloseSource) (float64, error) {
	year := int(date / 10000)
	month := time.Month((date % 10000) / 100)
	day := int(date % 100)
	loc := time.FixedZone("CST", 8*60*60)
	target := time.Date(year, month, day, 0, 0, 0, 0, loc)
	if target.Year() != year || target.Month() != month || target.Day() != day {
		return 0, fmt.Errorf("invalid history date: %d", date)
	}

	now := source.Now().In(loc)
	currentDate := uint32(now.Year()*10000 + int(now.Month())*100 + now.Day())
	if date == currentDate {
		preClose, err := source.Quote(market, code)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(preClose) || math.IsInf(preClose, 0) || preClose <= 0 {
			return 0, fmt.Errorf("invalid realtime preClose: %v", preClose)
		}
		return preClose, nil
	}

	bars, err := source.DailyBars(market, code, target)
	if err != nil {
		return 0, err
	}
	if len(bars) == 0 {
		return 0, errors.New("target daily bar not found")
	}
	preClose := SecurityBarPreClose(bars[0])
	if math.IsNaN(preClose) || math.IsInf(preClose, 0) || preClose <= 0 {
		return 0, fmt.Errorf("invalid historical preClose: %v", preClose)
	}
	return preClose, nil
}

// SecurityBarPreClose 读取日线昨收：优先 PreClose，其次 LastClose。
func SecurityBarPreClose(bar proto.SecurityBar) float64 {
	if bar.PreClose > 0 {
		return bar.PreClose
	}
	return bar.LastClose
}

// ExPreCloseSource 扩展行情分时昨收依赖，now/quote/dailyBars 可测注入。
type ExPreCloseSource struct {
	Now       func() time.Time
	Quote     func(category uint8, code string) (float64, error)
	DailyBars func(category uint8, code string, date time.Time) ([]proto.ExKLineItem, error)
}

// ResolveExPreClose 解析扩展行情分时昨收：目标日线为 SSOT，实时行情仅作当日日线缺失时的回退。
// 不能优先实时行情：gotdx ExQuote.PreClose 对美股返回当日现价（非昨收），港股则常为 0，均不可靠；
// 日线 ExKLine 的 PreClose 经实测与交易日历一致。
// 日线昨收读取顺序：PreClose → LastClose → Open。
func ResolveExPreClose(category uint8, code string, date uint32, source ExPreCloseSource) (float64, error) {
	year := int(date / 10000)
	month := time.Month((date % 10000) / 100)
	day := int(date % 100)
	loc := time.FixedZone("CST", 8*60*60)
	target := time.Date(year, month, day, 0, 0, 0, 0, loc)
	if target.Year() != year || target.Month() != month || target.Day() != day {
		return 0, fmt.Errorf("invalid history date: %d", date)
	}

	bars, dailyErr := source.DailyBars(category, code, target)
	if dailyErr == nil && len(bars) > 0 {
		preClose := ExKLinePreClose(bars[0])
		if math.IsNaN(preClose) || math.IsInf(preClose, 0) || preClose <= 0 {
			return 0, fmt.Errorf("invalid daily preClose: %v", preClose)
		}
		return preClose, nil
	}

	// 当日日线缺失（如盘前尚无当日 bar）回退实时行情；历史日无回退。
	now := source.Now().In(loc)
	currentDate := uint32(now.Year()*10000 + int(now.Month())*100 + now.Day())
	if date == currentDate {
		preClose, quoteErr := source.Quote(category, code)
		if quoteErr == nil && !math.IsNaN(preClose) && !math.IsInf(preClose, 0) && preClose > 0 {
			return preClose, nil
		}
	}
	if dailyErr != nil {
		return 0, dailyErr
	}
	return 0, errors.New("target daily bar not found")
}

// ExKLinePreClose 读取扩展日线昨收：PreClose → LastClose → Open。
func ExKLinePreClose(bar proto.ExKLineItem) float64 {
	if bar.PreClose > 0 {
		return bar.PreClose
	}
	if bar.LastClose > 0 {
		return bar.LastClose
	}
	return bar.Open
}

// BuildStockTimeSharePoints 构建 A 股/指数历史分时点，含开盘首笔补入与指数成交额换算。
func BuildStockTimeSharePoints(req StockTimeShareRequest, fetchTick StockHistoryTickFetcher, fetchOpening OpeningTradeFetcher, now func() time.Time) ([]TimeSharePoint, error) {
	tick, err := fetchTick(req.Date, req.Market, req.Code)
	if err != nil {
		return nil, err
	}
	year := int(req.Date / 10000)
	month := int((req.Date % 10000) / 100)
	day := int(req.Date % 100)
	loc := time.FixedZone("CST", 8*60*60)
	base := time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc)
	isIndex := IsIndexKind(req.Kind, req.Market, req.Code)

	var first *TimeSharePoint
	if len(tick) > 0 {
		if trade, ok := fetchOpening(req.Date, req.Market, req.Code, now()); ok {
			p := &TimeSharePoint{
				At:    base.Add(9*time.Hour + 30*time.Minute),
				Price: trade.Price,
				Avg:   trade.Price,
			}
			if isIndex {
				// 指数逐笔 Vol 为百元，统一转换为元再向前端传递。
				amount := int64(trade.Vol) * 100
				p.Amount = &amount
			} else {
				volume := trade.Vol
				p.Volume = &volume
			}
			first = p
		}
	}

	points := make([]TimeSharePoint, 0, len(tick)+1)
	if first != nil {
		points = append(points, *first)
	}
	for i, item := range tick {
		var t time.Time
		if i < 120 {
			t = base.Add(9*time.Hour + 31*time.Minute + time.Duration(i)*time.Minute)
		} else {
			t = base.Add(13*time.Hour + 1*time.Minute + time.Duration(i-120)*time.Minute)
		}
		p := TimeSharePoint{At: t, Price: item.Price, Avg: item.Avg}
		if isIndex {
			// 指数分钟 Vol 为万元，统一转换为元；首分钟包含集合竞价。
			amount := int64(item.Vol) * 10_000
			if i == 0 && first != nil && first.Amount != nil {
				amount = max(0, amount-*first.Amount)
			}
			p.Amount = &amount
		} else {
			volume := item.Vol
			if i == 0 && first != nil && first.Volume != nil {
				// gotdx 首分钟包含集合竞价，补出 09:30 后需从 09:31 扣除以避免重复。
				volume = max(0, volume-*first.Volume)
			}
			p.Volume = &volume
		}
		points = append(points, p)
	}
	return points, nil
}

// BuildExTimeSharePoints 构建扩展行情分时点，全零点视为无历史数据。
func BuildExTimeSharePoints(req ExTimeShareRequest, fetchTick ExHistoryTickFetcher) ([]TimeSharePoint, error) {
	tick, err := fetchTick(req.Date, req.Category, req.Code)
	if err != nil {
		return nil, err
	}
	if len(tick) > 0 {
		allZero := true
		for _, item := range tick {
			if item.Price != 0 || item.Avg != 0 || item.Vol != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return nil, ErrNoTimeShareData
		}
	}

	year := int(req.Date / 10000)
	month := int((req.Date % 10000) / 100)
	day := int(req.Date % 100)
	loc := time.FixedZone("CST", 8*60*60)
	base := time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc)

	points := make([]TimeSharePoint, 0, len(tick))
	for _, item := range tick {
		ts, err := ParseExTickClock(base, item.Time)
		if err != nil {
			return nil, err
		}
		points = append(points, TimeSharePoint{At: ts, Price: item.Price, Avg: item.Avg})
	}
	return points, nil
}

// ParseExTickClock 将 gotdx "HH:mm" 接到目标日 Asia/Shanghai 墙钟
func ParseExTickClock(base time.Time, clock string) (time.Time, error) {
	parts := strings.Split(clock, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("expected HH:mm, got %q", clock)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("out of range HH:mm %q", clock)
	}
	return time.Date(base.Year(), base.Month(), base.Day(), hour, minute, 0, 0, base.Location()), nil
}

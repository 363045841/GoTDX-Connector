// V1 协议分时接口：委托 domain 的点构建与昨收解析，映射为 V1 序列。
package v1

import (
	"errors"
	"log"
	"math"
	"net/http"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
)

// v1MaxTimeShareRangeDays 限制单次多日分时请求，避免耗尽 GOTDX TCP 查询容量。
const v1MaxTimeShareRangeDays = 20

// v1TimeShareRequest V1 分时请求；tradingDate 为品种时区内的 YYYY-MM-DD 交易日。
type v1TimeShareRequest struct {
	SourceID    string                `json:"sourceId"`
	Instrument  v1InstrumentReference `json:"instrument"`
	TradingDate string                `json:"tradingDate"`
}

// v1TimeShareItem V1 分时条目。
type v1TimeShareItem struct {
	Timestamp int64   `json:"timestamp"`
	Price     float64 `json:"price"`
	Average   float64 `json:"average"`
	Volume    *int    `json:"volume,omitempty"`
	Amount    *int64  `json:"amount,omitempty"`
}

// v1TimeShareSeries V1 分时序列。
type v1TimeShareSeries struct {
	InstrumentID string            `json:"instrumentId"`
	TradingDate  string            `json:"tradingDate"`
	Timezone     string            `json:"timezone"`
	PreClose     float64           `json:"preClose"`
	VolumeUnit   string            `json:"volumeUnit,omitempty"`
	Items        []v1TimeShareItem `json:"items"`
}

// v1TimeShareRangeRequest 多日分时请求；endTradingDate 包含在结果内，days 按实际交易日计数。
type v1TimeShareRangeRequest struct {
	SourceID       string                `json:"sourceId"`
	Instrument     v1InstrumentReference `json:"instrument"`
	EndTradingDate string                `json:"endTradingDate"`
	Days           int                   `json:"days"`
}

// v1TimeShareDay 多日分时中的单个交易日，preClose 不可跨日复用。
type v1TimeShareDay struct {
	TradingDate string            `json:"tradingDate"`
	PreClose    float64           `json:"preClose"`
	Items       []v1TimeShareItem `json:"items"`
}

// v1TimeShareRangeSeries 多日分时响应，days 按交易日升序排列。
type v1TimeShareRangeSeries struct {
	InstrumentID  string           `json:"instrumentId"`
	Timezone      string           `json:"timezone"`
	RequestedDays int              `json:"requestedDays"`
	Days          []v1TimeShareDay `json:"days"`
	OlderData     string           `json:"olderData"`
}

// v1ParseTradingDate 解析 YYYY-MM-DD 交易日为 YYYYMMDD 整数。
func v1ParseTradingDate(value string) (uint32, error) {
	t, err := time.ParseInLocation("2006-01-02", value, time.FixedZone("CST", 8*60*60))
	if err != nil {
		return 0, err
	}
	return uint32(t.Year()*10000 + int(t.Month())*100 + t.Day()), nil
}

// v1TradingDateTime 将 V1 交易日转换为 CST 日起点，供日线游标与分时日期复用。
func v1TradingDateTime(value string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02", value, time.FixedZone("CST", 8*60*60))
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// v1TimeShareItems 将领域分时点映射为 V1 点列。
func v1TimeShareItems(points []domain.TimeSharePoint) []v1TimeShareItem {
	items := make([]v1TimeShareItem, 0, len(points))
	for _, p := range points {
		items = append(items, v1TimeShareItem{
			Timestamp: p.At.UnixMilli(),
			Price:     p.Price,
			Average:   p.Avg,
			Volume:    p.Volume,
			Amount:    p.Amount,
		})
	}
	return items
}

// v1ValidPreClose 校验日线提供的昨收可作为分时百分比基准。
func v1ValidPreClose(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// writeV1UpstreamError 记录上游细节并给客户端返回稳定错误。
func writeV1UpstreamError(c *gin.Context, err error) {
	log.Printf("timeshare query failed: request_id=%s error=%v", requestID(c), err)
	writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, "upstream request failed")
}

// v1StockRangeDays 以截止交易日为上界取最近 days 根日线，日线天然跳过非交易日。
func v1StockRangeDays(fetch domain.Fetcher, market uint8, code, kind string, end time.Time, days int) ([]proto.SecurityBar, error) {
	before := end.AddDate(0, 0, 1)
	if domain.IsIndexKind(kind, market, code) {
		return domain.IndexKLineBefore(fetch, 4, market, code, days, &before)
	}
	return domain.StockKLineBefore(fetch, 4, market, code, 1, 0, days, &before)
}

// v1ExRangeDays 以截止交易日为上界取最近 days 根扩展行情日线。
func v1ExRangeDays(fetch domain.Fetcher, category uint8, code string, end time.Time, days int) ([]proto.ExKLineItem, error) {
	before := end.AddDate(0, 0, 1)
	return domain.ExKLineBefore(fetch, category, code, 4, 1, days, &before)
}

// handleV1TimeShareRange 拉取指定品种最近多个实际交易日的分时序列。
func handleV1TimeShareRange(fetch domain.Fetcher) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req v1TimeShareRangeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
			return
		}
		if req.SourceID != "" && req.SourceID != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, "unknown sourceId "+req.SourceID)
			return
		}
		if req.Days < 1 || req.Days > v1MaxTimeShareRangeDays {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "days must be between 1 and 20")
			return
		}
		end, err := v1TradingDateTime(req.EndTradingDate)
		if err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid endTradingDate: "+err.Error())
			return
		}
		resolved, apiErr, status := resolveInstrument(req.Instrument, nil)
		if apiErr != nil {
			writeV1APIError(c, apiErr, status)
			return
		}
		timezone := v1ExchangeToTimezone(resolved.Exchange)
		days := make([]v1TimeShareDay, 0, req.Days)
		if resolved.Routing.Kind == instrument.Ex {
			bars, err := v1ExRangeDays(fetch, resolved.Routing.Category, resolved.Symbol, end, req.Days)
			if err != nil {
				writeV1UpstreamError(c, err)
				return
			}
			for _, bar := range bars {
				preClose := domain.ExKLinePreClose(bar)
				if !v1ValidPreClose(preClose) {
					writeV1Error(c, http.StatusBadGateway, v1CodeInvalidResponse, "invalid historical preClose")
					return
				}
				date := uint32(bar.DateTime.Year()*10000 + int(bar.DateTime.Month())*100 + bar.DateTime.Day())
				points, err := domain.BuildExTimeSharePoints(
					domain.ExTimeShareRequest{Date: date, Category: resolved.Routing.Category, Code: resolved.Symbol},
					fetch.ExTick,
				)
				if err != nil {
					writeV1UpstreamError(c, err)
					return
				}
				days = append(days, v1TimeShareDay{TradingDate: bar.DateTime.Format("2006-01-02"), PreClose: preClose, Items: v1TimeShareItems(points)})
			}
		} else {
			kind := string(resolved.Routing.Kind)
			bars, err := v1StockRangeDays(fetch, resolved.Routing.Market, resolved.Symbol, kind, end, req.Days)
			if err != nil {
				writeV1UpstreamError(c, err)
				return
			}
			for _, bar := range bars {
				preClose := domain.SecurityBarPreClose(bar)
				if !v1ValidPreClose(preClose) {
					writeV1Error(c, http.StatusBadGateway, v1CodeInvalidResponse, "invalid historical preClose")
					return
				}
				date := uint32(bar.DateTime.Year()*10000 + int(bar.DateTime.Month())*100 + bar.DateTime.Day())
				points, err := domain.BuildStockTimeSharePoints(
					domain.StockTimeShareRequest{Date: date, Market: resolved.Routing.Market, Code: resolved.Symbol, Kind: kind},
					fetch.StockTick,
					fetch.OpeningTrade,
					time.Now,
				)
				if err != nil {
					writeV1UpstreamError(c, err)
					return
				}
				days = append(days, v1TimeShareDay{TradingDate: bar.DateTime.Format("2006-01-02"), PreClose: preClose, Items: v1TimeShareItems(points)})
			}
		}

		writeV1Data(c, http.StatusOK, v1TimeShareRangeSeries{
			InstrumentID:  resolved.ID,
			Timezone:      timezone,
			RequestedDays: req.Days,
			Days:          days,
			OlderData:     v1TimeShareRangeOlderData(len(days), req.Days),
		})
	}
}

// v1TimeShareRangeOlderData 声明历史日线不足请求天数时已到达可用历史边界。
func v1TimeShareRangeOlderData(returnedDays, requestedDays int) string {
	if returnedDays < requestedDays {
		return "exhausted"
	}
	return "unknown"
}

// handleV1Timeshare 拉取指定品种在单个交易日内的分时序列。
func handleV1Timeshare(fetch domain.Fetcher) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req v1TimeShareRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
			return
		}
		if req.SourceID != "" && req.SourceID != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, "unknown sourceId "+req.SourceID)
			return
		}
		date, err := v1ParseTradingDate(req.TradingDate)
		if err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid tradingDate: "+err.Error())
			return
		}
		resolved, apiErr, status := resolveInstrument(req.Instrument, nil)
		if apiErr != nil {
			writeV1APIError(c, apiErr, status)
			return
		}
		timezone := v1ExchangeToTimezone(resolved.Exchange)

		var points []domain.TimeSharePoint
		var preClose float64
		switch resolved.Routing.Kind {
		case instrument.Ex:
			points, err = domain.BuildExTimeSharePoints(
				domain.ExTimeShareRequest{Date: date, Category: resolved.Routing.Category, Code: resolved.Symbol},
				fetch.ExTick,
			)
			if err != nil {
				if errors.Is(err, domain.ErrNoTimeShareData) {
					writeV1Error(c, http.StatusNotFound, v1CodeInstrumentNotFound, "no timeshare data for the date")
				} else {
					writeV1UpstreamError(c, err)
				}
				return
			}
			preClose, err = domain.ResolveExPreClose(
				resolved.Routing.Category, resolved.Symbol, date, domain.NewExPreCloseSource(fetch),
			)
			if err != nil {
				writeV1UpstreamError(c, err)
				return
			}
		default:
			stockReq := domain.StockTimeShareRequest{
				Date: date, Market: resolved.Routing.Market, Code: resolved.Symbol, Kind: string(resolved.Routing.Kind),
			}
			points, err = domain.BuildStockTimeSharePoints(stockReq, fetch.StockTick, fetch.OpeningTrade, time.Now)
			if err != nil {
				writeV1UpstreamError(c, err)
				return
			}
			isIndex := resolved.Routing.Kind == instrument.Index
			preClose, err = domain.ResolveStockPreClose(
				resolved.Routing.Market, resolved.Symbol, date,
				domain.NewStockPreCloseSource(fetch, resolved.Routing.Market, resolved.Symbol, isIndex),
			)
			if err != nil {
				writeV1UpstreamError(c, err)
				return
			}
		}

		writeV1Data(c, http.StatusOK, v1TimeShareSeries{
			InstrumentID: resolved.ID,
			TradingDate:  req.TradingDate,
			Timezone:     timezone,
			PreClose:     preClose,
			Items:        v1TimeShareItems(points),
		})
	}
}

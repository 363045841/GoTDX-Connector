// V1 协议 K 线接口：按统一解析出的路由分发到股票/指数/扩展行情，并映射为 V1 序列。
package v1

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	"github.com/gin-gonic/gin"
)

// v1MaxBarsPerRequest 是 HTTP 校验与能力声明共用的单页根数上限。
const v1MaxBarsPerRequest = 798

// writeV1BarsError 统一映射 K 线查询错误；上游细节只写日志，不下发给客户端。
func writeV1BarsError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, domain.ErrNoKLineData):
		writeV1Error(c, http.StatusNotFound, v1CodeInstrumentNotFound, "no kline data for instrument")
	case errors.Is(err, client.ErrMalformedReply):
		log.Printf("bars query aborted on malformed upstream reply: request_id=%s error=%v", requestID(c), err)
		writeV1Error(c, http.StatusBadGateway, v1CodeInvalidResponse, "upstream reply is malformed")
	default:
		log.Printf("bars query failed: request_id=%s error=%v", requestID(c), err)
		writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, "upstream request failed")
	}
}

// v1BarRequest V1 K 线请求；beforeTimestamp 为可选 UTC Unix 毫秒排他游标。
type v1BarRequest struct {
	SourceID        string                `json:"sourceId"`
	Instrument      v1InstrumentReference `json:"instrument"`
	Period          string                `json:"period"`
	Adjustment      string                `json:"adjustment"`
	Limit           int                   `json:"limit"`
	BeforeTimestamp *int64                `json:"beforeTimestamp,omitempty"`
}

// v1BarSeries V1 K 线序列。
type v1BarSeries struct {
	InstrumentID string        `json:"instrumentId"`
	Period       string        `json:"period"`
	Adjustment   string        `json:"adjustment"`
	Timezone     string        `json:"timezone"`
	VolumeUnit   string        `json:"volumeUnit,omitempty"`
	Items        []v1KLineItem `json:"items"`
	OlderData    string        `json:"olderData"`
}

// v1OlderData 返回可证明的历史状态；无法确认时保持 unknown，禁止前端自行推断。
func v1OlderData(itemCount, limit int) string {
	if itemCount < limit {
		return "exhausted"
	}
	return "unknown"
}

// handleV1Bars 拉取指定品种、周期和 cursor 页的 K 线。
func handleV1Bars(fetch domain.Fetcher) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req v1BarRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
			return
		}
		if req.SourceID != "" && req.SourceID != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, "unknown sourceId "+req.SourceID)
			return
		}
		if req.Limit < 1 || req.Limit > v1MaxBarsPerRequest {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest,
				fmt.Sprintf("limit must be between 1 and %d", v1MaxBarsPerRequest))
			return
		}
		resolved, apiErr, status := resolveInstrument(req.Instrument, nil)
		if apiErr != nil {
			writeV1APIError(c, apiErr, status)
			return
		}
		category, ok := v1PeriodToCategory[req.Period]
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability,
				"period "+req.Period+" is not supported")
			return
		}
		if !v1AdjustmentSupported(string(resolved.Routing.Kind), req.Adjustment) {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability,
				"adjustment "+req.Adjustment+" is not supported for kind "+string(resolved.Routing.Kind))
			return
		}
		timezone := v1ExchangeToTimezone(resolved.Exchange)
		adjust := v1AdjustToUint[req.Adjustment]
		var before *time.Time
		if req.BeforeTimestamp != nil {
			cursor := time.UnixMilli(*req.BeforeTimestamp).UTC()
			before = &cursor
		}

		started := time.Now()
		// 查询可能阻塞在共享客户端锁上，未返回错误时也要留下诊断信息。
		slowQuery := time.AfterFunc(10*time.Second, func() {
			log.Printf("bars query still pending: request_id=%s instrument=%s symbol=%s period=%s routing=%v limit=%d elapsed=%s",
				requestID(c), resolved.ID, resolved.Symbol, req.Period, resolved.Routing, req.Limit,
				time.Since(started).Round(time.Millisecond))
		})
		defer slowQuery.Stop()

		items := make([]v1KLineItem, 0)
		switch resolved.Routing.Kind {
		case instrument.Index:
			bars, err := domain.IndexKLineBefore(fetch, category, resolved.Routing.Market, resolved.Symbol, req.Limit, before)
			if err != nil {
				if req.BeforeTimestamp != nil && errors.Is(err, domain.ErrNoKLineData) {
					break
				}
				writeV1BarsError(c, err)
				return
			}
			for _, bar := range bars {
				items = append(items, securityBarToV1KLine(bar))
			}
		case instrument.Ex:
			bars, err := domain.ExKLineBefore(fetch, resolved.Routing.Category, resolved.Symbol, category, 1, req.Limit, before)
			if err != nil {
				if req.BeforeTimestamp != nil && errors.Is(err, domain.ErrNoKLineData) {
					break
				}
				writeV1BarsError(c, err)
				return
			}
			for _, bar := range bars {
				items = append(items, exKLineToV1KLine(bar))
			}
		default:
			bars, err := domain.StockKLineBefore(fetch, category, resolved.Routing.Market, resolved.Symbol, 1, adjust, req.Limit, before)
			if err != nil {
				if req.BeforeTimestamp != nil && errors.Is(err, domain.ErrNoKLineData) {
					break
				}
				writeV1BarsError(c, err)
				return
			}
			for _, bar := range bars {
				items = append(items, securityBarToV1KLine(bar))
			}
		}

		writeV1Data(c, http.StatusOK, v1BarSeries{
			InstrumentID: resolved.ID,
			Period:       req.Period,
			Adjustment:   req.Adjustment,
			Timezone:     timezone,
			Items:        items,
			OlderData:    v1OlderData(len(items), req.Limit),
		})
	}
}

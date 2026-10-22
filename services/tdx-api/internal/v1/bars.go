// V1 协议 K 线接口：按 providerRef 的 kind 路由到股票/指数/扩展行情，并映射为 V1 序列。
package v1

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"github.com/gin-gonic/gin"
)

// writeV1BarsError 统一映射 K 线查询错误：无数据返回确定性可流转码，上游故障返回网关错误。
func writeV1BarsError(c *gin.Context, err error) {
	if errors.Is(err, domain.ErrNoKLineData) {
		writeV1Error(c, http.StatusNotFound, v1CodeInstrumentNotFound, err.Error())
		return
	}
	writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, err.Error())
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
func handleV1Bars(c *gin.Context) {
	var req v1BarRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Instrument.Symbol == "" {
		writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "instrument.symbol is required")
		return
	}
	if req.Limit < 1 || req.Limit > 798 {
		writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "limit must be between 1 and 798")
		return
	}
	timezone := v1ExchangeToTimezone(req.Instrument.Exchange)
	var before *time.Time
	if req.BeforeTimestamp != nil {
		cursor := time.UnixMilli(*req.BeforeTimestamp).UTC()
		before = &cursor
	}
	ref := req.Instrument.ProviderRef
	kind := v1ProviderRefKind(ref)
	category, ok := v1PeriodToCategory[req.Period]
	if !ok {
		writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability,
			fmt.Sprintf("period %q is not supported for kind %q", req.Period, kind))
		return
	}
	if !v1AdjustmentSupported(kind, req.Adjustment) {
		writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability,
			fmt.Sprintf("adjustment %q is not supported for kind %q", req.Adjustment, kind))
		return
	}
	adjust := v1AdjustToUint[req.Adjustment]

	items := make([]v1KLineItem, 0)
	switch kind {
	case domain.KindIndex:
		market, ok := v1ProviderRefNumber(ref, "market")
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "providerRef.market is required for index")
			return
		}
		bars, err := domain.IndexKLineBefore(category, uint8(market), req.Instrument.Symbol, req.Limit, before)
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
	case domain.KindEx:
		cat, ok := v1ProviderRefNumber(ref, "category")
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "providerRef.category is required for ex")
			return
		}
		bars, err := domain.ExKLineBefore(uint8(cat), req.Instrument.Symbol, category, 1, req.Limit, before)
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
		market, ok := v1ProviderRefNumber(ref, "market")
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "providerRef.market is required")
			return
		}
		bars, err := domain.StockKLineBefore(category, uint8(market), req.Instrument.Symbol, 1, adjust, req.Limit, before)
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
		InstrumentID: req.Instrument.ID,
		Period:       req.Period,
		Adjustment:   req.Adjustment,
		Timezone:     timezone,
		Items:        items,
		OlderData:    v1OlderData(len(items), req.Limit),
	})
}

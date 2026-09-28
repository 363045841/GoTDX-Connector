// V1 行情协议路由层：统一 envelope 包装、数据源探测与品种目录搜索。
// V1 为前端唯一数据接入层，接口契约以 KLineChartQuant 前端 api/types.ts 为准。
package v1

import (
	"net/http"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// V1 错误码，与前端 MarketDataErrorCode / OpenAPI ApiError 枚举对齐。
const (
	v1CodeInvalidRequest        = "INVALID_REQUEST"
	v1CodeUnsupportedCapability = "UNSUPPORTED_CAPABILITY"
	v1CodeInstrumentNotFound    = "INSTRUMENT_NOT_FOUND"
	v1CodeUpstreamUnavailable   = "UPSTREAM_UNAVAILABLE"
	v1CodeInvalidResponse       = "INVALID_RESPONSE"
	v1CodeInternal              = "INTERNAL"
)

// 品种搜索分页限制，独立于旧 /api/symbol/search 接口。
const (
	defaultSearchLimit = 20
	maxSearchLimit     = 100
)

// v1Envelope V1 成功响应外壳。
type v1Envelope struct {
	Data      any    `json:"data"`
	RequestID string `json:"requestId"`
}

// v1APIError V1 错误信息。
type v1APIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// v1ErrorEnvelope V1 错误响应外壳。
type v1ErrorEnvelope struct {
	Error     v1APIError `json:"error"`
	RequestID string     `json:"requestId"`
}

// v1SourceProbe 数据源探测结果，与前端 V1SourceProbe 对齐。
type v1SourceProbe struct {
	Status       string                `json:"status"`
	CheckedAt    int64                 `json:"checkedAt"`
	LatencyMs    int64                 `json:"latencyMs,omitempty"`
	Message      string                `json:"message,omitempty"`
	Capabilities *v1SourceCapabilities `json:"capabilities,omitempty"`
}

// v1InstrumentSearchRequest 品种目录搜索请求。
type v1InstrumentSearchRequest struct {
	SourceID     string   `json:"sourceId"`
	Keyword      string   `json:"keyword"`
	Limit        int      `json:"limit"`
	AssetClasses []string `json:"assetClasses,omitempty"`
}

// v1InstrumentSearchResult 品种目录搜索结果。
type v1InstrumentSearchResult struct {
	Items []v1InstrumentDescriptor `json:"items"`
}

// writeV1Data 输出 V1 成功 envelope。
func writeV1Data(c *gin.Context, status int, data any) {
	c.JSON(status, v1Envelope{Data: data, RequestID: uuid.NewString()})
}

// writeV1Error 输出 V1 错误 envelope。
func writeV1Error(c *gin.Context, status int, code, message string) {
	c.JSON(status, v1ErrorEnvelope{
		Error:     v1APIError{Code: code, Message: message},
		RequestID: uuid.NewString(),
	})
}

// WriteInternalError 输出 V1 INTERNAL 错误 envelope，供恢复中间件对 V1 路径统一响应。
func WriteInternalError(c *gin.Context) {
	writeV1Error(c, http.StatusInternalServerError, v1CodeInternal, "internal server error")
	c.Abort()
}

// RegisterRoutes 注册 V1 行情协议路由。
func RegisterRoutes(r *gin.Engine, symbolCache *directory.Cache, status func() client.Status) {
	v1 := r.Group("/api/v1/market-data")
	{
		v1.GET("/sources/:sourceId/probe", handleV1Probe(status))
		v1.POST("/instruments/search", handleV1Search(symbolCache))
		v1.POST("/bars", handleV1Bars)
		v1.POST("/trading-calendar", handleV1TradingCalendar)
		v1.POST("/timeshare", handleV1Timeshare)
		v1.POST("/timeshare/range", handleV1TimeShareRange)
	}
}

// v1HeartbeatProbe 一次主站心跳探测，返回 nil 表示可用。
type v1HeartbeatProbe func() error

// handleV1Probe 探测数据源可用性：一次主站心跳测延迟，失败记为离线，成功且全域就绪记为在线。
func handleV1Probe(status func() client.Status) gin.HandlerFunc {
	return handleV1ProbeWithDeps(status, func() error {
		_, err := client.ProbeMain(func(q client.MainQuerier) (*proto.HeartBeatReply, error) {
			return q.GetServerHeartbeat()
		})
		return err
	})
}

// handleV1ProbeWithDeps 可注入心跳探测的分时变体，供测试使用。
func handleV1ProbeWithDeps(status func() client.Status, heartbeat v1HeartbeatProbe) gin.HandlerFunc {
	return func(c *gin.Context) {
		startedAt := time.Now()
		err := heartbeat()
		checkedAt := time.Now().UnixMilli()
		latency := time.Since(startedAt).Milliseconds()
		caps := v1SourceCapabilitiesFor()
		if err != nil {
			writeV1Data(c, http.StatusOK, v1SourceProbe{
				Status: "offline", CheckedAt: checkedAt, LatencyMs: latency, Message: err.Error(),
				Capabilities: &caps,
			})
			return
		}
		st := status()
		resultStatus := "online"
		if !st.Ready {
			resultStatus = "degraded"
		}
		writeV1Data(c, http.StatusOK, v1SourceProbe{
			Status: resultStatus, CheckedAt: checkedAt, LatencyMs: latency,
			Capabilities: &caps,
		})
	}
}

// handleV1Search 按关键字搜索标准品种目录并映射为 V1 品种描述。
func handleV1Search(cache *directory.Cache) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req v1InstrumentSearchRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
			return
		}
		keyword := strings.TrimSpace(req.Keyword)
		if keyword == "" {
			writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "keyword is required")
			return
		}
		if cache == nil {
			writeV1Error(c, http.StatusServiceUnavailable, v1CodeUpstreamUnavailable, "symbol directory unavailable")
			return
		}
		limit := req.Limit
		if limit <= 0 {
			limit = defaultSearchLimit
		}
		if limit > maxSearchLimit {
			limit = maxSearchLimit
		}
		items, err := cache.Search(keyword, limit)
		if err != nil {
			writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, err.Error())
			return
		}
		result := make([]v1InstrumentDescriptor, 0, len(items))
		for _, item := range items {
			result = append(result, toV1Instrument(item))
		}
		writeV1Data(c, http.StatusOK, v1InstrumentSearchResult{Items: result})
	}
}

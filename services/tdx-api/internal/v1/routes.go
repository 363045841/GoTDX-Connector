// V1 行情协议路由层：统一 envelope 包装、数据源探测与品种目录搜索。
// 依赖由 main 装配后注入，路由层不读取环境变量、不构造资源。
package v1

import (
	"log"
	"net/http"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/live"
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

// 品种搜索分页限制。
const (
	defaultSearchLimit = 20
	maxSearchLimit     = 100
)

// Deps V1 路由的全部外部依赖。
type Deps struct {
	Fetch  domain.Fetcher
	Cache  *directory.Cache
	Status func() client.Status
	Probe  func() error
	Hub    *live.Hub
}

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

// requestIDContextKey 请求 ID 在 gin.Context 中的键。
const requestIDContextKey = "v1.requestID"

// RequestIDMiddleware 为每个请求生成或透传请求 ID，并写入响应头。
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.GetHeader("X-Request-Id"))
		if id == "" {
			id = uuid.NewString()
		}
		c.Set(requestIDContextKey, id)
		c.Header("X-Request-Id", id)
		c.Next()
	}
}

// RequestID 返回当前请求 ID，供中间件日志与其他包使用。
func RequestID(c *gin.Context) string { return requestID(c) }

// requestID 返回当前请求 ID。
func requestID(c *gin.Context) string {
	if value, ok := c.Get(requestIDContextKey); ok {
		if id, ok := value.(string); ok && id != "" {
			return id
		}
	}
	return uuid.NewString()
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
	SourceID        string   `json:"sourceId"`
	Keyword         string   `json:"keyword"`
	Limit           int      `json:"limit"`
	InstrumentTypes []string `json:"instrumentTypes,omitempty"`
}

// v1InstrumentSearchResult 品种目录搜索结果。
type v1InstrumentSearchResult struct {
	Items []v1InstrumentDescriptor `json:"items"`
}

// writeV1Data 输出 V1 成功 envelope。
func writeV1Data(c *gin.Context, status int, data any) {
	c.JSON(status, v1Envelope{Data: data, RequestID: requestID(c)})
}

// writeV1Error 输出 V1 错误 envelope。
func writeV1Error(c *gin.Context, status int, code, message string) {
	writeV1APIError(c, &v1APIError{Code: code, Message: message}, status)
}

// writeV1APIError 输出带详情与状态码的 V1 错误 envelope。
func writeV1APIError(c *gin.Context, apiErr *v1APIError, status int) {
	c.JSON(status, v1ErrorEnvelope{Error: *apiErr, RequestID: requestID(c)})
}

// WriteInternalError 输出 V1 INTERNAL 错误 envelope，供恢复中间件对 V1 路径统一响应。
func WriteInternalError(c *gin.Context) {
	writeV1Error(c, http.StatusInternalServerError, v1CodeInternal, "internal server error")
	c.Abort()
}

// RegisterRoutes 注册 V1 行情协议路由。
func RegisterRoutes(r *gin.Engine, deps Deps) {
	group := r.Group("/api/v1/market-data")
	{
		group.GET("/sources/:sourceId/probe", handleV1Probe(deps.Status, deps.Probe))
		group.GET("/sources/:sourceId/stream", handleV1Stream(deps.Hub, deps.Cache, deps.Fetch))
		group.GET("/sources/:sourceId/quotes", handleV1Quotes(deps.Hub, deps.Cache, deps.Fetch))
		group.POST("/instruments/search", handleV1Search(deps.Cache))
		group.POST("/bars", handleV1Bars(deps.Fetch))
		group.POST("/trading-calendar", handleV1TradingCalendar)
		group.POST("/timeshare", handleV1Timeshare(deps.Fetch))
		group.POST("/timeshare/range", handleV1TimeShareRange(deps.Fetch))
	}
}

// v1HeartbeatProbe 一次主站心跳探测，返回 nil 表示可用。
type v1HeartbeatProbe func() error

// handleV1Probe 探测数据源可用性：一次主站心跳测延迟，失败记为离线，成功且全域就绪记为在线。
func handleV1Probe(status func() client.Status, probe v1HeartbeatProbe) gin.HandlerFunc {
	return handleV1ProbeWithDeps(status, probe)
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
			Status: resultStatus, CheckedAt: checkedAt, LatencyMs: latency, Capabilities: &caps,
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
		if req.SourceID != "" && req.SourceID != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, "unknown sourceId "+req.SourceID)
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
			log.Printf("instrument search failed: request_id=%s error=%v", requestID(c), err)
			writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, "symbol directory unavailable")
			return
		}
		result := make([]v1InstrumentDescriptor, 0, len(items))
		for _, item := range items {
			result = append(result, toV1Instrument(item))
		}
		writeV1Data(c, http.StatusOK, v1InstrumentSearchResult{Items: result})
	}
}

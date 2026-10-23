// V1 实时 K 线 SSE 接口：GET /sources/:sourceId/stream，按 KCQ BarsLiveSource 帧协议推送
// snapshot / forming / closed / status 帧；上游轮询由 live.Hub 按序列共享。
package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/live"
	"github.com/bensema/gotdx/proto"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// liveFetchSize 每次轮询拉取的尾部根数：比快照多一根，用于确认上一根的收线终值。
const liveFetchSize = 3

// liveHeartbeatInterval SSE 心跳注释间隔，防止代理与浏览器判定空闲断开。
const liveHeartbeatInterval = 15 * time.Second

// liveRetryMillis 建议 EventSource 断线后的重连等待。
const liveRetryMillis = 3000

// liveCodePattern 主市场证券代码格式。
var liveCodePattern = regexp.MustCompile(`^[0-9A-Za-z]{1,12}$`)

// liveTarget 已解析的实时订阅品种（仅主市场 A 股/指数）。
type liveTarget struct {
	Kind   string
	Market uint8
	Code   string
}

// instrumentID 返回与搜索接口一致的稳定品种 ID。
func (t liveTarget) instrumentID() string {
	return v1InstrumentID(t.Kind, int(t.Market), t.Code)
}

// liveFetchBuilder 为目标品种构造上游拉取函数；测试可替换以避免访问 gotdx。
type liveFetchBuilder func(target liveTarget, category, adjust uint16) live.FetchFunc

// liveStreamDeps 实时流处理器依赖。
type liveStreamDeps struct {
	hub       *live.Hub
	cache     *directory.Cache
	fetch     liveFetchBuilder
	session   live.SessionFunc
	heartbeat time.Duration
}

// liveHubConfigFromEnv 读取实时流环境变量：轮询间隔与订阅上限。
func liveHubConfigFromEnv() live.Config {
	cfg := live.DefaultConfig()
	if d, err := time.ParseDuration(os.Getenv("GOTDX_LIVE_POLL_INTERVAL")); err == nil && d >= 500*time.Millisecond {
		cfg.OpenInterval = d
	}
	if n, err := strconv.Atoi(os.Getenv("GOTDX_LIVE_MAX_STREAMS")); err == nil && n > 0 {
		cfg.MaxSubscriptions = n
	}
	if n, err := strconv.Atoi(os.Getenv("GOTDX_LIVE_MAX_SERIES")); err == nil && n > 0 {
		cfg.MaxSeries = n
	}
	return cfg
}

// handleV1Stream 创建生产用实时 K 线 SSE 处理器。
func handleV1Stream(hub *live.Hub, cache *directory.Cache) gin.HandlerFunc {
	return handleV1StreamWithDeps(liveStreamDeps{
		hub: hub, cache: cache, fetch: gotdxLiveFetch, session: cnLiveSession,
		heartbeat: liveHeartbeatInterval,
	})
}

// handleV1StreamWithDeps 可注入依赖的实时流处理器。
// 请求错误在建流前以 V1 错误 envelope 返回（非 200 时 EventSource 不再重连）；
// 建流后上游故障与休市只通过 status 帧表达，连接保持。
func handleV1StreamWithDeps(deps liveStreamDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Param("sourceId") != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, fmt.Sprintf("unknown sourceId %q", c.Param("sourceId")))
			return
		}
		symbol := c.Query("symbol")
		period := c.Query("period")
		category, ok := v1PeriodToCategory[period]
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability, fmt.Sprintf("period %q is not supported", period))
			return
		}
		target, apiErr, status := resolveLiveTarget(deps.cache, symbol, c.Query("instrumentId"), c.Query("exchange"))
		if apiErr != nil {
			c.JSON(status, v1ErrorEnvelope{Error: *apiErr, RequestID: uuid.NewString()})
			return
		}
		// 实时尾部的 qfq 与 none 数值一致（前复权以最新价为基准），默认 none；hfq 需显式指定。
		adjustment := c.DefaultQuery("adjustment", "none")
		if !v1AdjustmentSupported(target.Kind, adjustment) {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability,
				fmt.Sprintf("adjustment %q is not supported for kind %q", adjustment, target.Kind))
			return
		}
		adjust := v1AdjustToUint[adjustment]
		if adjustment == "qfq" {
			adjust = v1AdjustToUint["none"]
		}

		// barAggregation 与 bars 接口一致：gotdx 只提供上游原生周期边界，不参与序列身份。
		key := fmt.Sprintf("%s|%s|%d", target.instrumentID(), period, adjust)
		sub, err := deps.hub.Subscribe(key, live.Source{
			Fetch:   deps.fetch(target, category, adjust),
			Session: deps.session,
		})
		if err != nil {
			writeV1Error(c, http.StatusServiceUnavailable, v1CodeUpstreamUnavailable, err.Error())
			return
		}
		defer sub.Close()

		w := c.Writer
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		// 首包立即下发，触发 EventSource onopen 并冲开代理缓冲。
		if _, err := fmt.Fprintf(w, "retry: %d\n: connected %s\n\n", liveRetryMillis, target.instrumentID()); err != nil {
			return
		}
		w.Flush()

		ticker := time.NewTicker(deps.heartbeat)
		defer ticker.Stop()
		ctx := c.Request.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.Lost():
				// 慢消费者：断开让客户端重连并重新取快照
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
					return
				}
				w.Flush()
			case ev := <-sub.Events():
				if err := writeLiveFrame(w, symbol, period, ev); err != nil {
					return
				}
				w.Flush()
			}
		}
	}
}

// writeLiveFrame 把事件编码为一条无名 SSE data 消息。
func writeLiveFrame(w io.Writer, symbol, period string, ev live.Event) error {
	payload, err := encodeLiveFrame(symbol, period, ev)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

// encodeLiveFrame 按 KCQ LiveBarsFrame 编码 JSON；symbol/period 原样回显客户端请求值。
func encodeLiveFrame(symbol, period string, ev live.Event) ([]byte, error) {
	switch ev.Type {
	case live.EventSnapshot:
		bars := ev.Bars
		if bars == nil {
			bars = []live.Bar{}
		}
		return json.Marshal(struct {
			Type   live.EventType `json:"type"`
			Symbol string         `json:"symbol"`
			Period string         `json:"period"`
			Bars   []live.Bar     `json:"bars"`
		}{ev.Type, symbol, period, bars})
	case live.EventForming, live.EventClosed:
		return json.Marshal(struct {
			Type   live.EventType `json:"type"`
			Symbol string         `json:"symbol"`
			Period string         `json:"period"`
			Bar    live.Bar       `json:"bar"`
		}{ev.Type, symbol, period, ev.Bar})
	case live.EventStatus:
		return json.Marshal(struct {
			Type   live.EventType `json:"type"`
			Symbol string         `json:"symbol"`
			Period string         `json:"period"`
			Status string         `json:"status"`
			Detail string         `json:"detail,omitempty"`
		}{ev.Type, symbol, period, ev.Status, ev.Detail})
	default:
		return nil, fmt.Errorf("unknown live event type %q", ev.Type)
	}
}

// resolveLiveTarget 把请求解析为主市场品种。优先级：instrumentId（或 symbol 本身即 ID）→
// symbol 带 .SZ/.SH/.BJ 后缀 → exchange 参数 → 证券目录精确匹配（多义时要求消歧）。
// 返回的 HTTP 状态仅在 apiErr 非空时有意义。
func resolveLiveTarget(cache *directory.Cache, symbol, instrumentID, exchange string) (liveTarget, *v1APIError, int) {
	symbol = strings.TrimSpace(symbol)
	if instrumentID == "" && strings.HasPrefix(symbol, v1SourceID+":") {
		instrumentID = symbol
	}
	if instrumentID != "" {
		return parseLiveInstrumentID(instrumentID)
	}
	if symbol == "" {
		return liveTarget{}, &v1APIError{Code: v1CodeInvalidRequest, Message: "symbol is required"}, http.StatusBadRequest
	}
	code := symbol
	if dot := strings.LastIndexByte(symbol, '.'); dot > 0 {
		code, exchange = symbol[:dot], symbol[dot+1:]
	}
	if !liveCodePattern.MatchString(code) {
		return liveTarget{}, &v1APIError{Code: v1CodeInvalidRequest, Message: fmt.Sprintf("invalid symbol %q", symbol)}, http.StatusBadRequest
	}
	if exchange != "" {
		market, ok := liveMarketForExchange(exchange)
		if !ok {
			return liveTarget{}, &v1APIError{Code: v1CodeUnsupportedCapability,
				Message: fmt.Sprintf("live bars are not supported for exchange %q", exchange)}, http.StatusBadRequest
		}
		return liveTarget{Kind: domain.MainMarketSymbolKind(code, domain.MainExchange(market)), Market: market, Code: code}, nil, 0
	}
	return lookupLiveTarget(cache, code)
}

// parseLiveInstrumentID 解析 gotdx:{kind}:{market}:{code}；扩展行情不支持实时流。
func parseLiveInstrumentID(id string) (liveTarget, *v1APIError, int) {
	parts := strings.Split(id, ":")
	if len(parts) != 4 || parts[0] != v1SourceID {
		return liveTarget{}, &v1APIError{Code: v1CodeInvalidRequest, Message: fmt.Sprintf("invalid instrumentId %q", id)}, http.StatusBadRequest
	}
	kind, code := parts[1], parts[3]
	if kind != domain.KindStock && kind != domain.KindIndex {
		return liveTarget{}, &v1APIError{Code: v1CodeUnsupportedCapability,
			Message: fmt.Sprintf("live bars are not supported for kind %q", kind)}, http.StatusBadRequest
	}
	market, err := strconv.Atoi(parts[2])
	if err != nil || market < 0 || market > 2 || !liveCodePattern.MatchString(code) {
		return liveTarget{}, &v1APIError{Code: v1CodeInvalidRequest, Message: fmt.Sprintf("invalid instrumentId %q", id)}, http.StatusBadRequest
	}
	return liveTarget{Kind: kind, Market: uint8(market), Code: code}, nil, 0
}

// liveMarketForExchange 主市场交易所缩写到 gotdx market。
func liveMarketForExchange(exchange string) (uint8, bool) {
	switch strings.ToUpper(strings.TrimSpace(exchange)) {
	case "SZ":
		return 0, true
	case "SH":
		return 1, true
	case "BJ":
		return 2, true
	default:
		return 0, false
	}
}

// lookupLiveTarget 在证券目录中按代码精确匹配主市场品种；同码跨市场（如 000001 平安银行/上证指数）时拒绝猜测。
func lookupLiveTarget(cache *directory.Cache, code string) (liveTarget, *v1APIError, int) {
	if cache == nil {
		return liveTarget{}, &v1APIError{Code: v1CodeUpstreamUnavailable, Message: "symbol directory unavailable; pass instrumentId or exchange"}, http.StatusServiceUnavailable
	}
	items, err := cache.Search(code, maxSearchLimit)
	if err != nil {
		return liveTarget{}, &v1APIError{Code: v1CodeUpstreamUnavailable, Message: err.Error()}, http.StatusServiceUnavailable
	}
	var found []liveTarget
	for _, item := range items {
		if item.Symbol != code {
			continue
		}
		kind := v1ProviderRefKind(item.Params)
		market, ok := liveParamMarket(item.Params["market"])
		if !ok || (kind != domain.KindStock && kind != domain.KindIndex) {
			continue
		}
		found = append(found, liveTarget{Kind: kind, Market: market, Code: code})
	}
	switch len(found) {
	case 1:
		return found[0], nil, 0
	case 0:
		return liveTarget{}, &v1APIError{Code: v1CodeInstrumentNotFound,
			Message: fmt.Sprintf("no A-share/index instrument with symbol %q", code)}, http.StatusNotFound
	default:
		candidates := make([]string, 0, len(found))
		for _, t := range found {
			candidates = append(candidates, t.instrumentID())
		}
		return liveTarget{}, &v1APIError{Code: v1CodeInvalidRequest,
			Message: fmt.Sprintf("symbol %q is ambiguous; pass instrumentId or exchange", code),
			Details: map[string]any{"candidates": candidates}}, http.StatusBadRequest
	}
}

// liveParamMarket 读取目录条目的 market：内存目录为 uint8，SQLite/JSON 回读可能为其他数值类型。
func liveParamMarket(v any) (uint8, bool) {
	var n int64
	switch x := v.(type) {
	case uint8:
		n = int64(x)
	case int:
		n = int64(x)
	case int64:
		n = x
	case float64:
		n = int64(x)
	default:
		return 0, false
	}
	if n < 0 || n > 2 {
		return 0, false
	}
	return uint8(n), true
}

// gotdxLiveFetch 构造访问 gotdx 的尾部拉取函数；映射复用 bars 接口，保证时间戳与数值口径一致。
func gotdxLiveFetch(target liveTarget, category, adjust uint16) live.FetchFunc {
	return func(context.Context) ([]live.Bar, error) {
		var (
			bars []proto.SecurityBar
			err  error
		)
		if target.Kind == domain.KindIndex {
			bars, err = domain.IndexKLineBefore(category, target.Market, target.Code, liveFetchSize, nil)
		} else {
			bars, err = domain.StockKLineBefore(category, target.Market, target.Code, 1, adjust, liveFetchSize, nil)
		}
		if errors.Is(err, domain.ErrNoKLineData) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		out := make([]live.Bar, 0, len(bars))
		for _, bar := range bars {
			out = append(out, securityBarToLiveBar(bar))
		}
		return out, nil
	}
}

// securityBarToLiveBar 经 bars 接口同一映射转换为实时载荷，并记录交易所本地交易日。
func securityBarToLiveBar(bar proto.SecurityBar) live.Bar {
	item := securityBarToV1KLine(bar)
	return live.Bar{
		Timestamp:  item.Timestamp,
		Open:       item.Open,
		High:       item.High,
		Low:        item.Low,
		Close:      item.Close,
		Volume:     item.Volume,
		Turnover:   item.Turnover,
		TradingDay: bar.DateTime.Year()*10000 + int(bar.DateTime.Month())*100 + bar.DateTime.Day(),
	}
}

// cnShanghai A 股交易所时区；系统缺少 tzdata 时退化为固定 UTC+8（中国无夏令时）。
var cnShanghai = func() *time.Location {
	if loc, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return loc
	}
	return time.FixedZone("CST", 8*60*60)
}()

// cnLiveSession A 股轮询会话：工作日 09:15–11:32、12:59–15:02（上海时间，两端各留余量以取到集合竞价与收盘终值）。
// 09:35 后最近一根仍不是今天，视为节假日或停牌并按休市降频。
func cnLiveSession(now time.Time, tail []live.Bar) (bool, string) {
	t := now.In(cnShanghai)
	if wd := t.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return false, "weekend"
	}
	minute := t.Hour()*60 + t.Minute()
	inMorning := minute >= 9*60+15 && minute < 11*60+32
	inAfternoon := minute >= 12*60+59 && minute < 15*60+2
	if !inMorning && !inAfternoon {
		return false, "outside trading hours"
	}
	today := t.Year()*10000 + int(t.Month())*100 + t.Day()
	if minute >= 9*60+35 && len(tail) > 0 && tail[len(tail)-1].TradingDay < today {
		return false, "no bars today (holiday or suspension)"
	}
	return true, ""
}

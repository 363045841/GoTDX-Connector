// V1 实时 K 线 SSE 接口：GET /sources/:sourceId/stream，按 KCQ BarsLiveSource 帧协议推送
// snapshot / forming / closed / status 帧；上游轮询由 live.Hub 按序列共享。
package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/instrument"
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

// liveSeries 实时订阅的上游查询身份；路由由统一解析器给出。
type liveSeries struct {
	Routing instrument.Routing
	Code    string
}

// ID 返回与搜索接口一致的规范品种 ID。
func (s liveSeries) ID() string {
	return s.Routing.ID(v1SourceID, s.Code)
}

// liveFetchBuilder 为目标品种构造上游拉取函数；测试可替换以避免访问 gotdx。
type liveFetchBuilder func(target liveSeries, category, adjust uint16) live.FetchFunc

// liveStreamDeps 实时流处理器依赖。
type liveStreamDeps struct {
	hub       *live.Hub
	cache     *directory.Cache
	fetch     liveFetchBuilder
	session   live.SessionFunc
	heartbeat time.Duration
}

// ── 批量实时订阅 ──

// liveStreamTarget 已解析的品种与其批量订阅；一条 SSE 连接承载多个品种。
type liveStreamTarget struct {
	instrumentID string
	sub          *live.Subscription
}

// liveStreamFrame 合并后的事件：帧身份由订阅目标补全。
type liveStreamFrame struct {
	instrumentID string
	event        live.Event
}

// parseLiveInstruments 解析 instruments 参数；重复 ID 只保留首个，前缀不匹配直接拒绝。
func parseLiveInstruments(raw string) ([]string, *v1APIError) {
	ids := make([]string, 0, 4)
	seen := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if _, _, err := instrument.ParseID(id, v1SourceID); err != nil {
			return nil, &v1APIError{Code: v1CodeInvalidRequest, Message: "unknown instrumentId: " + id}
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, &v1APIError{Code: v1CodeInvalidRequest, Message: "instruments is required"}
	}
	return ids, nil
}

// subscribeLiveTargets 按同一序列身份批量订阅；部分失败时回滚已建立的订阅。
func subscribeLiveTargets(deps liveStreamDeps, ids []string, period string, category uint16, adjustment string) ([]liveStreamTarget, *v1APIError, int) {
	targets := make([]liveStreamTarget, 0, len(ids))
	for _, instrumentID := range ids {
		target, apiErr, status := resolveLiveTarget(deps.cache, "", instrumentID, "")
		if apiErr != nil {
			closeLiveTargets(targets)
			return nil, apiErr, status
		}
		if !v1AdjustmentSupported(string(target.Routing.Kind), adjustment) {
			closeLiveTargets(targets)
			return nil, &v1APIError{
				Code:    v1CodeUnsupportedCapability,
				Message: fmt.Sprintf("adjustment %q is not supported for kind %q", adjustment, target.Routing.Kind),
			}, http.StatusBadRequest
		}
		// 实时尾部的 qfq 与 none 数值一致（前复权以最新价为基准），默认 none；hfq 需显式指定。
		adjust := v1AdjustToUint[adjustment]
		if adjustment == "qfq" {
			adjust = v1AdjustToUint["none"]
		}
		key := fmt.Sprintf("%s|%s|%d", target.ID(), period, adjust)
		sub, err := deps.hub.Subscribe(key, live.Source{
			Fetch:   deps.fetch(target, category, adjust),
			Session: deps.session,
		})
		if err != nil {
			closeLiveTargets(targets)
			return nil, &v1APIError{Code: v1CodeUpstreamUnavailable, Message: err.Error()}, http.StatusServiceUnavailable
		}
		targets = append(targets, liveStreamTarget{instrumentID: target.ID(), sub: sub})
	}
	return targets, nil, 0
}

// closeLiveTargets 释放批量订阅。
func closeLiveTargets(targets []liveStreamTarget) {
	for _, target := range targets {
		target.sub.Close()
	}
}

// pumpLiveStream 把单个品种的事件转发到连接级通道；上游丢失时让连接结束由客户端重连。
func pumpLiveStream(ctx context.Context, target liveStreamTarget, frames chan<- liveStreamFrame, lost chan<- struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-target.sub.Lost():
			select {
			case lost <- struct{}{}:
			default:
			}
			return
		case ev, ok := <-target.sub.Events():
			if !ok {
				return
			}
			select {
			case frames <- liveStreamFrame{instrumentID: target.instrumentID, event: ev}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// writeStreamError 输出建流前的 V1 错误 envelope（非 200，EventSource 不会重连）。
func writeStreamError(c *gin.Context, status int, apiErr *v1APIError) {
	c.JSON(status, v1ErrorEnvelope{Error: *apiErr, RequestID: uuid.NewString()})
}

// handleV1Stream 创建生产用实时 K 线 SSE 处理器。
func handleV1Stream(hub *live.Hub, cache *directory.Cache, fetch domain.Fetcher) gin.HandlerFunc {
	return handleV1StreamWithDeps(liveStreamDeps{
		hub: hub, cache: cache, fetch: gotdxLiveFetch(fetch), session: cnLiveSession,
		heartbeat: liveHeartbeatInterval,
	})
}

// handleV1StreamWithDeps 可注入依赖的实时流处理器。
// 一个连接承载一组品种；周期与复权是连接参数，帧只带 instrumentId。
func handleV1StreamWithDeps(deps liveStreamDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Param("sourceId") != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, fmt.Sprintf("unknown sourceId %q", c.Param("sourceId")))
			return
		}
		period := c.Query("period")
		category, ok := v1PeriodToCategory[period]
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability, fmt.Sprintf("period %q is not supported", period))
			return
		}
		ids, apiErr := parseLiveInstruments(c.Query("instruments"))
		if apiErr != nil {
			writeStreamError(c, http.StatusBadRequest, apiErr)
			return
		}
		targets, apiErr, status := subscribeLiveTargets(deps, ids, period, category, c.DefaultQuery("adjustment", "none"))
		if apiErr != nil {
			writeStreamError(c, status, apiErr)
			return
		}
		defer closeLiveTargets(targets)

		w := c.Writer
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		connected := make([]string, len(targets))
		for i, target := range targets {
			connected[i] = target.instrumentID
		}
		// 首包立即下发，触发 EventSource onopen 并冲开代理缓冲。
		if _, err := fmt.Fprintf(w, "retry: %d\n: connected %s\n\n", liveRetryMillis, strings.Join(connected, ",")); err != nil {
			return
		}
		w.Flush()

		ctx := c.Request.Context()
		frames := make(chan liveStreamFrame)
		lost := make(chan struct{}, 1)
		for _, target := range targets {
			go pumpLiveStream(ctx, target, frames, lost)
		}
		ticker := time.NewTicker(deps.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-lost:
				// 慢消费者或上游丢失：断开让客户端重连并重新取快照
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
					return
				}
				w.Flush()
			case frame := <-frames:
				if err := writeLiveFrame(w, frame.instrumentID, frame.event); err != nil {
					return
				}
				w.Flush()
			}
		}
	}
}

// writeLiveFrame 把事件编码为一条无名 SSE data 消息。
func writeLiveFrame(w io.Writer, instrumentID string, ev live.Event) error {
	payload, err := encodeLiveFrame(instrumentID, ev)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

// encodeLiveFrame 按 KCQ 实时帧协议编码 JSON；身份一律用 instrumentId。
func encodeLiveFrame(instrumentID string, ev live.Event) ([]byte, error) {
	switch ev.Type {
	case live.EventSnapshot:
		bars := ev.Bars
		if bars == nil {
			bars = []live.Bar{}
		}
		return json.Marshal(struct {
			Type         live.EventType `json:"type"`
			InstrumentID string         `json:"instrumentId"`
			Bars         []live.Bar     `json:"bars"`
		}{ev.Type, instrumentID, bars})
	case live.EventForming, live.EventClosed:
		return json.Marshal(struct {
			Type         live.EventType `json:"type"`
			InstrumentID string         `json:"instrumentId"`
			Bar          live.Bar       `json:"bar"`
		}{ev.Type, instrumentID, ev.Bar})
	case live.EventStatus:
		return json.Marshal(struct {
			Type         live.EventType `json:"type"`
			InstrumentID string         `json:"instrumentId"`
			Status       string         `json:"status"`
			Detail       string         `json:"detail,omitempty"`
		}{ev.Type, instrumentID, ev.Status, ev.Detail})
	default:
		return nil, fmt.Errorf("unknown live event type %q", ev.Type)
	}
}

// resolveLiveTarget 把请求解析为主市场品种；解析规则与 bars/timeshare 完全一致。
// 返回的 HTTP 状态仅在 apiErr 非空时有意义。
func resolveLiveTarget(cache *directory.Cache, symbol, instrumentID, exchange string) (liveSeries, *v1APIError, int) {
	resolved, apiErr, status := resolveInstrument(v1InstrumentReference{
		ID: instrumentID, Symbol: symbol, Exchange: exchange,
	}, cache)
	if apiErr != nil {
		return liveSeries{}, apiErr, status
	}
	if resolved.Routing.Kind == instrument.Ex {
		return liveSeries{}, &v1APIError{
			Code:    v1CodeUnsupportedCapability,
			Message: "live bars are not supported for kind " + string(resolved.Routing.Kind),
		}, http.StatusBadRequest
	}
	return liveSeries{Routing: resolved.Routing, Code: resolved.Symbol}, nil, 0
}

// gotdxLiveFetch 构造访问 gotdx 的尾部拉取函数；映射复用 bars 接口，保证时间戳与数值口径一致。
func gotdxLiveFetch(source domain.Fetcher) liveFetchBuilder {
	return func(target liveSeries, category, adjust uint16) live.FetchFunc {
		return func(context.Context) ([]live.Bar, error) {
			started := time.Now()
			var (
				bars []proto.SecurityBar
				err  error
			)
			if target.Routing.Kind == instrument.Index {
				bars, err = domain.IndexKLineBefore(source, category, target.Routing.Market, target.Code, liveFetchSize, nil)
			} else {
				bars, err = domain.StockKLineBefore(source, category, target.Routing.Market, target.Code, 1, adjust, liveFetchSize, nil)
			}
			if errors.Is(err, domain.ErrNoKLineData) {
				return nil, nil
			}
			if err != nil {
				log.Printf("live bars query failed: instrument=%s category=%d adjust=%d elapsed=%s error=%v",
					target.ID(), category, adjust, time.Since(started).Round(time.Millisecond), err)
				return nil, err
			}
			out := make([]live.Bar, 0, len(bars))
			for _, bar := range bars {
				out = append(out, securityBarToLiveBar(bar))
			}
			return out, nil
		}
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

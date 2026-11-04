// V1 报价 SSE 接口：GET /sources/:sourceId/quotes。
// 报价是日线实时流的投影——最新价与昨收，不新增上游查询路径，也不加载完整 K 线。
package v1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"KlineChartQuantGo/services/tdx-api/internal/live"
	"github.com/gin-gonic/gin"
)

// quotePeriod 报价订阅的序列周期；与图表日线流共用同一个 Hub 轮询器。
const quotePeriod = "daily"

// quoteBarLimit 只保留尾部窗口：两根用于计算，多留一根给收线后的修订。
const quoteBarLimit = 3

// quoteMarker 已推送的报价内容；值不变时不重复推送。
type quoteMarker struct {
	last      float64
	prevClose float64
}

// quoteProjection 按品种维护日线尾部，输出最新价与昨收。
type quoteProjection struct {
	bars    map[string][]live.Bar
	emitted map[string]quoteMarker
}

// newQuoteProjection 创建空投影；状态只属于当前连接。
func newQuoteProjection() *quoteProjection {
	return &quoteProjection{bars: map[string][]live.Bar{}, emitted: map[string]quoteMarker{}}
}

// push 送入一帧；产生新的报价才返回载荷。
func (p *quoteProjection) push(instrumentID string, ev live.Event) ([]byte, bool) {
	switch ev.Type {
	case live.EventSnapshot:
		p.bars[instrumentID] = tailBars(ev.Bars, quoteBarLimit)
	case live.EventForming, live.EventClosed:
		p.upsert(instrumentID, ev.Bar)
	default:
		return nil, false
	}
	return p.quote(instrumentID)
}

// upsert 按时间戳升序插入或覆盖，只保留尾部窗口。
func (p *quoteProjection) upsert(instrumentID string, bar live.Bar) {
	bars := p.bars[instrumentID]
	for i, existing := range bars {
		if existing.Timestamp == bar.Timestamp {
			bars[i] = bar
			return
		}
		if existing.Timestamp > bar.Timestamp {
			bars = append(bars, live.Bar{})
			copy(bars[i+1:], bars[i:])
			bars[i] = bar
			p.bars[instrumentID] = bars
			return
		}
	}
	p.bars[instrumentID] = tailBars(append(bars, bar), quoteBarLimit)
}

// quote 倒数两根即最新价与昨收；不足两根时没有可比基准。
func (p *quoteProjection) quote(instrumentID string) ([]byte, bool) {
	bars := p.bars[instrumentID]
	if len(bars) < 2 {
		return nil, false
	}
	last, previous := bars[len(bars)-1], bars[len(bars)-2]
	marker := quoteMarker{last: last.Close, prevClose: previous.Close}
	if p.emitted[instrumentID] == marker {
		return nil, false
	}
	p.emitted[instrumentID] = marker
	payload, err := json.Marshal(struct {
		Type         string  `json:"type"`
		InstrumentID string  `json:"instrumentId"`
		Last         float64 `json:"last"`
		PrevClose    float64 `json:"prevClose"`
		Timestamp    int64   `json:"timestamp"`
	}{"quote", instrumentID, last.Close, previous.Close, last.Timestamp})
	if err != nil {
		return nil, false
	}
	return payload, true
}

// tailBars 返回切片末尾至多 n 个元素的副本。
func tailBars(bars []live.Bar, n int) []live.Bar {
	if len(bars) <= n {
		return append([]live.Bar(nil), bars...)
	}
	return append([]live.Bar(nil), bars[len(bars)-n:]...)
}

// handleV1Quotes 创建生产用报价 SSE 处理器。
func handleV1Quotes(hub *live.Hub, cache *directory.Cache, fetch domain.Fetcher) gin.HandlerFunc {
	return handleV1QuotesWithDeps(liveStreamDeps{
		hub: hub, cache: cache, fetch: gotdxLiveFetch(fetch), session: cnLiveSession,
		heartbeat: liveHeartbeatInterval,
	})
}

// handleV1QuotesWithDeps 可注入依赖的报价处理器；连接参数与 K 线流一致。
func handleV1QuotesWithDeps(deps liveStreamDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Param("sourceId") != v1SourceID {
			writeV1Error(c, http.StatusNotFound, v1CodeInvalidRequest, fmt.Sprintf("unknown sourceId %q", c.Param("sourceId")))
			return
		}
		ids, apiErr := parseLiveInstruments(c.Query("instruments"))
		if apiErr != nil {
			writeStreamError(c, http.StatusBadRequest, apiErr)
			return
		}
		category, ok := v1PeriodToCategory[quotePeriod]
		if !ok {
			writeV1Error(c, http.StatusBadRequest, v1CodeUnsupportedCapability, "daily period is unavailable")
			return
		}
		targets, apiErr, status := subscribeLiveTargets(deps, ids, quotePeriod, category, "none")
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
		projection := newQuoteProjection()
		ticker := time.NewTicker(deps.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-lost:
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
					return
				}
				w.Flush()
			case frame := <-frames:
				payload, ok := projection.push(frame.instrumentID, frame.event)
				if !ok {
					continue
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
					return
				}
				w.Flush()
			}
		}
	}
}

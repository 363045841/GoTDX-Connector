// K 线分页与按日期区间过滤的领域逻辑，A 股/指数/扩展行情共用。
package domain

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/bensema/gotdx/proto"
)

// klinePageSize 主站与扩展行情单次请求上限，保持在已验证的 800 条以内。
const klinePageSize uint16 = 798

// indexPageSize GetIndexBars 单次请求上限，保持在已验证的 800 条以内
const indexPageSize uint16 = 798

// indexFallbackPageSize 部分 start+798 会撞 gotdx invalid kline datetime，缩小 count 再试
const indexFallbackPageSize uint16 = 400

// maxKLinePages 分页页数硬上限：防御异常数据源（或注入点）持续返回新页导致内存无限增长。
// 远大于任何真实 K 线历史深度，只兜底终止条件失效的场景。
const maxKLinePages = 20000

// paginateFromRecent 从最近向历史翻页，按实际条数推进 start。
// toleratePartial：某页失败且已有数据时截断返回（指数深页 dateNum 损坏）。
func paginateFromRecent[T any](
	pageSize uint16,
	fetch func(start uint32, count uint16) ([]T, error),
	oldestTime func(item T) (time.Time, bool),
	startDate time.Time,
	toleratePartial bool,
) ([]T, error) {
	out := make([]T, 0, int(pageSize))
	for pageNum, start := 0, uint32(0); ; pageNum++ {
		if pageNum >= maxKLinePages {
			log.Printf("[gotdx] kline pagination aborted after %d pages (safety cap)", pageNum)
			break
		}
		page, err := fetch(start, pageSize)
		if err != nil {
			if toleratePartial && len(out) > 0 {
				log.Printf("[gotdx] kline page stop at start=%d after %d bars: %v", start, len(out), err)
				break
			}
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		out = append(out, page...)
		if t, ok := oldestTime(page[0]); ok && t.Before(startDate) {
			break
		}
		start += uint32(len(page))
	}
	return out, nil
}

// paginateBefore 从最近向历史分页，直到取得足够多的 cursor 之前数据。
func paginateBefore[T any](
	pageSize uint16,
	limit int,
	before *time.Time,
	fetch func(start uint32, count uint16) ([]T, error),
	timestamp func(item T) (time.Time, bool),
	toleratePartial bool,
) ([]T, error) {
	count := uint16(limit)
	if count > pageSize {
		count = pageSize
	}
	out := make([]T, 0, limit)
	for pageNum, start := 0, uint32(0); ; pageNum++ {
		if pageNum >= maxKLinePages {
			log.Printf("[gotdx] kline cursor pagination aborted after %d pages (safety cap)", pageNum)
			break
		}
		page, err := fetch(start, count)
		if err != nil {
			if toleratePartial && len(out) > 0 {
				log.Printf("[gotdx] kline cursor page stop at start=%d after %d bars: %v", start, len(out), err)
				break
			}
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		out = append(out, page...)
		if before == nil {
			break
		}
		older := 0
		for _, item := range out {
			if at, ok := timestamp(item); ok && at.Before(*before) {
				older++
			}
		}
		if older >= limit {
			break
		}
		start += uint32(len(page))
	}
	return out, nil
}

// clampUint16Start 主站协议 start 为 uint16；超出则视为无更多历史
func clampUint16Start(start uint32) (uint16, bool) {
	if start > 0xffff {
		return 0, false
	}
	return uint16(start), true
}

func securityBarOldest(k proto.SecurityBar) (time.Time, bool) {
	return k.DateTime, !k.DateTime.IsZero()
}

func exKLineOldest(k proto.ExKLineItem) (time.Time, bool) {
	return k.DateTime, !k.DateTime.IsZero()
}

// filterKLineByDate 按日期区间过滤 A 股/指数 K 线，可去重并升序排序。
func filterKLineByDate(klines []proto.SecurityBar, startDate, endDate time.Time, dedup bool) []proto.SecurityBar {
	out := make([]proto.SecurityBar, 0, len(klines))
	end := endDate.Add(24 * time.Hour)
	seen := make(map[string]bool)

	for _, k := range klines {
		if dedup {
			key := fmt.Sprintf("%d-%02d-%02dT%02d:%02d", k.Year, k.Month, k.Day, k.Hour, k.Minute)
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		if (k.DateTime.Equal(startDate) || k.DateTime.After(startDate)) && k.DateTime.Before(end) {
			out = append(out, k)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DateTime.Before(out[j].DateTime)
	})
	return out
}

// filterExKLineByDate 按日期区间过滤扩展 K 线，去重并升序
func filterExKLineByDate(klines []proto.ExKLineItem, startDate, endDate time.Time) []proto.ExKLineItem {
	out := make([]proto.ExKLineItem, 0, len(klines))
	seen := make(map[int64]bool)
	end := endDate.Add(24 * time.Hour)

	for _, k := range klines {
		if k.DateTime.IsZero() {
			continue
		}
		key := k.DateTime.UnixNano()
		if seen[key] {
			continue
		}
		seen[key] = true
		if (k.DateTime.Equal(startDate) || k.DateTime.After(startDate)) && k.DateTime.Before(end) {
			out = append(out, k)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DateTime.Before(out[j].DateTime)
	})
	return out
}

// limitSecurityBarsBefore 严格按 cursor 过滤股票或指数 K 线，并保留最新 limit 条升序数据。
func limitSecurityBarsBefore(klines []proto.SecurityBar, before *time.Time, limit int) []proto.SecurityBar {
	filtered := make([]proto.SecurityBar, 0, len(klines))
	seen := make(map[int64]bool)
	for _, k := range klines {
		if k.DateTime.IsZero() || (before != nil && !k.DateTime.Before(*before)) {
			continue
		}
		key := k.DateTime.UnixNano()
		if seen[key] {
			continue
		}
		seen[key] = true
		filtered = append(filtered, k)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DateTime.Before(filtered[j].DateTime) })
	if len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered
}

// limitExKLinesBefore 严格按 cursor 过滤扩展行情 K 线，并保留最新 limit 条升序数据。
func limitExKLinesBefore(klines []proto.ExKLineItem, before *time.Time, limit int) []proto.ExKLineItem {
	filtered := make([]proto.ExKLineItem, 0, len(klines))
	seen := make(map[int64]bool)
	for _, k := range klines {
		if k.DateTime.IsZero() || (before != nil && !k.DateTime.Before(*before)) {
			continue
		}
		key := k.DateTime.UnixNano()
		if seen[key] {
			continue
		}
		seen[key] = true
		filtered = append(filtered, k)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DateTime.Before(filtered[j].DateTime) })
	if len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered
}

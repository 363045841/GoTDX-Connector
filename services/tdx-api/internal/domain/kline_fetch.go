// A 股/指数/扩展行情 K 线分页与按日期区间查询；上游拉取通过 Fetcher 注入。
package domain

import (
	"errors"
	"time"

	"github.com/bensema/gotdx/proto"
)

// ErrNoKLineData 数据源对该品种完全无 K 线数据（如已到期期货），与"指定区间内无数据"区分。
// 上层据此返回可流转的确定性错误，触发前端请求流转。
var ErrNoKLineData = errors.New("no kline data for instrument")

// StockKLineRange 按 StockKLine 分页拉取 A 股 K 线并按日期过滤。
func StockKLineRange(f Fetcher, category uint16, market uint8, code string, times uint16, adjust uint16, startDate, endDate time.Time) ([]proto.SecurityBar, error) {
	raw, err := paginateFromRecent(klinePageSize, func(start uint32, count uint16) ([]proto.SecurityBar, error) {
		s, ok := clampUint16Start(start)
		if !ok {
			return nil, nil
		}
		return f.StockPage(category, market, code, s, count, times, adjust)
	}, securityBarOldest, startDate, false)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return filterKLineByDate(raw, startDate, endDate, true), nil
}

// StockKLineBefore 按 cursor 拉取股票 K 线，返回严格早于 before 的最新 limit 条升序数据。
func StockKLineBefore(f Fetcher, category uint16, market uint8, code string, times uint16, adjust uint16, limit int, before *time.Time) ([]proto.SecurityBar, error) {
	raw, err := paginateBefore(klinePageSize, limit, before, func(start uint32, count uint16) ([]proto.SecurityBar, error) {
		s, ok := clampUint16Start(start)
		if !ok {
			return nil, nil
		}
		return f.StockPage(category, market, code, s, count, times, adjust)
	}, securityBarOldest, false)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return limitSecurityBarsBefore(raw, before, limit), nil
}

// IndexKLineRange 按指数接口分页拉取 K 线并按日期过滤。
// 深页偶发 gotdx invalid kline datetime：先缩 count 再试，仍失败且已有数据则截断。
func IndexKLineRange(f Fetcher, category uint16, market uint8, code string, startDate, endDate time.Time) ([]proto.SecurityBar, error) {
	raw, err := paginateFromRecent(indexPageSize, func(start uint32, count uint16) ([]proto.SecurityBar, error) {
		s, ok := clampUint16Start(start)
		if !ok {
			return nil, nil
		}
		klines, err := f.IndexPage(category, market, code, s, count)
		if err != nil && count > indexFallbackPageSize {
			return f.IndexPage(category, market, code, s, indexFallbackPageSize)
		}
		return klines, err
	}, securityBarOldest, startDate, true)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return filterKLineByDate(raw, startDate, endDate, true), nil
}

// IndexKLineBefore 按 cursor 拉取指数 K 线，返回严格早于 before 的最新 limit 条升序数据。
func IndexKLineBefore(f Fetcher, category uint16, market uint8, code string, limit int, before *time.Time) ([]proto.SecurityBar, error) {
	raw, err := paginateBefore(indexPageSize, limit, before, func(start uint32, count uint16) ([]proto.SecurityBar, error) {
		s, ok := clampUint16Start(start)
		if !ok {
			return nil, nil
		}
		klines, err := f.IndexPage(category, market, code, s, count)
		if err != nil && count > indexFallbackPageSize {
			return f.IndexPage(category, market, code, s, indexFallbackPageSize)
		}
		return klines, err
	}, securityBarOldest, true)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return limitSecurityBarsBefore(raw, before, limit), nil
}

// ExKLineRange 按 ExKLine 分页拉取扩展行情并按日期过滤。
func ExKLineRange(f Fetcher, category uint8, code string, period uint16, times uint16, startDate, endDate time.Time) ([]proto.ExKLineItem, error) {
	raw, err := paginateFromRecent(klinePageSize, func(start uint32, count uint16) ([]proto.ExKLineItem, error) {
		return f.ExPage(category, code, period, start, count, times)
	}, exKLineOldest, startDate, false)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return filterExKLineByDate(raw, startDate, endDate), nil
}

// ExKLineBefore 按 cursor 拉取扩展行情 K 线，返回严格早于 before 的最新 limit 条升序数据。
func ExKLineBefore(f Fetcher, category uint8, code string, period uint16, times uint16, limit int, before *time.Time) ([]proto.ExKLineItem, error) {
	raw, err := paginateBefore(klinePageSize, limit, before, func(start uint32, count uint16) ([]proto.ExKLineItem, error) {
		return f.ExPage(category, code, period, start, count, times)
	}, exKLineOldest, false)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoKLineData
	}
	return limitExKLinesBefore(raw, before, limit), nil
}

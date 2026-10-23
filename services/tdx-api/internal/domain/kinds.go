// 品种 kind 常量与主市场判定辅助；kind 词汇的唯一来源是 instrument 包。
package domain

import (
	"strings"

	"KlineChartQuantGo/services/tdx-api/internal/instrument"
)

// 搜索结果 kind：与 instrument 包定义保持一致。
const (
	KindStock = string(instrument.Stock)
	KindIndex = string(instrument.Index)
	KindEx    = string(instrument.Ex)
)

// MainExchange 返回主市场行情代码对应的交易所缩写。
func MainExchange(market uint8) string {
	return instrument.Exchange(market)
}

// MainMarketSymbolKind 用代码所属市场判定品种是股票还是指数。
// 市场未知时按股票处理，由后续路由校验拒绝非法组合。
func MainMarketSymbolKind(code, exchange string) string {
	market, ok := instrument.MarketForExchange(exchange)
	if !ok {
		return KindStock
	}
	return string(instrument.KindForMainMarket(code, market))
}

// IsIndexKind 是否按指数接口拉线：优先 kind，否则按代码与市场判定。
func IsIndexKind(kind string, market uint8, code string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case KindIndex:
		return true
	case KindStock, KindEx:
		return false
	}
	return instrument.IsIndexCode(code, market)
}

// V1 品种解析：把 instrumentId、symbol、exchange 与 providerRef 归一为唯一路由。
// 路由只允许有一个事实来源，任何字段互相矛盾都直接拒绝，不做猜测或兜底。
package v1

import (
	"fmt"
	"net/http"
	"strings"

	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/instrument"
)

// resolvedInstrument 解析后的品种身份；ID 与 Exchange 均由路由规范生成。
type resolvedInstrument struct {
	ID       string
	Symbol   string
	Exchange string
	Routing  instrument.Routing
}

// resolveInstrument 归一品种引用。
// 路由优先级：providerRef → instrumentId → symbol+exchange → 目录精确匹配；
// 同时出现时必须一致。cache 为 nil 时不做目录匹配。
func resolveInstrument(ref v1InstrumentReference, cache *directory.Cache) (resolvedInstrument, *v1APIError, int) {
	symbol := strings.TrimSpace(ref.Symbol)
	exchange := strings.TrimSpace(ref.Exchange)
	rawID := strings.TrimSpace(ref.ID)
	if rawID == "" && strings.HasPrefix(symbol, v1SourceID+":") {
		rawID, symbol = symbol, ""
	}
	// 兼容行情代码自带交易所后缀的写法（如 000001.SH），后缀与 exchange 同义。
	if dot := strings.LastIndexByte(symbol, '.'); dot > 0 {
		if exchange == "" {
			exchange = symbol[dot+1:]
		}
		symbol = symbol[:dot]
	}

	var idRouting *instrument.Routing
	idSymbol := ""
	if rawID != "" {
		routing, parsedSymbol, err := instrument.ParseID(rawID, v1SourceID)
		if err != nil {
			return resolvedInstrument{}, invalidRequest(err), http.StatusBadRequest
		}
		idRouting, idSymbol = &routing, parsedSymbol
	}

	var refRouting *instrument.Routing
	if len(ref.ProviderRef) > 0 {
		routing, ok, err := instrument.ParseRef(ref.ProviderRef)
		if err != nil {
			return resolvedInstrument{}, invalidRequest(err), http.StatusBadRequest
		}
		if ok {
			refRouting = &routing
		}
	}

	if symbol == "" {
		symbol = idSymbol
	}
	if symbol == "" {
		return resolvedInstrument{}, invalidRequest(fmt.Errorf("instrument.symbol is required")), http.StatusBadRequest
	}
	if idSymbol != "" && idSymbol != symbol {
		return resolvedInstrument{}, invalidRequest(fmt.Errorf(
			"instrumentId %q carries symbol %q but request symbol is %q", rawID, idSymbol, symbol)), http.StatusBadRequest
	}

	var routing instrument.Routing
	switch {
	case refRouting != nil && idRouting != nil:
		if *refRouting != *idRouting {
			return resolvedInstrument{}, invalidRequest(fmt.Errorf(
				"instrumentId %q and providerRef %v disagree", rawID, ref.ProviderRef)), http.StatusBadRequest
		}
		routing = *refRouting
	case refRouting != nil:
		routing = *refRouting
	case idRouting != nil:
		routing = *idRouting
	default:
		if len(ref.ProviderRef) > 0 {
			return resolvedInstrument{}, invalidRequest(
				fmt.Errorf("providerRef.kind is required")), http.StatusBadRequest
		}
		if exchange != "" {
			market, ok := instrument.MarketForExchange(exchange)
			if !ok {
				return resolvedInstrument{}, &v1APIError{
					Code:    v1CodeUnsupportedCapability,
					Message: fmt.Sprintf("instrument %q on exchange %q is not supported", symbol, exchange),
				}, http.StatusBadRequest
			}
			routing = instrument.Routing{Kind: instrument.KindForMainMarket(symbol, market), Market: market}
			break
		}
		item, apiErr, status := lookupInstrument(cache, symbol)
		if apiErr != nil {
			return resolvedInstrument{}, apiErr, status
		}
		routing = item.Routing
		if routing.Kind == instrument.Ex {
			exchange = item.Exchange
		}
	}

	if err := routing.Validate(symbol); err != nil {
		return resolvedInstrument{}, invalidRequest(err), http.StatusBadRequest
	}
	if routing.Kind != instrument.Ex {
		if exchange != "" {
			market, ok := instrument.MarketForExchange(exchange)
			if !ok || market != routing.Market {
				return resolvedInstrument{}, invalidRequest(fmt.Errorf(
					"exchange %q does not match market %d", exchange, routing.Market)), http.StatusBadRequest
			}
		}
		exchange = instrument.Exchange(routing.Market)
	}

	return resolvedInstrument{
		ID:       routing.ID(v1SourceID, symbol),
		Symbol:   symbol,
		Exchange: exchange,
		Routing:  routing,
	}, nil, 0
}

// lookupInstrument 在证券目录中按代码精确匹配；同码多义时返回候选，拒绝猜测。
func lookupInstrument(cache *directory.Cache, symbol string) (directory.Item, *v1APIError, int) {
	if cache == nil {
		return directory.Item{}, &v1APIError{
			Code:    v1CodeUpstreamUnavailable,
			Message: "symbol directory unavailable; pass instrumentId or exchange",
		}, http.StatusServiceUnavailable
	}
	items, err := cache.Search(symbol, maxSearchLimit)
	if err != nil {
		return directory.Item{}, &v1APIError{Code: v1CodeUpstreamUnavailable, Message: err.Error()},
			http.StatusServiceUnavailable
	}
	found := make([]directory.Item, 0, 1)
	for _, item := range items {
		if item.Symbol == symbol {
			found = append(found, item)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil, 0
	case 0:
		return directory.Item{}, &v1APIError{
			Code:    v1CodeInstrumentNotFound,
			Message: fmt.Sprintf("no instrument with symbol %q", symbol),
		}, http.StatusNotFound
	default:
		candidates := make([]string, 0, len(found))
		for _, item := range found {
			candidates = append(candidates, item.Routing.ID(v1SourceID, item.Symbol))
		}
		return directory.Item{}, &v1APIError{
			Code:    v1CodeInvalidRequest,
			Message: fmt.Sprintf("symbol %q is ambiguous; pass instrumentId or exchange", symbol),
			Details: map[string]any{"candidates": candidates},
		}, http.StatusBadRequest
	}
}

// invalidRequest 构造 INVALID_REQUEST 错误。
func invalidRequest(err error) *v1APIError {
	return &v1APIError{Code: v1CodeInvalidRequest, Message: err.Error()}
}

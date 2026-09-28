// V1 协议映射层：负责周期/复权/时区/资产类别的转换，以及内部数据到 V1 DTO 的映射。
// 映射表与前端旧 gotdx.ts 的 PERIOD_TO_CATEGORY / ADJUST_MAP 保持一致。
package v1

import (
	"fmt"
	"math"
	"strings"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/directory"
	"KlineChartQuantGo/services/tdx-api/internal/domain"
	"github.com/bensema/gotdx/proto"
)

// v1SourceID V1 协议当前唯一数据源标识。
const v1SourceID = "gotdx"

// v1KLinePeriods 支持暴露的标准 K 线周期，与前端 KLinePeriod 对齐。
var v1KLinePeriods = []string{
	"1min", "5min", "15min", "30min", "60min",
	"daily", "weekly", "monthly", "quarterly", "yearly",
}

// v1PeriodToCategory 标准周期到 gotdx K 线 category 的映射。
var v1PeriodToCategory = map[string]uint16{
	"1min": 8, "5min": 0, "15min": 1, "30min": 2, "60min": 3,
	"daily": 4, "weekly": 5, "monthly": 6, "quarterly": 10, "yearly": 11,
}

// v1AdjustToUint 标准复权方式到 gotdx adjust 参数的映射。
var v1AdjustToUint = map[string]uint16{
	"none": 0, "qfq": 1, "hfq": 2, "splits": 0,
}

// v1BarCapability V1 品种的 K 线能力。
type v1BarCapability struct {
	Periods     []string `json:"periods"`
	Adjustments []string `json:"adjustments"`
}

// v1InstrumentCapabilities V1 品种可启用的行情能力。
type v1InstrumentCapabilities struct {
	Bars            *v1BarCapability            `json:"bars,omitempty"`
	TradingCalendar *bool                       `json:"tradingCalendar,omitempty"`
	TimeShare       *bool                       `json:"timeShare,omitempty"`
	TimeShareRange  *v1TimeShareRangeCapability `json:"timeShareRange,omitempty"`
	Depth           *bool                       `json:"depth,omitempty"`
}

// v1TimeShareRangeCapability 声明多日分时接口可一次查询的交易日上限。
type v1TimeShareRangeCapability struct {
	MaxTradingDays int `json:"maxTradingDays"`
}

// v1HistoryCoverage V1 数据源历史数据粗粒度覆盖区间，UTC Unix 毫秒。
type v1HistoryCoverage struct {
	From int64 `json:"from,omitempty"`
	To   int64 `json:"to,omitempty"`
}

// v1SourceCapabilities V1 源级能力声明，前端流转层据此筛选候选源。
type v1SourceCapabilities struct {
	TradingCalendar bool                        `json:"tradingCalendar"`
	AssetClasses    []string                    `json:"assetClasses"`
	Bars            *v1BarCapability            `json:"bars,omitempty"`
	TimeShare       *bool                       `json:"timeShare,omitempty"`
	TimeShareRange  *v1TimeShareRangeCapability `json:"timeShareRange,omitempty"`
	Depth           *bool                       `json:"depth,omitempty"`
	HistoryCoverage *v1HistoryCoverage          `json:"historyCoverage,omitempty"`
}

// v1SourceCapabilitiesFor 构建 gotdx 源级能力声明。
// 资产类别覆盖主市场 A 股/指数与扩展行情全部可路由类别。
// 历史覆盖只声明上界（数据可到当下）：分时/分钟线历史明显浅于日线，无单一诚实的下界可声明，
// 更早区间的真实可用性交由 INSTRUMENT_NOT_FOUND 确定性错误在流转时判定。
func v1SourceCapabilitiesFor() v1SourceCapabilities {
	timeShare, depth := true, false
	now := time.Now().UnixMilli()
	return v1SourceCapabilities{
		TradingCalendar: true,
		AssetClasses:    []string{"stock", "index", "fund", "future", "option", "forex"},
		Bars: &v1BarCapability{
			Periods:     append([]string(nil), v1KLinePeriods...),
			Adjustments: []string{"qfq", "hfq", "none"},
		},
		TimeShare:      &timeShare,
		TimeShareRange: &v1TimeShareRangeCapability{MaxTradingDays: v1MaxTimeShareRangeDays},
		Depth:          &depth,
		HistoryCoverage: &v1HistoryCoverage{
			To: now,
		},
	}
}

// v1InstrumentCapabilitiesFor 返回指定 kind 品种实际支持的能力。
// kind=stock 经 StockKLine 支持复权；index/ex 均无复权参数，仅支持 none。
func v1InstrumentCapabilitiesFor(kind string) v1InstrumentCapabilities {
	adjustments := []string{"none"}
	if kind == domain.KindStock {
		adjustments = []string{"qfq", "hfq", "none"}
	}
	timeShare := true
	return v1InstrumentCapabilities{
		Bars: &v1BarCapability{
			Periods:     append([]string(nil), v1KLinePeriods...),
			Adjustments: adjustments,
		},
		TimeShare:      &timeShare,
		TimeShareRange: &v1TimeShareRangeCapability{MaxTradingDays: v1MaxTimeShareRangeDays},
	}
}

// v1AdjustmentSupported 判断复权方式是否属于该 kind 的能力声明范围。
func v1AdjustmentSupported(kind, adjustment string) bool {
	for _, a := range v1InstrumentCapabilitiesFor(kind).Bars.Adjustments {
		if a == adjustment {
			return true
		}
	}
	return false
}

// v1InstrumentDescriptor V1 品种描述，与前端 V1InstrumentDescriptor 对齐。
type v1InstrumentDescriptor struct {
	ID           string                   `json:"id"`
	SourceID     string                   `json:"sourceId"`
	Symbol       string                   `json:"symbol"`
	Name         string                   `json:"name"`
	AssetClass   string                   `json:"assetClass"`
	Exchange     string                   `json:"exchange"`
	SessionID    string                   `json:"sessionId,omitempty"`
	Currency     string                   `json:"currency,omitempty"`
	ProviderRef  map[string]any           `json:"providerRef,omitempty"`
	Capabilities v1InstrumentCapabilities `json:"capabilities"`
}

// v1InstrumentReference V1 请求体中的品种引用，providerRef 由客户端原样带回。
type v1InstrumentReference struct {
	ID          string         `json:"id"`
	Symbol      string         `json:"symbol"`
	Exchange    string         `json:"exchange"`
	ProviderRef map[string]any `json:"providerRef,omitempty"`
}

// v1ExchangeToTimezone 由交易所缩写推断品种会话时区；未知市场默认 A 股时区。
func v1ExchangeToTimezone(exchange string) string {
	switch strings.ToUpper(strings.TrimSpace(exchange)) {
	case "HK":
		return "Asia/Hong_Kong"
	case "US":
		return "America/New_York"
	default:
		return "Asia/Shanghai"
	}
}

// v1ExchangeToSession 由交易所缩写映射前端 MarketSessionRegistry 的会话标识。
func v1ExchangeToSession(exchange string) string {
	switch strings.ToUpper(strings.TrimSpace(exchange)) {
	case "HK":
		return "HK"
	case "US":
		return "US"
	default:
		return "CN"
	}
}

// v1ExchangeToCurrency 由交易所缩写推断交易币种。
func v1ExchangeToCurrency(exchange string) string {
	switch strings.ToUpper(strings.TrimSpace(exchange)) {
	case "HK":
		return "HKD"
	case "US":
		return "USD"
	default:
		return "CNY"
	}
}

// v1AssetClass 由品种 kind 与交易所归一化前端资产类别；规则与旧前端 resolveGotdxAssetClass 一致。
func v1AssetClass(exchange, kind string) string {
	if kind == domain.KindIndex {
		return "index"
	}
	if kind == domain.KindStock {
		return "stock"
	}
	switch strings.ToUpper(strings.TrimSpace(exchange)) {
	case "CN", "HK", "US":
		return "stock"
	case "FUND", "MONEY", "MONEY_FUND":
		return "fund"
	case "FUTURES":
		return "future"
	case "FX":
		return "forex"
	case "OPTION":
		return "option"
	case "CRYPTO":
		return "crypto"
	default:
		return "unknown"
	}
}

// v1InstrumentID 生成稳定品种 ID：gotdx:{kind}:{market|category}:{symbol}。
func v1InstrumentID(kind string, key int, symbol string) string {
	return fmt.Sprintf("gotdx:%s:%d:%s", kind, key, symbol)
}

// v1ProviderRefKind 读取 providerRef 中的路由 kind。
func v1ProviderRefKind(ref map[string]any) string {
	if ref == nil {
		return ""
	}
	if kind, ok := ref["kind"].(string); ok {
		return kind
	}
	return ""
}

// v1ProviderRefNumber 读取 providerRef 中的数字路由键（market/category）。
func v1ProviderRefNumber(ref map[string]any, key string) (int, bool) {
	if ref == nil {
		return 0, false
	}
	switch v := ref[key].(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	default:
		return 0, false
	}
}

// toV1Instrument 将内部搜索项映射为 V1 品种描述。
func toV1Instrument(item directory.Item) v1InstrumentDescriptor {
	kind := ""
	key := 0
	if params := item.Params; params != nil {
		kind = v1ProviderRefKind(params)
		if c, ok := v1ProviderRefNumber(params, "category"); ok {
			key = c
		} else if m, ok := v1ProviderRefNumber(params, "market"); ok {
			key = m
		}
	}
	assetClass := v1AssetClass(item.Exchange, kind)
	caps := v1InstrumentCapabilitiesFor(kind)
	if _, ok := futuresCalendarExchange(item.Exchange, item.Symbol); ok {
		supported := true
		caps.TradingCalendar = &supported
	}
	return v1InstrumentDescriptor{
		ID:           v1InstrumentID(kind, key, item.Symbol),
		SourceID:     v1SourceID,
		Symbol:       item.Symbol,
		Name:         item.Description,
		AssetClass:   assetClass,
		Exchange:     item.Exchange,
		SessionID:    v1ExchangeToSession(item.Exchange),
		Currency:     v1ExchangeToCurrency(item.Exchange),
		ProviderRef:  item.Params,
		Capabilities: caps,
	}
}

// v1KLineItem V1 K 线条目。
type v1KLineItem struct {
	Timestamp     int64    `json:"timestamp"`
	Date          string   `json:"date,omitempty"`
	Open          float64  `json:"open"`
	High          float64  `json:"high"`
	Low           float64  `json:"low"`
	Close         float64  `json:"close"`
	Volume        *float64 `json:"volume,omitempty"`
	Turnover      *float64 `json:"turnover,omitempty"`
	Amplitude     *float64 `json:"amplitude,omitempty"`
	ChangePercent *float64 `json:"changePercent,omitempty"`
	ChangeAmount  *float64 `json:"changeAmount,omitempty"`
	TurnoverRate  *float64 `json:"turnoverRate,omitempty"`
}

// v1Amplitude 计算振幅百分比：(High-Low)/base*100，base 无效时返回 nil。
func v1Amplitude(base, high, low float64) *float64 {
	if base == 0 {
		return nil
	}
	amp := math.Round((high-low)/base*10000) / 100
	return &amp
}

// securityBarToV1KLine 将 A 股/指数 K 线映射为 V1 条目。
func securityBarToV1KLine(bar proto.SecurityBar) v1KLineItem {
	item := v1KLineItem{
		Timestamp: bar.DateTime.UnixMilli(),
		Open:      bar.Open,
		High:      bar.High,
		Low:       bar.Low,
		Close:     bar.Close,
	}
	if !bar.DateTime.IsZero() {
		item.Date = bar.DateTime.Format("2006-01-02")
	}
	volume, turnover, rate := bar.Vol, bar.Amount, bar.Turnover
	risePrice, riseRate := bar.RisePrice, bar.RiseRate
	item.Volume, item.Turnover, item.TurnoverRate = &volume, &turnover, &rate
	item.ChangeAmount, item.ChangePercent = &risePrice, &riseRate
	item.Amplitude = v1Amplitude(domain.SecurityBarPreClose(bar), bar.High, bar.Low)
	return item
}

// exKLineToV1KLine 将扩展行情 K 线映射为 V1 条目。
func exKLineToV1KLine(item proto.ExKLineItem) v1KLineItem {
	entry := v1KLineItem{
		Timestamp: item.DateTime.UnixMilli(),
		Open:      item.Open,
		High:      item.High,
		Low:       item.Low,
		Close:     item.Close,
	}
	if !item.DateTime.IsZero() {
		entry.Date = item.DateTime.Format("2006-01-02")
	}
	volume, turnover := float64(item.Vol), item.Amount
	risePrice, riseRate := item.RisePrice, item.RiseRate
	entry.Volume, entry.Turnover = &volume, &turnover
	entry.ChangeAmount, entry.ChangePercent = &risePrice, &riseRate
	entry.Amplitude = v1Amplitude(domain.ExKLinePreClose(item), item.High, item.Low)
	return entry
}

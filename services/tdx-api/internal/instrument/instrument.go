// 品种路由词汇的唯一来源：kind、market/category、品种 ID 与 providerRef 的编码规则。
// 内存目录、SQLite 快照与 V1 协议共用这一套类型，避免多份数值解析各自漂移。
package instrument

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/bensema/gotdx/types"
)

// Kind 品种路由分类。
type Kind string

// 协议允许的品种分类。
const (
	Stock Kind = "stock"
	Index Kind = "index"
	Ex    Kind = "ex"
)

// ParseKind 把外部字符串归一为 Kind。
func ParseKind(value string) (Kind, bool) {
	switch kind := Kind(strings.ToLower(strings.TrimSpace(value))); kind {
	case Stock, Index, Ex:
		return kind, true
	default:
		return "", false
	}
}

// Routing 品种路由：主市场使用 Market，扩展行情使用 Category。
type Routing struct {
	Kind     Kind
	Market   uint8
	Category uint8
}

// Key 返回品种 ID 中表示路由的数字段。
func (r Routing) Key() int {
	if r.Kind == Ex {
		return int(r.Category)
	}
	return int(r.Market)
}

// ID 生成规范品种 ID：{source}:{kind}:{market|category}:{symbol}。
func (r Routing) ID(source, symbol string) string {
	return fmt.Sprintf("%s:%s:%d:%s", source, r.Kind, r.Key(), symbol)
}

// ProviderRef 生成 V1 providerRef，字段与路由一一对应。
func (r Routing) ProviderRef() map[string]any {
	if r.Kind == Ex {
		return map[string]any{"kind": string(r.Kind), "category": r.Category}
	}
	return map[string]any{"kind": string(r.Kind), "market": r.Market}
}

// Validate 校验路由与代码自洽：主市场的 kind 必须与代码所属市场一致。
func (r Routing) Validate(code string) error {
	switch r.Kind {
	case Stock, Index:
		if r.Market > 2 {
			return fmt.Errorf("market %d is out of range", r.Market)
		}
		if r.Category != 0 {
			return fmt.Errorf("main-market routing must not carry category")
		}
		if len(code) != 6 {
			return fmt.Errorf("main-market code %q must be six characters", code)
		}
		if IsIndexCode(code, r.Market) != (r.Kind == Index) {
			return fmt.Errorf("%s is not a %s instrument on %s", code, r.Kind, Exchange(r.Market))
		}
	case Ex:
		if r.Market != 0 {
			return fmt.Errorf("extended-market routing must not carry market")
		}
	default:
		return fmt.Errorf("unknown kind %q", r.Kind)
	}
	return nil
}

// Exchange 返回主市场对应的交易所缩写。
func Exchange(market uint8) string {
	switch market {
	case 0:
		return "SZ"
	case 1:
		return "SH"
	case 2:
		return "BJ"
	default:
		return ""
	}
}

// MarketForExchange 把交易所缩写映射为主市场编号。
func MarketForExchange(exchange string) (uint8, bool) {
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

// IsIndexCode 判断代码在指定主市场是否属于指数。
func IsIndexCode(code string, market uint8) bool {
	exchange := Exchange(market)
	if exchange == "" {
		return false
	}
	return types.IsIndex(fmt.Sprintf("%s.%s", code, exchange))
}

// KindForMainMarket 按代码与市场判定主市场品种是股票还是指数。
func KindForMainMarket(code string, market uint8) Kind {
	if IsIndexCode(code, market) {
		return Index
	}
	return Stock
}

// ParseID 解析 {source}:{kind}:{key}:{symbol}；数字段按 kind 解释为 market 或 category。
func ParseID(id, source string) (Routing, string, error) {
	parts := strings.Split(id, ":")
	if len(parts) != 4 || parts[0] != source {
		return Routing{}, "", fmt.Errorf("invalid instrument id %q", id)
	}
	kind, ok := ParseKind(parts[1])
	if !ok {
		return Routing{}, "", fmt.Errorf("invalid instrument id %q: unknown kind", id)
	}
	key, err := strconv.Atoi(parts[2])
	if err != nil || key < 0 || key > 255 {
		return Routing{}, "", fmt.Errorf("invalid instrument id %q: bad routing key", id)
	}
	symbol := parts[3]
	if symbol == "" {
		return Routing{}, "", fmt.Errorf("invalid instrument id %q: empty symbol", id)
	}
	routing := Routing{Kind: kind}
	if kind == Ex {
		routing.Category = uint8(key)
	} else {
		routing.Market = uint8(key)
	}
	return routing, symbol, nil
}

// ParseRef 读取 providerRef 的路由；缺少 kind 时返回 ok=false。
func ParseRef(ref map[string]any) (Routing, bool, error) {
	if len(ref) == 0 {
		return Routing{}, false, nil
	}
	rawKind, present := ref["kind"]
	if !present {
		return Routing{}, false, nil
	}
	kindText, ok := rawKind.(string)
	if !ok {
		return Routing{}, false, fmt.Errorf("providerRef.kind must be a string")
	}
	kind, ok := ParseKind(kindText)
	if !ok {
		return Routing{}, false, fmt.Errorf("providerRef.kind %q is not supported", kindText)
	}
	if kind == Ex {
		value, ok := Number(ref["category"])
		if !ok {
			return Routing{}, false, fmt.Errorf("providerRef.category is required for kind %q", kind)
		}
		if _, conflict := ref["market"]; conflict {
			return Routing{}, false, fmt.Errorf("providerRef must not mix market with kind %q", kind)
		}
		return Routing{Kind: Ex, Category: value}, true, nil
	}
	value, ok := Number(ref["market"])
	if !ok {
		return Routing{}, false, fmt.Errorf("providerRef.market is required for kind %q", kind)
	}
	if _, conflict := ref["category"]; conflict {
		return Routing{}, false, fmt.Errorf("providerRef must not mix category with kind %q", kind)
	}
	return Routing{Kind: kind, Market: value}, true, nil
}

// Number 把内存或 JSON 解码得到的数值统一转成 uint8；不接受字符串。
func Number(value any) (uint8, bool) {
	var n int64
	switch v := value.(type) {
	case uint8:
		n = int64(v)
	case uint16:
		n = int64(v)
	case uint32:
		n = int64(v)
	case uint64:
		if v > 255 {
			return 0, false
		}
		n = int64(v)
	case int:
		n = int64(v)
	case int8:
		n = int64(v)
	case int16:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	case float32:
		if v < 0 || v > 255 || v != float32(math.Trunc(float64(v))) {
			return 0, false
		}
		n = int64(v)
	case float64:
		if v < 0 || v > 255 || v != math.Trunc(v) {
			return 0, false
		}
		n = int64(v)
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return 0, false
		}
		n = parsed
	default:
		return 0, false
	}
	if n < 0 || n > 255 {
		return 0, false
	}
	return uint8(n), true
}

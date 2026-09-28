// V1 交易日历接口：从连接器自有的 gotdx 交易日历数据库读取已确定的期货日线槽位。
package v1

import (
	"database/sql"
	_ "embed"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	_ "modernc.org/sqlite"
)

//go:embed futures-trade-calendar.db
var futuresCalendarDB []byte

const maxCalendarSlots = 4096

type v1TradingCalendarRequest struct {
	SourceID        string                `json:"sourceId"`
	Instrument      v1InstrumentReference `json:"instrument"`
	Period          string                `json:"period"`
	Adjustment      string                `json:"adjustment"`
	BarAggregation  string                `json:"barAggregation"`
	AnchorTimestamp int64                 `json:"anchorTimestamp"`
	Count           int                   `json:"count"`
}

type v1TradingCalendarResult struct {
	AnchorTimestamp  int64   `json:"anchorTimestamp"`
	FutureTimestamps []int64 `json:"futureTimestamps"`
}

// futuresCalendarVenue 是日历数据库覆盖的一个期货交易所及其商品代码前缀表。
type futuresCalendarVenue struct {
	venue string
	// codes 为空格分隔的品种代码前缀，首尾各留一个空格，
	// 使匹配时 " "+prefix+" " 只能命中完整前缀，避免 "I" 误配 "IC"。
	codes string
}

// futuresCalendarVenues 覆盖数据库 exchange 列出现的全部期货交易所：
//
//	SHFE  上海期货交易所：AL 铝、AO 氧化铝、AU 黄金、AG 白银、ZN 锌、CU 铜、NI 沪镍、
//	      PB 沪铅、SN 沪锡、RB 螺纹钢、WR 线材、SS 不锈钢、HC 热卷、FU 燃油、
//	      BU 沥青、RU 橡胶、SP 纸浆、BR 原油。
//	INE   上海国际能源交易中心：SC 原油、BC 国际航线船用燃料油、LU 液化天然气、
//	      EC 集运指数（欧线）、NR 纯苯。
//	DCE   大连商品交易所：I 铁矿石、J 焦炭、JM 焦煤、M 豆粕、Y 豆油、P 棕榈油、
//	      L 塑料、PP 聚丙烯、V PVC、EG 乙二醇、EB 苯乙烯、FB 纤维板、PG 液化石油气、
//	      A 豆一、B 豆二、C 玉米、CS 玉米淀粉、JD 鸡蛋、RR 粳米。
//	CZCE  郑州商品交易所：CF 棉花、CY 棉纱、SR 白糖、OI 菜油、RM 菜粕、RS 菜籽、
//	      AP 苹果、CJ 红枣、FG 玻璃、SA 纯碱、MA 甲醇、TA PTA、PF 短纤、PK 花生、
//	      PM 普麦、WH 强麦、JR 粳稻、UR 尿素、PR 瓶片、SF 硅铁、SM 锰硅、ZC 动力煤。
//	CFFEX 中国金融期货交易所：IF/IC/IH/IM 股指、TF/T/TL/TS 国债。
//
// 顺序固定：契约代码由交易所唯一决定，但显式按切片而非 map 迭代，
// 避免未来新增品种前缀冲突时匹配结果随 map 遍历顺序漂移。
var futuresCalendarVenues = []futuresCalendarVenue{
	{venue: "SHFE", codes: " AL AO AG AU BR BU CU FU HC NI PB RB RU SN SP SS WR ZN "},
	{venue: "INE", codes: " BC EC LU NR SC "},
	{venue: "DCE", codes: " A B C CS EB EG FB I J JD JM L LH M P PG PP RR V Y "},
	{venue: "CZCE", codes: " AP CF CJ CY FG JR MA OI PF PK PM PR RM RS SA SF SM SR TA UR WH ZC "},
	{venue: "CFFEX", codes: " IC IF IH IM T TF TL TS "},
}

// futuresCalendarExchange 仅识别数据库覆盖的中国期货交易所及对应合约前缀。
// 交易所以外的上市场所（如 "SH"、"HK"）或无法归类的代码一律返回 false，
// 调用方据此关闭该序列的交易日历能力，而非退化为工作日推测。
func futuresCalendarExchange(exchange, symbol string) (string, bool) {
	if strings.ToUpper(exchange) != "FUTURES" {
		return "", false
	}
	prefix := strings.TrimRightFunc(strings.ToUpper(strings.TrimSpace(symbol)), unicode.IsDigit)
	// 纯数字代码去掉后为空，与任何 codes 都不匹配，自然被排除。
	for _, entry := range futuresCalendarVenues {
		if strings.Contains(entry.codes, " "+prefix+" ") {
			return entry.venue, true
		}
	}
	return "", false
}

// handleV1TradingCalendar 验证周期与品种，并返回日历覆盖范围内的真实交易日。
func handleV1TradingCalendar(c *gin.Context) {
	var req v1TradingCalendarRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.SourceID != v1SourceID || req.Count < 1 || req.Count > maxCalendarSlots || req.AnchorTimestamp <= 0 || req.Instrument.Symbol == "" {
		writeV1Error(c, http.StatusBadRequest, v1CodeInvalidRequest, "invalid source, anchor, count or instrument")
		return
	}
	venue, ok := futuresCalendarExchange(req.Instrument.Exchange, req.Instrument.Symbol)
	if !ok || v1ProviderRefKind(req.Instrument.ProviderRef) != "ex" || req.Period != "daily" || req.Adjustment != "none" || req.BarAggregation != "original" {
		writeV1Error(c, http.StatusUnprocessableEntity, v1CodeUnsupportedCapability, "trading calendar unavailable for this series")
		return
	}
	result, err := futureTradingDates(venue, req.AnchorTimestamp, req.Count)
	if err != nil {
		writeV1Error(c, http.StatusBadGateway, v1CodeUpstreamUnavailable, err.Error())
		return
	}
	writeV1Data(c, http.StatusOK, v1TradingCalendarResult{AnchorTimestamp: req.AnchorTimestamp, FutureTimestamps: result})
}

// futureTradingDates 查询内置数据库；保持原 K 线在上海时区的时分秒，遇到日历末尾返回前缀。
func futureTradingDates(venue string, anchor int64, count int) ([]int64, error) {
	file, err := os.CreateTemp("", "gotdx-calendar-*.db")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(futuresCalendarDB); err != nil {
		file.Close()
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", file.Name())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	zone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, err
	}
	last := time.UnixMilli(anchor).In(zone)
	rows, err := db.Query(`SELECT cal_date FROM futures_trade_calendar WHERE exchange = ? AND cal_date > ? AND is_open = 1 ORDER BY cal_date LIMIT ?`, venue, last.Format("20060102"), count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]int64, 0, count)
	for rows.Next() {
		var day string
		if err := rows.Scan(&day); err != nil {
			return nil, err
		}
		date, err := time.ParseInLocation("20060102", day, zone)
		if err != nil {
			return nil, fmt.Errorf("invalid calendar date %q: %w", day, err)
		}
		stamp := time.Date(date.Year(), date.Month(), date.Day(), last.Hour(), last.Minute(), last.Second(), last.Nanosecond(), zone).UnixMilli()
		if stamp <= anchor {
			return nil, fmt.Errorf("invalid calendar ordering at %q", day)
		}
		result = append(result, stamp)
	}
	return result, rows.Err()
}

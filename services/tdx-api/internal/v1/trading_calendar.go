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

// futuresCalendarExchange 仅识别数据库覆盖的中国期货交易所及对应合约前缀。
func futuresCalendarExchange(exchange, symbol string) (string, bool) {
	if strings.ToUpper(exchange) != "FUTURES" {
		return "", false
	}
	prefix := strings.TrimRightFunc(strings.ToUpper(strings.TrimSpace(symbol)), unicode.IsDigit)
	for venue, codes := range map[string]string{
		"SHFE":  " AL AO AG AU BR BU CU FU HC NI PB RB RU SN SP SS WR ZN ",
		"INE":   " BC EC LU NR SC ",
		"DCE":   " A B C CS EB EG FB I J JD JM L LH M P PG PP RR V Y ",
		"CZCE":  " AP CF CJ CY FG JR MA OI PF PK PM PR RM RS SA SF SM SR TA UR WH ZC ",
		"CFFEX": " IC IF IH IM T TF TL TS ",
	} {
		if strings.Contains(codes, " "+prefix+" ") {
			return venue, true
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

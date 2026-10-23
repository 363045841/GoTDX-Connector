// 证券目录领域类型：搜索条目、加载器与持久化接口。
package directory

import (
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	"github.com/bensema/gotdx/proto"
)

// Item 证券目录条目；Routing 是唯一的路由事实，搜索响应与后续查询都由它派生。
type Item struct {
	Symbol      string             `json:"symbol"`
	Description string             `json:"description"`
	Exchange    string             `json:"exchange"`
	Source      string             `json:"source"`
	Routing     instrument.Routing `json:"routing"`
}

// Loader 证券目录数据源加载接口，隐藏 gotdx 主站/扩展行情访问细节。
type Loader interface {
	StockAll(market uint8) ([]proto.Security, error)
	ExCount() (uint32, error)
	ExList(start uint32, count uint16) ([]proto.ExListItem, error)
}

// Snapshot 证券目录持久化快照。
type Snapshot struct {
	Entries  []Item    `json:"entries"`
	LoadedAt time.Time `json:"loadedAt"`
}

// Store 证券目录持久化存储接口。
type Store interface {
	Load() (Snapshot, bool, error)
	Replace(snapshot Snapshot) error
}

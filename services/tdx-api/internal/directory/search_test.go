// 本文件测试证券目录缓存、持久化快照和搜索行为。
package directory

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	"github.com/bensema/gotdx/proto"
)

type fakeLoader struct {
	stocks     map[uint8][]proto.Security
	ex         []proto.ExListItem
	err        error
	stockCalls int
	exCalls    int
}

type fakeStore struct {
	snapshot     Snapshot
	found        bool
	loadErr      error
	replaceErr   error
	loadCalls    int
	replaceCalls int
}

// Load 返回预设的证券目录快照读取结果。
func (s *fakeStore) Load() (Snapshot, bool, error) {
	s.loadCalls++
	return s.snapshot, s.found, s.loadErr
}

// Replace 记录待写入快照并返回预设写入错误。
func (s *fakeStore) Replace(snapshot Snapshot) error {
	s.replaceCalls++
	if s.replaceErr != nil {
		return s.replaceErr
	}
	s.snapshot = snapshot
	s.found = true
	return nil
}

// StockAll 返回指定市场预设的股票目录数据。
func (l *fakeLoader) StockAll(market uint8) ([]proto.Security, error) {
	l.stockCalls++
	if l.err != nil {
		return nil, l.err
	}
	return l.stocks[market], nil
}

// ExCount 返回预设的扩展行情目录总数。
func (l *fakeLoader) ExCount() (uint32, error) {
	if l.err != nil {
		return 0, l.err
	}
	return uint32(len(l.ex)), nil
}

// ExList 返回预设的扩展行情目录分页数据。
func (l *fakeLoader) ExList(start uint32, count uint16) ([]proto.ExListItem, error) {
	l.exCalls++
	if l.err != nil {
		return nil, l.err
	}
	end := int(start) + int(count)
	if end > len(l.ex) {
		end = len(l.ex)
	}
	if int(start) >= len(l.ex) {
		return nil, nil
	}
	return l.ex[start:end], nil
}

// newTestCache 创建使用测试加载器和可控时间的证券目录缓存。
func newTestCache(loader *fakeLoader, now *time.Time) *Cache {
	cache := NewCache(loader, nil, 30*time.Minute)
	cache.now = func() time.Time { return *now }
	return cache
}

// 验证符号目录缓存优先使用新鲜的持久化快照。
func TestSymbolDirectoryCacheUsesFreshPersistedSnapshot(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	loader := &fakeLoader{err: errors.New("loader must not be called")}
	store := &fakeStore{snapshot: Snapshot{
		Entries: []Item{{
			Symbol: "000001", Description: "Ping An", Exchange: "SZ", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
		}},
		LoadedAt: now.Add(-time.Hour),
	}, found: true}
	cache := NewCache(loader, store, 24*time.Hour)
	cache.now = func() time.Time { return now }

	items, err := cache.Search("000001", 20)
	if err != nil || len(items) != 1 {
		t.Fatalf("search = %#v, err:%v", items, err)
	}
	if loader.stockCalls != 0 || store.loadCalls != 1 || store.replaceCalls != 0 {
		t.Fatalf("calls = loader:%d load:%d replace:%d", loader.stockCalls, store.loadCalls, store.replaceCalls)
	}
}

// 验证刷新失败时符号目录缓存回退过期持久化快照。
func TestSymbolDirectoryCacheUsesStalePersistedSnapshotWhenRefreshFails(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	loader := &fakeLoader{err: errors.New("TDX unavailable")}
	store := &fakeStore{snapshot: Snapshot{
		Entries: []Item{{
			Symbol: "000001", Description: "Ping An", Exchange: "SZ", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
		}},
		LoadedAt: now.Add(-25 * time.Hour),
	}, found: true}
	cache := NewCache(loader, store, 24*time.Hour)
	cache.now = func() time.Time { return now }

	items, err := cache.Search("000001", 20)
	if err != nil || len(items) != 1 {
		t.Fatalf("search = %#v, err:%v", items, err)
	}
	if loader.stockCalls != 1 || store.loadCalls != 1 || store.replaceCalls != 0 {
		t.Fatalf("calls = loader:%d load:%d replace:%d", loader.stockCalls, store.loadCalls, store.replaceCalls)
	}
}

// 验证符号目录刷新成功后写入持久化存储。
func TestSymbolDirectoryCachePersistsSuccessfulRefresh(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{
		0: {{Code: "000001", Name: "Ping An"}},
	}}
	store := &fakeStore{}
	cache := NewCache(loader, store, 24*time.Hour)
	cache.now = func() time.Time { return now }

	items, err := cache.Search("000001", 20)
	if err != nil || len(items) != 1 {
		t.Fatalf("search = %#v, err:%v", items, err)
	}
	if store.loadCalls != 1 || store.replaceCalls != 1 {
		t.Fatalf("store calls = load:%d replace:%d", store.loadCalls, store.replaceCalls)
	}
	if !store.found || !store.snapshot.LoadedAt.Equal(now) || !reflect.DeepEqual(store.snapshot.Entries, cache.entries) {
		t.Fatalf("persisted snapshot = %#v", store.snapshot)
	}
}

// 验证持久化读写失败不影响符号目录缓存可用性。
func TestSymbolDirectoryCacheIgnoresStoreReadAndWriteFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{
		0: {{Code: "000001", Name: "Ping An"}},
	}}
	store := &fakeStore{
		loadErr:    errors.New("read failed"),
		replaceErr: errors.New("write failed"),
	}
	cache := NewCache(loader, store, 24*time.Hour)
	cache.now = func() time.Time { return now }

	items, err := cache.Search("000001", 20)
	if err != nil || len(items) != 1 {
		t.Fatalf("search = %#v, err:%v", items, err)
	}
	if loader.stockCalls != 3 || store.loadCalls != 1 || store.replaceCalls != 1 {
		t.Fatalf("calls = loader:%d load:%d replace:%d", loader.stockCalls, store.loadCalls, store.replaceCalls)
	}
}

// 验证不同缓存实例复用同一SQLite目录快照。
func TestSymbolDirectoryCacheReusesSQLiteSnapshotAcrossInstances(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "symbols.db")
	firstStore, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	firstLoader := &fakeLoader{stocks: map[uint8][]proto.Security{
		0: {{Code: "000001", Name: "Ping An"}},
	}}
	firstCache := NewCache(firstLoader, firstStore, 24*time.Hour)
	firstCache.now = func() time.Time { return now }
	if _, err := firstCache.Search("000001", 20); err != nil {
		t.Fatalf("populate first cache: %v", err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	secondStore, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	secondLoader := &fakeLoader{err: errors.New("loader must not be called")}
	secondCache := NewCache(secondLoader, secondStore, 24*time.Hour)
	secondCache.now = func() time.Time { return now }

	items, err := secondCache.Search("000001", 20)
	if err != nil || len(items) != 1 {
		t.Fatalf("search second cache = %#v, err:%v", items, err)
	}
	if secondLoader.stockCalls != 0 {
		t.Fatalf("second loader stock calls = %d, want 0", secondLoader.stockCalls)
	}
}

// 验证符号搜索优先匹配代码并应用结果上限。
func TestSymbolSearchRanksCodeBeforeNameAndAppliesLimit(t *testing.T) {
	now := time.Unix(0, 0)
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{
		0: {
			{Code: "ABC", Name: "other"},
			{Code: "ABCD", Name: "other"},
			{Code: "XABC", Name: "other"},
			{Code: "001", Name: "ABC"},
			{Code: "002", Name: "ABCD name"},
			{Code: "003", Name: "name ABC tail"},
		},
	}}
	cache := newTestCache(loader, &now)

	items, err := cache.Search("aBc", 4)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	got := make([]string, len(items))
	for i, item := range items {
		got[i] = item.Symbol
	}
	want := []string{"ABC", "ABCD", "XABC", "001"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("symbols = %v, want %v", got, want)
	}
}

// 验证符号搜索返回 gotdx 元数据并去重。
func TestSymbolSearchReturnsGotdxMetadataAndDeduplicates(t *testing.T) {
	now := time.Unix(0, 0)
	loader := &fakeLoader{
		stocks: map[uint8][]proto.Security{0: {{Code: "000001", Name: "Ping An Bank"}}},
		ex: []proto.ExListItem{
			{Market: 2, Category: 7, Code: "HK0001", Name: "Tencent"},
			{Market: 2, Category: 7, Code: "HK0001", Name: "Tencent duplicate"},
		},
	}
	cache := newTestCache(loader, &now)

	mainItems, err := cache.Search("000001", 20)
	if err != nil {
		t.Fatalf("main search: %v", err)
	}
	if len(mainItems) != 1 {
		t.Fatalf("main items = %d, want 1", len(mainItems))
	}
	if got, want := mainItems[0], (Item{
		Symbol:      "000001",
		Description: "Ping An Bank",
		Exchange:    "SZ",
		Source:      "gotdx",
		Routing:     instrument.Routing{Kind: instrument.Stock, Market: 0},
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("main item = %#v, want %#v", got, want)
	}

	exItems, err := cache.Search("hk0001", 20)
	if err != nil {
		t.Fatalf("extended search: %v", err)
	}
	if len(exItems) != 1 {
		t.Fatalf("extended items = %d, want 1", len(exItems))
	}
	if got, want := exItems[0], (Item{
		Symbol:      "HK0001",
		Description: "Tencent",
		Exchange:    "HK",
		Source:      "gotdx",
		Routing:     instrument.Routing{Kind: instrument.Ex, Category: 7},
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("extended item = %#v, want %#v", got, want)
	}
}

// 验证同码股票与指数各自得到规范路由与 ID，指数不会被落到 market 0。
func TestSymbolDirectoryRoutesMainMarketByCodeAndMarket(t *testing.T) {
	now := time.Unix(0, 0)
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{
		0: {{Code: "000001", Name: "平安银行"}},
		1: {{Code: "000001", Name: "上证指数"}},
	}}
	cache := newTestCache(loader, &now)

	items, err := cache.Search("000001", 20)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	byID := make(map[string]Item, len(items))
	for _, item := range items {
		byID[item.Routing.ID(item.Source, item.Symbol)] = item
	}
	if got := byID["gotdx:index:1:000001"].Routing; got != (instrument.Routing{Kind: instrument.Index, Market: 1}) {
		t.Fatalf("index routing = %#v, want index on market 1", got)
	}
	if got := byID["gotdx:stock:0:000001"].Routing; got != (instrument.Routing{Kind: instrument.Stock, Market: 0}) {
		t.Fatalf("stock routing = %#v, want stock on market 0", got)
	}
}

// 验证未过期时符号目录缓存复用内存目录。
func TestSymbolDirectoryCacheReusesFreshDirectory(t *testing.T) {
	now := time.Unix(0, 0)
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{0: {{Code: "000001", Name: "Ping An"}}}}
	cache := newTestCache(loader, &now)

	for range 2 {
		if _, err := cache.Search("000001", 20); err != nil {
			t.Fatalf("search: %v", err)
		}
	}
	if loader.stockCalls != 3 || loader.exCalls != 0 {
		t.Fatalf("loader calls = stock:%d ex:%d, want stock:3 ex:0", loader.stockCalls, loader.exCalls)
	}
}

// 验证刷新失败后符号目录缓存回退旧内存目录，并在退避期内不重复请求上游。
func TestSymbolDirectoryCacheFallsBackToOldDirectoryAfterRefreshFailure(t *testing.T) {
	now := time.Unix(0, 0)
	loader := &fakeLoader{stocks: map[uint8][]proto.Security{0: {{Code: "000001", Name: "Ping An"}}}}
	cache := newTestCache(loader, &now)

	if _, err := cache.Search("000001", 20); err != nil {
		t.Fatalf("initial search: %v", err)
	}
	loader.err = errors.New("TDX unavailable")
	now = now.Add(31 * time.Minute)
	items, err := cache.Search("000001", 20)
	if err != nil {
		t.Fatalf("fallback search: %v", err)
	}
	if len(items) != 1 || items[0].Symbol != "000001" {
		t.Fatalf("fallback items = %#v", items)
	}
	failedRefreshCalls := loader.stockCalls
	if _, err := cache.Search("000001", 20); err != nil {
		t.Fatalf("second fallback search: %v", err)
	}
	if loader.stockCalls != failedRefreshCalls {
		t.Fatalf("loader retried before backoff elapsed: calls = %d, want %d", loader.stockCalls, failedRefreshCalls)
	}
}

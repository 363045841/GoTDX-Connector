// 证券目录缓存与搜索：从 gotdx 主站/扩展行情加载目录，缓存并持久化，按关键字搜索。
package directory

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/client"
	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	"github.com/bensema/gotdx/proto"
)

const (
	defaultTTL          = 24 * time.Hour
	exDirectoryPageSize = 1000
	directoryRetryDelay = time.Minute
)

// GotdxLoader 通过 gotdx 客户端加载证券目录。
type GotdxLoader struct {
	Manager *client.Manager
}

func (l GotdxLoader) StockAll(market uint8) ([]proto.Security, error) {
	return client.QueryMain(l.Manager, func(c client.MainQuerier) ([]proto.Security, error) {
		return c.StockAll(market)
	})
}

func (l GotdxLoader) ExCount() (uint32, error) {
	return client.QueryEx(l.Manager, func(c client.ExQuerier) (uint32, error) { return c.ExCount() })
}

func (l GotdxLoader) ExList(start uint32, count uint16) ([]proto.ExListItem, error) {
	return client.QueryEx(l.Manager, func(c client.ExQuerier) ([]proto.ExListItem, error) {
		return c.ExList(start, count)
	})
}

// Cache 证券目录缓存：TTL 内复用内存目录，过期刷新，持久化失败不影响可用性。
// 加载在锁外进行，并发调用等待同一次加载结果，避免全量拉取阻塞所有搜索。
type Cache struct {
	loader Loader
	store  Store
	ttl    time.Duration
	now    func() time.Time

	mu        sync.Mutex
	entries   []Item
	loadedAt  time.Time
	retryAt   time.Time
	lastError error
	loaded    bool
	storeRead bool
	loading   chan struct{}
}

// NewCache 创建证券目录缓存。
func NewCache(loader Loader, store Store, ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &Cache{loader: loader, store: store, ttl: ttl, now: time.Now}
}

// WarmUp 启动时预热目录缓存。
func (c *Cache) WarmUp() error {
	log.Printf("symbol directory: warming up")
	_, err := c.directory()
	return err
}

// Search 按关键字搜索证券目录，返回按匹配度排序的条目。
func (c *Cache) Search(query string, limit int) ([]Item, error) {
	entries, err := c.directory()
	if err != nil {
		return nil, err
	}
	needle := strings.ToLower(query)
	type ranked struct {
		item Item
		rank int
	}
	matches := make([]ranked, 0, len(entries))
	for _, entry := range entries {
		if rank := matchRank(entry, needle); rank < 6 {
			matches = append(matches, ranked{item: entry, rank: rank})
		}
	}
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].rank < matches[j].rank })
	if limit < len(matches) {
		matches = matches[:limit]
	}
	items := make([]Item, len(matches))
	for i, match := range matches {
		items[i] = match.item
	}
	return items, nil
}

// directory 返回当前目录；缓存过期时由第一个调用者加载，其余调用者等待同一次结果。
func (c *Cache) directory() ([]Item, error) {
	for {
		now := c.now()
		c.mu.Lock()
		c.loadPersistedDirectoryLocked()
		if c.loaded && (now.Sub(c.loadedAt) < c.ttl || now.Before(c.retryAt)) {
			entries := c.entries
			c.mu.Unlock()
			return entries, nil
		}
		if c.loading != nil {
			wait := c.loading
			c.mu.Unlock()
			<-wait
			continue
		}
		// 无缓存且处于失败退避期：直接返回上次错误，避免并发调用反复重试上游。
		if !c.loaded && c.lastError != nil && now.Before(c.retryAt) {
			err := c.lastError
			c.mu.Unlock()
			return nil, err
		}
		c.loading = make(chan struct{})
		c.mu.Unlock()

		entries, err := c.refresh()
		if err == nil {
			c.persist(entries, now)
		}

		c.mu.Lock()
		if err == nil {
			c.entries = entries
			c.loadedAt = now
			c.retryAt = time.Time{}
			c.lastError = nil
			c.loaded = true
		} else {
			c.retryAt = now.Add(directoryRetryDelay)
			c.lastError = err
		}
		close(c.loading)
		c.loading = nil
		loaded, entries, lastError := c.loaded, c.entries, c.lastError
		c.mu.Unlock()

		if loaded {
			return entries, nil
		}
		return nil, lastError
	}
}

// refresh 从上游加载完整目录并记录耗时。
func (c *Cache) refresh() ([]Item, error) {
	started := time.Now()
	entries, err := c.loadDirectory()
	if err != nil {
		return nil, err
	}
	log.Printf("symbol directory: loaded %d entries from gotdx in %s",
		len(entries), time.Since(started).Round(time.Millisecond))
	return entries, nil
}

// persist 写入快照；失败只记录日志，内存目录仍然可用。
func (c *Cache) persist(entries []Item, loadedAt time.Time) {
	if c.store == nil {
		return
	}
	if err := c.store.Replace(Snapshot{Entries: entries, LoadedAt: loadedAt}); err != nil {
		log.Printf("symbol directory database write failed: %v", err)
		return
	}
	log.Printf("symbol directory: persisted %d entries to sqlite", len(entries))
}

// loadPersistedDirectoryLocked 首次使用时读取持久化快照，调用方必须持有 c.mu。
func (c *Cache) loadPersistedDirectoryLocked() {
	if c.storeRead || c.store == nil {
		return
	}
	c.storeRead = true
	snapshot, found, err := c.store.Load()
	if err != nil {
		log.Printf("symbol directory database read failed: %v", err)
		return
	}
	if !found {
		log.Printf("symbol directory: no sqlite snapshot found")
		return
	}
	c.entries = snapshot.Entries
	c.loadedAt = snapshot.LoadedAt
	c.loaded = true
	log.Printf("symbol directory: loaded %d entries from sqlite (age=%s)",
		len(snapshot.Entries), c.now().Sub(snapshot.LoadedAt).Round(time.Second))
}

// loadDirectory 拉取主市场与扩展行情全部条目。
func (c *Cache) loadDirectory() ([]Item, error) {
	entries := make([]Item, 0)
	seen := make(map[string]struct{})
	for _, market := range []uint8{0, 1, 2} {
		stocks, err := c.loader.StockAll(market)
		if err != nil {
			return nil, err
		}
		exchange := instrument.Exchange(market)
		for _, stock := range stocks {
			item := Item{
				Symbol: stock.Code, Description: stock.Name, Exchange: exchange, Source: "gotdx",
				Routing: instrument.Routing{
					Kind:   instrument.KindForMainMarket(stock.Code, market),
					Market: market,
				},
			}
			appendUnique(&entries, seen, item)
		}
	}

	count, err := c.loader.ExCount()
	if err != nil {
		return nil, err
	}
	for start := uint32(0); start < count; start += exDirectoryPageSize {
		pageSize := uint32(exDirectoryPageSize)
		if remaining := count - start; remaining < pageSize {
			pageSize = remaining
		}
		items, err := c.loader.ExList(start, uint16(pageSize))
		if err != nil {
			return nil, err
		}
		for _, ex := range items {
			item := Item{
				Symbol: ex.Code, Description: ex.Name, Exchange: extendedExchange(ex.Market), Source: "gotdx",
				Routing: instrument.Routing{Kind: instrument.Ex, Category: ex.Category},
			}
			appendUnique(&entries, seen, item)
		}
	}
	return entries, nil
}

// appendUnique 以规范品种 ID 去重，避免同一条目被多次加载。
func appendUnique(entries *[]Item, seen map[string]struct{}, item Item) {
	key := item.Routing.ID(item.Source, item.Symbol)
	if _, exists := seen[key]; exists {
		return
	}
	seen[key] = struct{}{}
	*entries = append(*entries, item)
}

func matchRank(item Item, query string) int {
	code := strings.ToLower(item.Symbol)
	name := strings.ToLower(item.Description)
	switch {
	case code == query:
		return 0
	case strings.HasPrefix(code, query):
		return 1
	case strings.Contains(code, query):
		return 2
	case name == query:
		return 3
	case strings.HasPrefix(name, query):
		return 4
	case strings.Contains(name, query):
		return 5
	default:
		return 6
	}
}

func extendedExchange(market uint8) string {
	labels := map[uint8]string{1: "CN", 2: "HK", 3: "FUTURES", 4: "FX", 5: "INDEX", 6: "VALUATION", 7: "MONEY", 8: "FUND", 9: "MONEY_FUND", 10: "INDICATOR", 11: "MIRROR", 12: "OPTION", 13: "US", 14: "DE", 15: "SG"}
	if label, ok := labels[market]; ok {
		return label
	}
	return fmt.Sprintf("EX-%d", market)
}

// 本文件测试SQLite证券目录快照的读写、一致性和日志配置。
package directory

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/instrument"
)

// 验证SQLite符号目录存储持久化并替换快照。
func TestSQLiteSymbolDirectoryStorePersistsAndReplacesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "symbols.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("empty Load() = found:%v err:%v", found, err)
	}

	first := Snapshot{
		Entries: []Item{{
			Symbol: "000001", Description: "Ping An", Exchange: "SZ", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
		}},
		LoadedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	if err := store.Replace(first); err != nil {
		t.Fatalf("replace first snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	got, found, err := store.Load()
	if err != nil || !found || !reflect.DeepEqual(got, first) {
		t.Fatalf("Load() = %#v, found:%v err:%v, want %#v", got, found, err, first)
	}

	second := Snapshot{
		Entries: []Item{{
			Symbol: "00700", Description: "Tencent", Exchange: "HK", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Ex, Category: 7},
		}},
		LoadedAt: first.LoadedAt.Add(time.Hour),
	}
	if err := store.Replace(second); err != nil {
		t.Fatalf("replace second snapshot: %v", err)
	}
	got, found, err = store.Load()
	if err != nil || !found || !reflect.DeepEqual(got, second) {
		t.Fatalf("Load() after replacement = %#v, found:%v err:%v", got, found, err)
	}
}

// 验证旧版目录表在打开时被丢弃重建，不做数据迁移。
func TestSQLiteSymbolDirectoryStoreDropsLegacySchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "symbols.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE symbol_directory (
			source TEXT NOT NULL,
			param_kind TEXT NOT NULL,
			param_value INTEGER NOT NULL,
			symbol TEXT NOT NULL,
			description TEXT NOT NULL,
			exchange TEXT NOT NULL,
			kind TEXT NOT NULL DEFAULT 'stock'
		);
		CREATE TABLE symbol_directory_metadata (id INTEGER PRIMARY KEY, loaded_at_unix INTEGER NOT NULL);
		INSERT INTO symbol_directory VALUES ('gotdx', 'market', 0, '000001', 'Ping An', 'SZ', 'stock');
		INSERT INTO symbol_directory_metadata VALUES (1, 1700000000);`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store on legacy database: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("legacy snapshot survived rebuild: found=%v err=%v", found, err)
	}
}

// 验证SQLite快照替换失败时保留原有快照。
func TestSQLiteSymbolDirectoryStorePreservesSnapshotWhenReplacementFails(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "symbols.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	first := Snapshot{
		Entries: []Item{{
			Symbol: "000001", Description: "Ping An", Exchange: "SZ", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
		}},
		LoadedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	if err := store.Replace(first); err != nil {
		t.Fatalf("replace first snapshot: %v", err)
	}
	if _, err := store.db.Exec(`
		CREATE TRIGGER reject_failed_symbol
		BEFORE INSERT ON symbol_directory
		WHEN NEW.symbol = 'FAIL'
		BEGIN SELECT RAISE(ABORT, 'forced failure'); END
	`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}

	failed := Snapshot{
		Entries: []Item{{
			Symbol: "FAIL", Description: "Failure", Exchange: "SZ", Source: "gotdx",
			Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
		}},
		LoadedAt: first.LoadedAt.Add(time.Hour),
	}
	if err := store.Replace(failed); err == nil {
		t.Fatal("Replace() error = nil, want forced failure")
	}
	got, found, err := store.Load()
	if err != nil || !found || !reflect.DeepEqual(got, first) {
		t.Fatalf("Load() after rollback = %#v, found:%v err:%v, want %#v", got, found, err, first)
	}
}

// 验证SQLite符号目录存储串行化并发写入。
func TestSQLiteSymbolDirectoryStoreWaitsForConcurrentWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "symbols.db")
	firstStore, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() { _ = firstStore.Close() })
	secondStore, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })

	tx, err := firstStore.db.Begin()
	if err != nil {
		t.Fatalf("begin blocking transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO symbol_directory_metadata (id, loaded_at_unix) VALUES (1, 1)`); err != nil {
		t.Fatalf("acquire write lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- secondStore.Replace(Snapshot{
			Entries: []Item{{
				Symbol: "000001", Description: "Ping An", Exchange: "SZ", Source: "gotdx",
				Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
			}},
			LoadedAt: time.Unix(2, 0).UTC(),
		})
	}()

	select {
	case err := <-done:
		t.Fatalf("Replace() returned before lock release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("release write lock: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Replace() after lock release: %v", err)
	}
}

// 验证SQLite快照替换期间读取结果保持一致。
func TestSQLiteSymbolDirectoryStoreLoadsConsistentSnapshotDuringReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "symbols.db")
	reader, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	snapshots := []Snapshot{
		{
			Entries: []Item{{
				Symbol: "000001", Description: "Version A", Exchange: "SZ", Source: "gotdx",
				Routing: instrument.Routing{Kind: instrument.Stock, Market: 0},
			}},
			LoadedAt: time.Unix(1, 0).UTC(),
		},
		{
			Entries: []Item{{
				Symbol: "600519", Description: "Version B", Exchange: "SH", Source: "gotdx",
				Routing: instrument.Routing{Kind: instrument.Stock, Market: 1},
			}},
			LoadedAt: time.Unix(2, 0).UTC(),
		},
	}
	if err := writer.Replace(snapshots[0]); err != nil {
		t.Fatalf("write initial snapshot: %v", err)
	}

	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			if err := writer.Replace(snapshots[i%len(snapshots)]); err != nil {
				writerDone <- err
				return
			}
		}
	}()

	var readErr error
	for range 200 {
		snapshot, found, err := reader.Load()
		if err != nil {
			readErr = err
			break
		}
		if !found || len(snapshot.Entries) != 1 {
			readErr = fmt.Errorf("invalid snapshot: %#v, found:%v", snapshot, found)
			break
		}
		wantSymbol := map[int64]string{1: "000001", 2: "600519"}[snapshot.LoadedAt.Unix()]
		if snapshot.Entries[0].Symbol != wantSymbol {
			readErr = fmt.Errorf("mixed snapshot: loadedAt:%s entry:%#v", snapshot.LoadedAt, snapshot.Entries[0])
			break
		}
	}
	close(stop)
	if err := <-writerDone; err != nil {
		t.Fatalf("replace snapshots: %v", err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
}

// 验证SQLite符号目录存储启用WAL日志模式。
func TestSQLiteSymbolDirectoryStoreUsesWAL(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "symbols.db"))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var mode string
	if err := store.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal mode = %q, want wal", mode)
	}
}

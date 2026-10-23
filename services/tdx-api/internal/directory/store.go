// 证券目录 SQLite 持久化：快照读写、一致性替换与 WAL 日志模式。
package directory

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KlineChartQuantGo/services/tdx-api/internal/instrument"
	_ "modernc.org/sqlite"
)

// sqliteSchemaVersion 目录表结构版本；目录是可重建缓存，版本不符时直接丢弃旧表。
const sqliteSchemaVersion = 2

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS symbol_directory (
	source TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('stock', 'index', 'ex')),
	market INTEGER NOT NULL CHECK (market BETWEEN 0 AND 255),
	category INTEGER NOT NULL CHECK (category BETWEEN 0 AND 255),
	symbol TEXT NOT NULL,
	description TEXT NOT NULL,
	exchange TEXT NOT NULL,
	PRIMARY KEY (source, kind, market, category, symbol)
);
CREATE TABLE IF NOT EXISTS symbol_directory_metadata (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	loaded_at_unix INTEGER NOT NULL
);`

const sqliteBusyTimeout = 5 * time.Second

// SQLiteStore 基于 SQLite 的证券目录快照存储。
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore 打开或创建证券目录 SQLite 数据库。
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create symbol database directory: %w", err)
	}
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)", path, sqliteBusyTimeout.Milliseconds())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open symbol database: %w", err)
	}
	db.SetMaxOpenConns(1)

	version, err := schemaVersion(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if version != sqliteSchemaVersion {
		if _, err := db.Exec(`DROP TABLE IF EXISTS symbol_directory; DROP TABLE IF EXISTS symbol_directory_metadata;`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("reset symbol database: %w", err)
		}
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize symbol database: %w", err)
	}
	if version != sqliteSchemaVersion {
		if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, sqliteSchemaVersion)); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("set symbol database version: %w", err)
		}
	}
	return &SQLiteStore{db: db}, nil
}

// schemaVersion 读取 SQLite 的 user_version。
func schemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read symbol database version: %w", err)
	}
	return version, nil
}

// Load 读取证券目录快照；无快照时返回 found=false。
func (s *SQLiteStore) Load() (Snapshot, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("begin symbol directory read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var loadedAtUnix int64
	err = tx.QueryRow(`SELECT loaded_at_unix FROM symbol_directory_metadata WHERE id = 1`).Scan(&loadedAtUnix)
	if err == sql.ErrNoRows {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("read symbol directory metadata: %w", err)
	}

	rows, err := tx.Query(`
		SELECT source, kind, market, category, symbol, description, exchange
		FROM symbol_directory
		ORDER BY rowid`)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("read symbol directory: %w", err)
	}
	entries := make([]Item, 0)
	for rows.Next() {
		var item Item
		var kind string
		var market, category uint8
		if err := rows.Scan(&item.Source, &kind, &market, &category, &item.Symbol, &item.Description, &item.Exchange); err != nil {
			_ = rows.Close()
			return Snapshot{}, false, fmt.Errorf("scan symbol directory: %w", err)
		}
		item.Routing = instrument.Routing{Kind: instrument.Kind(kind), Market: market, Category: category}
		if err := item.Routing.Validate(item.Symbol); err != nil {
			_ = rows.Close()
			return Snapshot{}, false, fmt.Errorf("invalid stored symbol %s: %w", item.Symbol, err)
		}
		entries = append(entries, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return Snapshot{}, false, fmt.Errorf("iterate symbol directory: %w", err)
	}
	if err := rows.Close(); err != nil {
		return Snapshot{}, false, fmt.Errorf("close symbol directory rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, false, fmt.Errorf("commit symbol directory read: %w", err)
	}
	return Snapshot{
		Entries:  entries,
		LoadedAt: time.Unix(loadedAtUnix, 0).UTC(),
	}, true, nil
}

// Replace 以事务方式替换证券目录快照，失败时回滚保持原快照。
func (s *SQLiteStore) Replace(snapshot Snapshot) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin symbol directory replacement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM symbol_directory`); err != nil {
		return fmt.Errorf("clear symbol directory: %w", err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO symbol_directory
			(source, kind, market, category, symbol, description, exchange)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare symbol directory insert: %w", err)
	}
	defer stmt.Close()

	for _, item := range snapshot.Entries {
		if _, err := stmt.Exec(
			item.Source, string(item.Routing.Kind), item.Routing.Market, item.Routing.Category,
			item.Symbol, item.Description, item.Exchange,
		); err != nil {
			return fmt.Errorf("insert symbol %s: %w", item.Symbol, err)
		}
	}
	if _, err := tx.Exec(`
		INSERT INTO symbol_directory_metadata (id, loaded_at_unix)
		VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET loaded_at_unix = excluded.loaded_at_unix`, snapshot.LoadedAt.Unix()); err != nil {
		return fmt.Errorf("update symbol directory metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit symbol directory replacement: %w", err)
	}
	return nil
}

// Close 关闭底层数据库连接。
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

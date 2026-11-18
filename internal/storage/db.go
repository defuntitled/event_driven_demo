package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/defuntitled/event_driven_demo/migrations"
)

const Timeout = 10 * time.Second

func Open(ctx context.Context, path, role string) (*sql.DB, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite3", u.String()+"?_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = migrate(ctx, db, role); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func migrate(ctx context.Context, db *sql.DB, role string) error {
	paths := []string{"common/001.sql", "common/002.sql", role + "/001.sql"}
	if role == "demo-web" {
		paths = []string{"demo/001.sql", "demo/002.sql"}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at_ms INTEGER NOT NULL)"); err != nil {
		return err
	}
	for _, path := range paths {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE name=?", path).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			continue
		}
		b, err := migrations.Files.ReadFile(path)
		if err != nil {
			return fmt.Errorf("migration %s: %w", path, err)
		}
		if _, err = tx.ExecContext(ctx, string(b)); err != nil {
			return fmt.Errorf("migration %s: %w", path, err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(name,applied_at_ms) VALUES (?,?)", path, time.Now().UnixMilli()); err != nil {
			return err
		}
	}
	if role != "demo-web" {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO service_state(singleton,admission) VALUES (1,'CLOSED'); INSERT OR IGNORE INTO worker_control(worker,desired_state,updated_at_ms) VALUES ('consumer','STOPPED',0),('publisher','STOPPED',0)"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

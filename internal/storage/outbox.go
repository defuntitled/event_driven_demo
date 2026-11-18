package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func AppendEvent(ctx context.Context, tx *sql.Tx, e contracts.Event, logical string) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(event_id,run_id,order_id,logical_key,event_type,topic,record_key,envelope_json,status,created_at_ms) VALUES (?,?,?,?,?,?,?,?,'PENDING',?)`, e.EventID, e.RunID, e.OrderID, logical, e.Type, e.Producer+".events", e.OrderID, string(raw), time.Now().UnixMilli())
	return err
}

func CheckRun(ctx context.Context, tx *sql.Tx, run string, generation int64, admission bool) error {
	var current sql.NullString
	var gen int64
	var status string
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,generation,admission FROM service_state WHERE singleton=1").Scan(&current, &gen, &status); err != nil {
		return err
	}
	if current.String != run || gen != generation {
		return contracts.Fail(409, "STALE_RUN", "Демо уже переключено на другой прогон")
	}
	if admission && status != "OPEN" {
		return contracts.Fail(503, "PREPARING", "Подготовка нового прогона")
	}
	return nil
}

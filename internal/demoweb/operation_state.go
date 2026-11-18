package demoweb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (a *App) failOperation(ctx context.Context, id, message string) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE control_operations SET state='FAILED',error_text=?,updated_at_ms=? WHERE operation_id=?", message, time.Now().UnixMilli(), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE demo_state SET state='ERROR' WHERE active_operation_id=?", id); err != nil {
		return err
	}
	return tx.Commit()
}
func (a *App) RetryOperation(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_operation_id FROM demo_state WHERE singleton=1").Scan(&active); err != nil {
		return contracts.Fail(409, "STALE_RUN", "Операция уже не активна")
	}
	if active != id {
		return contracts.Fail(409, "STALE_RUN", "Операция уже не активна")
	}
	if _, err = tx.ExecContext(ctx, "UPDATE control_operations SET state='RUNNING',error_text=NULL,updated_at_ms=? WHERE operation_id=?", time.Now().UnixMilli(), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE demo_state SET state='PREPARING' WHERE singleton=1"); err != nil {
		return err
	}
	return tx.Commit()
}
func (a *App) progress(ctx context.Context, op Operation) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	raw, err := json.Marshal(op.Components)
	if err != nil {
		return err
	}
	_, err = a.DB.ExecContext(ctx, "UPDATE control_operations SET stage=?,progress_json=?,updated_at_ms=? WHERE operation_id=? AND state='RUNNING'", op.Stage, string(raw), time.Now().UnixMilli(), op.OperationID)
	return err
}
func (a *App) completeOperation(ctx context.Context, op Operation) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE control_operations SET state='SUCCEEDED',stage='DONE',error_text=NULL,updated_at_ms=? WHERE operation_id=?", time.Now().UnixMilli(), op.OperationID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE demo_state SET state='READY',active_run_id=?,active_operation_id=NULL WHERE active_operation_id=?", op.RunID, op.OperationID); err != nil {
		return err
	}
	return tx.Commit()
}

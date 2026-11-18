package demoweb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type ControlCommand struct {
	Kind         string
	Scenario     string
	RunID        string
	Generation   int64
	InitialSeats int
}
type Operation struct {
	OperationID  string            `json:"operation_id"`
	State        string            `json:"state"`
	Stage        string            `json:"stage"`
	RunID        string            `json:"run_id"`
	Generation   int64             `json:"generation"`
	Components   map[string]string `json:"components"`
	Error        *string           `json:"error"`
	Kind         string            `json:"kind"`
	Scenario     string            `json:"scenario"`
	InitialSeats int               `json:"initial_seats"`
	CreatedAtMS  int64             `json:"created_at_ms"`
}

func (a *App) Operation(ctx context.Context, id string) (Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var op Operation
	var progress string
	err := a.DB.QueryRowContext(ctx, `SELECT o.operation_id,o.state,o.stage,o.run_id,o.generation,o.kind,o.progress_json,o.error_text,r.scenario,r.initial_seats,r.created_at_ms FROM control_operations o JOIN demo_runs r ON r.run_id=o.run_id WHERE o.operation_id=?`, id).Scan(&op.OperationID, &op.State, &op.Stage, &op.RunID, &op.Generation, &op.Kind, &progress, &op.Error, &op.Scenario, &op.InitialSeats, &op.CreatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return op, contracts.Fail(404, "OPERATION_NOT_FOUND", "Операция не найдена")
	}
	if err != nil {
		return op, err
	}
	err = json.Unmarshal([]byte(progress), &op.Components)
	return op, err
}

func (a *App) BeginOperation(ctx context.Context, key string, c ControlCommand) (Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	if key == "" || len(key) > 128 {
		return Operation{}, contracts.Fail(400, "INVALID_REQUEST", "Требуется Idempotency-Key")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return Operation{}, err
	}
	hash := contracts.Hash(raw)
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback()
	var oldID, oldHash string
	err = tx.QueryRowContext(ctx, "SELECT operation_id,request_hash FROM control_operations WHERE request_key=?", key).Scan(&oldID, &oldHash)
	if err == nil {
		if oldHash != hash {
			return Operation{}, contracts.Fail(409, "IDEMPOTENCY_CONFLICT", "Ключ операции уже использован")
		}
		if err = tx.Commit(); err != nil {
			return Operation{}, err
		}
		return a.Operation(ctx, oldID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	var active sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT active_operation_id FROM demo_state WHERE singleton=1").Scan(&active)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	if active.Valid {
		return Operation{}, contracts.Fail(409, "CONTROL_BUSY", "Другая операция ещё не завершена")
	}
	op, err := allocateOperation(ctx, tx, c)
	if err != nil {
		return Operation{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO control_operations(operation_id,request_key,request_hash,kind,run_id,generation,stage,state,progress_json,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,'RUNNING','{}',?,?)`, op.OperationID, key, hash, c.Kind, op.RunID, op.Generation, op.Stage, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return Operation{}, err
	}
	_, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO demo_state(singleton,active_run_id,state) VALUES (1,?,'PREPARING')", op.RunID)
	if err != nil {
		return Operation{}, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE demo_state SET state='PREPARING',active_operation_id=? WHERE singleton=1", op.OperationID)
	if err != nil {
		return Operation{}, err
	}
	if err = tx.Commit(); err != nil {
		return Operation{}, err
	}
	return a.Operation(ctx, op.OperationID)
}

func allocateOperation(ctx context.Context, tx *sql.Tx, c ControlCommand) (Operation, error) {
	var op Operation
	var err error
	op.OperationID, err = contracts.NewID("op")
	if err != nil {
		return op, err
	}
	if c.Kind != "prepare" && c.Kind != "reset" {
		var state string
		err = tx.QueryRowContext(ctx, `SELECT r.run_id,r.generation,r.scenario,s.state FROM demo_state s JOIN demo_runs r ON r.run_id=s.active_run_id WHERE s.singleton=1`).Scan(&op.RunID, &op.Generation, &op.Scenario, &state)
		if err != nil {
			return op, err
		}
		expected := map[string]string{"resume_reservations": "reservation_paused", "restore_payments": "payment_crash", "resume_orders_outbox": "orders_outbox_paused"}[c.Kind]
		if expected == "" || op.Scenario != expected {
			return op, contracts.Fail(400, "INVALID_REQUEST", "Действие не соответствует сценарию")
		}
		if op.RunID != c.RunID || op.Generation != c.Generation {
			return op, contracts.Fail(409, "STALE_RUN", "Устаревший прогон")
		}
		if state != "READY" {
			return op, contracts.Fail(409, "CONTROL_BUSY", "Демо ещё не готово")
		}
		op.Stage = "RESUME"
		return op, nil
	}
	if !slices.Contains(control.Scenarios, c.Scenario) || (c.InitialSeats != 0 && c.InitialSeats != 1) {
		return op, contracts.Fail(400, "INVALID_REQUEST", "Неизвестный сценарий")
	}
	op.RunID, err = contracts.NewID("run")
	if err != nil {
		return op, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(generation),0)+1 FROM demo_runs").Scan(&op.Generation); err != nil {
		return op, err
	}
	op.Stage = "QUIESCE"
	_, err = tx.ExecContext(ctx, "INSERT INTO demo_runs(run_id,generation,scenario,initial_seats,created_at_ms) VALUES (?,?,?,?,?)", op.RunID, op.Generation, c.Scenario, c.InitialSeats, time.Now().UnixMilli())
	return op, err
}

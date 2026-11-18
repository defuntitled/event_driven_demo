package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

var ErrStopped = errors.New("worker no longer active")

type Handler func(context.Context, *sql.Tx, contracts.Event) (string, error)
type Result struct {
	AttemptID int64
	Outcome   string
	Crash     bool
}

func Process(ctx context.Context, db *sql.DB, role, run string, gen int64, instance string, r contracts.Record, handler Handler) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	e, validation := contracts.Decode(r.Value, r.Topic, r.Key)
	attemptRun := e.RunID
	if validation != nil {
		// Invalid envelopes cannot supply a trustworthy run; retain the worker's context.
		attemptRun = run
	}
	insert, err := db.ExecContext(ctx, `INSERT INTO processing_attempts(worker_instance_id,handler,run_id,order_id,event_id,topic,partition_id,offset_value,started_at_ms,outcome,raw_json) VALUES (?,?,?,?,?,?,?,?,?,'STARTED',?)`, instance, role, attemptRun, e.OrderID, e.EventID, r.Topic, r.Partition, r.Offset, time.Now().UnixMilli(), string(r.Value))
	if err != nil {
		return Result{}, err
	}
	id, err := insert.LastInsertId()
	if err != nil {
		return Result{}, err
	}
	result := Result{AttemptID: id}
	if validation != nil {
		err = contracts.Protocol("%s", validation)
	}
	if err == nil {
		result, err = retryProcess(ctx, db, role, run, gen, r, e, id, handler)
	}
	if err != nil {
		outcome := "DB_ERROR"
		if f, ok := errors.AsType[*contracts.Failure](err); ok && f.Code == "PROTOCOL_ERROR" {
			outcome = "PROTOCOL_ERROR"
		}
		recordCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer done()
		_, diagErr := db.ExecContext(recordCtx, "UPDATE processing_attempts SET outcome=?,error_text=?,finished_at_ms=? WHERE attempt_id=?", outcome, err.Error(), time.Now().UnixMilli(), id)
		return result, errors.Join(err, diagErr)
	}
	return result, nil
}

func retryProcess(ctx context.Context, db *sql.DB, role, run string, gen int64, r contracts.Record, e contracts.Event, id int64, handler Handler) (Result, error) {
	for retries := 0; ; retries++ {
		result, err := processTx(ctx, db, role, run, gen, r, e, id, handler)
		if !IsBusy(err) {
			return result, err
		}
		timer := time.NewTimer(100 * time.Millisecond * time.Duration(1<<min(retries, 2)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
		if _, err = db.ExecContext(ctx, "UPDATE processing_attempts SET database_retries=database_retries+1 WHERE attempt_id=?", id); err != nil {
			return result, err
		}
	}
}

func IsBusy(err error) bool {
	e, ok := errors.AsType[sqlite3.Error](err)
	return ok && (e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked)
}

func processTx(ctx context.Context, db *sql.DB, role, run string, gen int64, r contracts.Record, e contracts.Event, id int64, handler Handler) (Result, error) {
	result := Result{AttemptID: id}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if err = CheckRun(ctx, tx, run, gen, false); err != nil {
		return result, ErrStopped
	}
	var desired string
	if err = tx.QueryRowContext(ctx, "SELECT desired_state FROM worker_control WHERE worker='consumer' AND run_id=? AND generation=?", run, gen).Scan(&desired); err != nil {
		return result, err
	}
	if desired != "RUNNING" {
		return result, ErrStopped
	}
	switch {
	case e.RunID != run:
		result.Outcome = "IGNORED_RUN"
	case !contracts.Handles(role, e.Type):
		result.Outcome = "IGNORED_TYPE"
	default:
		result.Outcome, err = applyInbox(ctx, tx, role, r, e, handler)
	}
	if err != nil {
		return result, err
	}
	if role == "payments" && e.Type == contracts.OrderCreated && result.Outcome == "PROCESSED" {
		result.Crash, err = armCrash(ctx, tx, run, e.EventID, id)
		if err != nil {
			return result, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE processing_attempts SET outcome=?,finished_at_ms=? WHERE attempt_id=?", result.Outcome, time.Now().UnixMilli(), id); err != nil {
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func armCrash(ctx context.Context, tx *sql.Tx, run, event string, attempt int64) (bool, error) {
	r, err := tx.ExecContext(ctx, "UPDATE fault_injections SET remaining=0,triggered_event_id=?,triggered_at_ms=? WHERE run_id=? AND name='crash_after_payment_tx' AND remaining=1", event, time.Now().UnixMilli(), run)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE worker_control SET desired_state='FAULTED',updated_at_ms=? WHERE worker='consumer'", time.Now().UnixMilli()); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE processing_attempts SET fault_name='crash_after_payment_tx' WHERE attempt_id=?", attempt)
	return true, err
}

func applyInbox(ctx context.Context, tx *sql.Tx, role string, r contracts.Record, e contracts.Event, handler Handler) (string, error) {
	var hash, run, order string
	err := tx.QueryRowContext(ctx, "SELECT payload_hash,run_id,order_id FROM processed_events WHERE handler=? AND event_id=?", role, e.EventID).Scan(&hash, &run, &order)
	if err == nil {
		if hash != contracts.Hash(r.Value) || run != e.RunID || order != e.OrderID {
			return "", contracts.Protocol("conflicting event_id %s", e.EventID)
		}
		return "DUPLICATE_EVENT", nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	outcome, err := handler(ctx, tx, e)
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO processed_events(handler,event_id,run_id,order_id,payload_hash,first_topic,first_partition,first_offset,processed_at_ms) VALUES (?,?,?,?,?,?,?,?,?)`, role, e.EventID, e.RunID, e.OrderID, contracts.Hash(r.Value), r.Topic, r.Partition, r.Offset, time.Now().UnixMilli())
	return outcome, err
}

func ObserveCommit(ctx context.Context, db *sql.DB, id int64, commitErr error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	status := "ACKNOWLEDGED"
	var detail any
	if commitErr != nil {
		status = "ERROR"
		detail = fmt.Sprint(commitErr)
	}
	_, err := db.ExecContext(ctx, "UPDATE processing_attempts SET commit_status=?,error_text=? WHERE attempt_id=?", status, detail, id)
	return err
}

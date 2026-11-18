package control

import (
	"context"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (s *ServiceControl) initialize(ctx context.Context, r contracts.Run) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO runs(run_id,generation,scenario,initial_seats,created_at_ms) VALUES (?,?,?,?,?)", r.RunID, r.Generation, r.Scenario, r.InitialSeats, r.CreatedAtMS)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE outbox SET status='ABANDONED' WHERE status='PENDING' AND run_id<>?", r.RunID)
	if err != nil {
		return err
	}
	if s.Role == "reservations" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO inventory(run_id,concert_id,capacity,available) VALUES (?,'concert-1',?,?)", r.RunID, r.InitialSeats, r.InitialSeats); err != nil {
			return err
		}
	}
	if s.Role == "payments" && r.Scenario == "payment_crash" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO fault_injections(run_id,name,remaining) VALUES (?,'crash_after_payment_tx',1)", r.RunID); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE service_state SET active_run_id=?,control_phase=2 WHERE singleton=1 AND generation=? AND control_operation_id=?", r.RunID, r.Generation, r.OperationID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ServiceControl) startWorkers(ctx context.Context, r contracts.Run) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, worker := range []string{"consumer", "publisher"} {
		desired := "RUNNING"
		if r.Scenario == "reservation_paused" && s.Role == "reservations" && worker == "consumer" {
			desired = "PAUSED"
		}
		if r.Scenario == "orders_outbox_paused" && s.Role == "orders" && worker == "publisher" {
			desired = "PAUSED"
		}
		if _, err = tx.ExecContext(ctx, "UPDATE worker_control SET desired_state=?,run_id=?,generation=?,updated_at_ms=? WHERE worker=?", desired, r.RunID, r.Generation, time.Now().UnixMilli(), worker); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *ServiceControl) Resume(ctx context.Context, run string, gen int64, worker string) error {
	if worker != "consumer" && worker != "publisher" {
		return contracts.Fail(400, "INVALID_REQUEST", "Неизвестный работник")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	states, err := s.Workers.Snapshot(ctx)
	if err != nil {
		return err
	}
	if st := states[worker]; st.DesiredState == "FAULTED" && st.ActualState != "EXITED" && st.ActualState != "STOPPED" {
		return contracts.Fail(409, "CONTROL_BUSY", "Обработчик ещё завершает аварийный выход")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = storage.CheckRun(ctx, tx, run, gen, true); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE worker_control SET desired_state='RUNNING',last_error=NULL,updated_at_ms=? WHERE worker=? AND run_id=? AND generation=?", time.Now().UnixMilli(), worker, run, gen)
	if err != nil {
		return err
	}
	return tx.Commit()
}

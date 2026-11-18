package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
	"github.com/defuntitled/event_driven_demo/internal/workers"
)

var Scenarios = []string{"happy_path", "reservation_paused", "race_last_ticket", "payment_crash", "orders_outbox_paused"}

type WorkerObserver interface {
	Snapshot(context.Context) (map[string]workers.State, error)
}
type ServiceControl struct {
	DB      *sql.DB
	Role    string
	Workers WorkerObserver
	mu      sync.Mutex
}

func (s *ServiceControl) Apply(ctx context.Context, r contracts.Run) error {
	if !contracts.ValidID(r.RunID, "run") || !contracts.ValidID(r.OperationID, "op") || r.Generation < 1 || r.Phase < 1 || r.Phase > 4 || !slices.Contains(Scenarios, r.Scenario) || (r.InitialSeats != 0 && r.InitialSeats != 1) {
		return contracts.Fail(400, "INVALID_REQUEST", "Некорректная фаза подготовки")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	complete, err := s.admitPhase(ctx, r)
	if err != nil || complete {
		return err
	}
	switch r.Phase {
	case 1:
		err = s.waitWorkers(ctx, r, true)
	case 2:
		return s.initialize(ctx, r)
	case 3:
		if err = s.startWorkers(ctx, r); err == nil {
			err = s.waitWorkers(ctx, r, false)
		}
	case 4:
		err = s.waitWorkers(ctx, r, false)
	}
	if err != nil {
		return err
	}
	return s.finishPhase(ctx, r)
}

func (s *ServiceControl) admitPhase(ctx context.Context, r contracts.Run) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var gen int64
	var phase int
	if err = tx.QueryRowContext(ctx, "SELECT generation,control_phase FROM service_state WHERE singleton=1").Scan(&gen, &phase); err != nil {
		return false, err
	}
	normalized := r
	normalized.Phase = 0
	raw, err := json.Marshal(normalized)
	if err != nil {
		return false, err
	}
	hash := contracts.Hash(raw)
	if r.Generation < gen {
		return false, contracts.Fail(409, "STALE_RUN", "Устаревшее поколение управления")
	}
	if r.Generation > gen {
		if r.Phase != 1 {
			return false, contracts.Fail(409, "INVALID_PHASE", "Новое поколение начинается с QUIESCE")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO run_control_requests(generation,operation_id,request_hash) VALUES (?,?,?)", r.Generation, r.OperationID, hash); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE service_state SET generation=?,control_operation_id=?,control_phase=0,admission='CLOSED' WHERE singleton=1", r.Generation, r.OperationID); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE worker_control SET desired_state='STOPPED',run_id=?,generation=?,last_error=NULL,updated_at_ms=?", r.RunID, r.Generation, time.Now().UnixMilli()); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	var oldHash, op string
	err = tx.QueryRowContext(ctx, "SELECT request_hash,operation_id FROM run_control_requests WHERE generation=?", gen).Scan(&oldHash, &op)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (oldHash != hash || op != r.OperationID) {
		return false, contracts.Fail(409, "IDEMPOTENCY_CONFLICT", "Параметры поколения управления изменились")
	}
	if err != nil {
		return false, err
	}
	if r.Phase <= phase {
		return true, tx.Commit()
	}
	if r.Phase != phase+1 {
		return false, contracts.Fail(409, "INVALID_PHASE", "Предыдущая фаза ещё не завершена")
	}
	return false, tx.Commit()
}

func (s *ServiceControl) finishPhase(ctx context.Context, r contracts.Run) error {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	admission := "CLOSED"
	if r.Phase == 4 {
		admission = "OPEN"
	}
	_, err := s.DB.ExecContext(ctx, "UPDATE service_state SET control_phase=?,admission=? WHERE singleton=1 AND generation=? AND control_operation_id=? AND control_phase<?", r.Phase, admission, r.Generation, r.OperationID, r.Phase)
	return err
}

func (s *ServiceControl) waitWorkers(ctx context.Context, r contracts.Run, stopping bool) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		states, err := s.Workers.Snapshot(ctx)
		if err != nil {
			return err
		}
		ready := len(states) == 2
		for _, st := range states {
			if stopping {
				ready = ready && (st.ActualState == "STOPPED" || st.ActualState == "EXITED")
				continue
			}
			if st.DesiredState == "RUNNING" {
				ready = ready && st.ActualState == "RUNNING" && st.RunID == r.RunID && st.Generation == r.Generation
			} else {
				ready = ready && st.DesiredState == "PAUSED" && (st.ActualState == "STOPPED" || st.ActualState == "EXITED")
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

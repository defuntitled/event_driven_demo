package demoweb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (a *App) ControlLoop(ctx context.Context) {
	if err := a.reconcileStartup(ctx); err != nil && ctx.Err() == nil {
		slog.Error("startup reconciliation", "error", err)
	}
	for ctx.Err() == nil {
		s, err := a.Session(ctx)
		if err == nil && s.RunID != "" && s.ActiveOperationID == nil && s.State != "READY" {
			err = a.reconcileStartup(ctx)
		}
		if err == nil && s.RunID == "" {
			_, err = a.BeginOperation(ctx, "initial", ControlCommand{Kind: "prepare", Scenario: "happy_path", InitialSeats: 1})
		}
		if err == nil && s.ActiveOperationID != nil {
			op, readErr := a.Operation(ctx, *s.ActiveOperationID)
			err = readErr
			if err == nil && op.State == "RUNNING" {
				err = a.executeOperation(ctx, op)
				if err != nil && ctx.Err() == nil {
					if saveErr := a.failOperation(ctx, op.OperationID, err.Error()); saveErr != nil {
						slog.Error("persist failed operation", "error", saveErr)
					}
				}
			}
		}
		if err != nil && ctx.Err() == nil {
			slog.Error("demo control", "error", err)
		}
		if !messaging.Wait(ctx, 100*time.Millisecond) {
			return
		}
	}
}

func (a *App) executeOperation(ctx context.Context, op Operation) error {
	if op.Kind != "prepare" && op.Kind != "reset" {
		return a.executeResume(ctx, op)
	}
	phases := []string{"QUIESCE", "INITIALIZE", "START", "OPEN"}
	start := 0
	for i, p := range phases {
		if p == op.Stage {
			start = i
		}
	}
	if start == 0 {
		a.deliveryGate.Lock()
		local, cancel := context.WithTimeout(ctx, storage.Timeout)
		_, err := a.DB.ExecContext(local, "UPDATE purchase_jobs SET state='CANCELED',updated_at_ms=? WHERE state='PENDING' AND run_id<>?", time.Now().UnixMilli(), op.RunID)
		cancel()
		a.deliveryGate.Unlock()
		if err != nil {
			return err
		}
	}
	for i := start; i < len(phases); i++ {
		op.Stage = phases[i]
		op.Components = map[string]string{}
		if err := a.progress(ctx, op); err != nil {
			return err
		}
		phaseCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := a.executePhase(phaseCtx, &op, i+1)
		cancel()
		if err != nil {
			return err
		}
	}
	return a.completeOperation(ctx, op)
}

func (a *App) executePhase(ctx context.Context, op *Operation, phase int) error {
	run := contracts.Run{OperationID: op.OperationID, RunID: op.RunID, Generation: op.Generation, Scenario: op.Scenario, InitialSeats: op.InitialSeats, CreatedAtMS: op.CreatedAtMS, Phase: phase}
	for _, role := range contracts.Services {
		for {
			err := httpapi.Call(ctx, a.Client, "PUT", a.Config.URLs[role]+"/internal/run-control", run, nil)
			if err == nil {
				op.Components[role] = "READY"
				if err = a.progress(ctx, *op); err != nil {
					return err
				}
				break
			}
			op.Components[role] = err.Error()
			if saveErr := a.progress(context.WithoutCancel(ctx), *op); saveErr != nil {
				return saveErr
			}
			if !messaging.Wait(ctx, 250*time.Millisecond) {
				return fmt.Errorf("%s %s: %w", op.Stage, role, err)
			}
		}
	}
	return nil
}

func (a *App) executeResume(ctx context.Context, op Operation) error {
	role, worker := "reservations", "consumer"
	if op.Kind == "restore_payments" {
		role = "payments"
	}
	if op.Kind == "resume_orders_outbox" {
		role = "orders"
		worker = "publisher"
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	body := map[string]any{"run_id": op.RunID, "generation": op.Generation, "desired_state": "RUNNING"}
	for {
		err := httpapi.Call(ctx, a.Client, "PUT", a.Config.URLs[role]+"/internal/workers/"+worker, body, nil)
		if err == nil {
			var state control.ServiceState
			err = httpapi.Call(ctx, a.Client, "GET", a.Config.URLs[role]+"/internal/state", nil, &state)
			if err == nil && state.RunID == op.RunID && state.Generation == op.Generation && state.Workers[worker].ActualState == "RUNNING" {
				op.Components = map[string]string{role: "READY"}
				if err = a.progress(ctx, op); err != nil {
					return err
				}
				return a.completeOperation(ctx, op)
			}
		}
		if !messaging.Wait(ctx, 200*time.Millisecond) {
			return fmt.Errorf("restore %s did not become ready: %v", role, err)
		}
	}
}

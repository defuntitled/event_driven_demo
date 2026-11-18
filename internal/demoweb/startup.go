package demoweb

import (
	"context"
	"fmt"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (a *App) PublicSession(ctx context.Context) (Session, error) {
	s, err := a.Session(ctx)
	if err != nil || !s.CanPurchase {
		return s, err
	}
	var state control.ServiceState
	if err = httpapi.Call(ctx, a.Client, "GET", a.Config.URLs["orders"]+"/internal/state", nil, &state); err != nil {
		s.CanPurchase = false
		return s, nil
	}
	s.CanPurchase = state.RunID == s.RunID && state.Generation == s.Generation && state.Admission == "OPEN"
	return s, nil
}

func (a *App) reconcileStartup(ctx context.Context) error {
	s, err := a.Session(ctx)
	if err != nil || s.RunID == "" || s.ActiveOperationID != nil {
		return err
	}
	local, cancel := context.WithTimeout(ctx, storage.Timeout)
	_, err = a.DB.ExecContext(local, "UPDATE demo_state SET state='PREPARING' WHERE singleton=1 AND active_operation_id IS NULL")
	cancel()
	if err != nil {
		return err
	}
	deadline, done := context.WithTimeout(ctx, 20*time.Second)
	defer done()
	for {
		ready := true
		for _, role := range contracts.Services {
			var state control.ServiceState
			err = httpapi.Call(deadline, a.Client, "GET", a.Config.URLs[role]+"/internal/state", nil, &state)
			if err != nil || state.RunID != s.RunID || state.Generation != s.Generation || state.Admission != "OPEN" {
				ready = false
				break
			}
		}
		if ready {
			local, cancel := context.WithTimeout(ctx, storage.Timeout)
			defer cancel()
			_, err = a.DB.ExecContext(local, "UPDATE demo_state SET state='READY' WHERE singleton=1 AND active_run_id=? AND active_operation_id IS NULL", s.RunID)
			return err
		}
		if !messaging.Wait(deadline, 200*time.Millisecond) {
			return fmt.Errorf("reconcile saved run %s: %w", s.RunID, deadline.Err())
		}
	}
}

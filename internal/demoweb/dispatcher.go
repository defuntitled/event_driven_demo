package demoweb

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type job struct {
	ID, RunID, Body, Headers string
	Attempts                 int
}

func (a *App) pending(ctx context.Context) ([]job, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	rows, err := a.DB.QueryContext(ctx, `SELECT j.job_id,j.run_id,j.body_json,j.attempts,j.headers_json FROM purchase_jobs j JOIN demo_state s ON s.active_run_id=j.run_id WHERE s.state='READY' AND j.state='PENDING' AND j.next_retry_at_ms<=? ORDER BY j.rowid LIMIT 100`, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []job
	for rows.Next() {
		var j job
		if err = rows.Scan(&j.ID, &j.RunID, &j.Body, &j.Attempts, &j.Headers); err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

func (a *App) Dispatch(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for ctx.Err() == nil {
		jobs, err := a.pending(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("read purchase jobs", "error", err)
		}
		start := make(chan struct{})
		for _, j := range jobs {
			a.jobsMu.Lock()
			active := a.inflight[j.ID]
			if !active {
				a.inflight[j.ID] = true
			}
			a.jobsMu.Unlock()
			if active {
				continue
			}
			wg.Go(func() {
				defer func() { a.jobsMu.Lock(); delete(a.inflight, j.ID); a.jobsMu.Unlock() }()
				<-start
				a.deliver(ctx, j)
			})
		}
		close(start)
		if !messaging.Wait(ctx, 50*time.Millisecond) {
			return
		}
	}
}

func (a *App) deliver(ctx context.Context, j job) {
	a.deliveryGate.RLock()
	defer a.deliveryGate.RUnlock()
	local, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var eligible int
	err := a.DB.QueryRowContext(local, `SELECT count(*) FROM purchase_jobs j JOIN demo_state s ON s.active_run_id=j.run_id WHERE j.job_id=? AND j.state='PENDING' AND s.state='READY'`, j.ID).Scan(&eligible)
	if err != nil {
		slog.Error("check purchase job", "error", err)
		return
	}
	if eligible == 0 {
		return
	}
	var command orders.CreateCommand
	err = json.Unmarshal([]byte(j.Body), &command)
	var headers http.Header
	if err == nil {
		err = json.Unmarshal([]byte(j.Headers), &headers)
	}
	var accepted orders.Order
	if err == nil {
		err = httpapi.Call(httpapi.WithHeaders(ctx, headers), a.Client, "POST", a.Config.URLs["orders"]+"/internal/orders", command, &accepted)
	}
	if err == nil && (accepted.OrderID != command.OrderID || accepted.RunID != command.RunID) {
		err = contracts.Fail(503, "INVALID_RESPONSE", "Сервис заказов вернул другие идентификаторы")
	}
	state := "ACCEPTED"
	var message any
	next := int64(0)
	if err != nil {
		state = "PENDING"
		message = err.Error()
		next = time.Now().Add(time.Duration(250*(1<<min(j.Attempts, 3))) * time.Millisecond).UnixMilli()
		if f, ok := errors.AsType[*contracts.Failure](err); ok && f.Status >= 400 && f.Status < 500 {
			state = "FAILED"
		}
	}
	saved, done := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer done()
	if _, saveErr := a.DB.ExecContext(saved, "UPDATE purchase_jobs SET state=?,attempts=attempts+1,last_error=?,next_retry_at_ms=?,updated_at_ms=? WHERE job_id=? AND state='PENDING'", state, message, next, time.Now().UnixMilli(), j.ID); saveErr != nil {
		slog.Error("persist job delivery", "error", saveErr)
	}
}

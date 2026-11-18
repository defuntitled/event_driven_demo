package demoweb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type Purchase struct {
	RunID     string `json:"run_id"`
	ConcertID string `json:"concert_id"`
	Quantity  int    `json:"quantity"`
}

func (a *App) purchase(w http.ResponseWriter, r *http.Request) {
	var p Purchase
	if err := httpapi.Read(w, r, &p); err != nil {
		httpapi.Error(w, err)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 || p.ConcertID != contracts.ConcertID || p.Quantity != 1 {
		httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Некорректная покупка или Idempotency-Key"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.Config.HTTPTimeout)
	defer cancel()
	a.deliveryGate.RLock()
	command, err := a.savePurchase(ctx, key, p)
	a.deliveryGate.RUnlock()
	if err != nil {
		httpapi.Error(w, err)
		return
	}
	for ctx.Err() == nil {
		var state string
		var message sql.NullString
		err = a.DB.QueryRowContext(ctx, "SELECT state,last_error FROM purchase_jobs WHERE order_id=?", command.OrderID).Scan(&state, &message)
		if err != nil {
			break
		}
		switch state {
		case "ACCEPTED":
			httpapi.Write(w, 202, map[string]any{"run_id": command.RunID, "generation": command.Generation, "order_id": command.OrderID, "status": "PROCESSING", "status_url": "/api/orders/" + command.OrderID + "?run_id=" + command.RunID})
			return
		case "CANCELED":
			httpapi.Error(w, contracts.Fail(409, "STALE_RUN", "Прогон изменился до доставки покупки"))
			return
		case "FAILED":
			httpapi.Error(w, contracts.Fail(400, "PURCHASE_FAILED", message.String))
			return
		}
		if !messaging.Wait(ctx, 25*time.Millisecond) {
			break
		}
	}
	httpapi.Write(w, 503, map[string]any{"error": map[string]any{"code": "ACCEPTANCE_UNCONFIRMED", "message": "Приём покупки ещё не подтверждён. Повторите тот же ключ", "retryable": true}, "request_key": key})
}

func (a *App) savePurchase(ctx context.Context, key string, p Purchase) (orders.CreateCommand, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var command orders.CreateCommand
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return command, err
	}
	defer tx.Rollback()
	var active, state string
	var gen int64
	if err = tx.QueryRowContext(ctx, `SELECT s.active_run_id,s.state,r.generation FROM demo_state s JOIN demo_runs r ON r.run_id=s.active_run_id WHERE s.singleton=1`).Scan(&active, &state, &gen); err != nil {
		return command, err
	}
	if active != p.RunID {
		return command, contracts.Fail(409, "STALE_RUN", "Демо переключено на другой прогон")
	}
	if state != "READY" {
		return command, contracts.Fail(503, "PREPARING", "Подготовка нового прогона")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return command, err
	}
	hash := contracts.Hash(raw)
	var oldHash, body string
	err = tx.QueryRowContext(ctx, `SELECT r.request_hash,j.body_json FROM purchase_requests r JOIN purchase_jobs j ON j.order_id=r.primary_order_id WHERE r.run_id=? AND r.request_key=?`, p.RunID, key).Scan(&oldHash, &body)
	if err == nil {
		if oldHash != hash {
			return command, contracts.Fail(409, "IDEMPOTENCY_CONFLICT", "Ключ уже использован с другими параметрами")
		}
		if err = json.Unmarshal([]byte(body), &command); err != nil {
			return command, err
		}
		return command, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return command, err
	}

	command, err = newPurchaseCommand(p, gen, "presenter")
	if err != nil {
		return command, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO purchase_requests(run_id,request_key,request_hash,primary_order_id,created_at_ms) VALUES (?,?,?,?,?)", p.RunID, key, hash, command.OrderID, time.Now().UnixMilli())
	if err != nil {
		return command, err
	}
	if err = insertJob(ctx, tx, key, command); err != nil {
		return command, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE demo_runs SET race_claimed=1 WHERE run_id=? AND scenario='race_last_ticket' AND race_claimed=0", p.RunID)
	if err != nil {
		return command, err
	}
	claimed, err := result.RowsAffected()
	if err != nil {
		return command, err
	}
	if claimed == 1 {
		competitor, err := newPurchaseCommand(p, gen, "demo-competitor")
		if err != nil {
			return command, err
		}
		if err = insertJob(ctx, tx, key, competitor); err != nil {
			return command, err
		}
	}
	return command, tx.Commit()
}

func newPurchaseCommand(p Purchase, gen int64, buyer string) (orders.CreateCommand, error) {
	var c orders.CreateCommand
	id, err := contracts.NewID("ord")
	if err != nil {
		return c, err
	}
	req, err := contracts.NewID("req")
	if err != nil {
		return c, err
	}
	return orders.CreateCommand{RunID: p.RunID, Generation: gen, RequestID: req, OrderID: id, BuyerID: buyer, ConcertID: p.ConcertID, Quantity: 1}, nil
}
func insertJob(ctx context.Context, tx *sql.Tx, key string, c orders.CreateCommand) error {
	id, err := contracts.NewID("job")
	if err != nil {
		return err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	headers, err := json.Marshal(httpapi.ForwardHeaders(ctx))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO purchase_jobs(job_id,run_id,request_key,request_id,order_id,buyer_id,state,body_json,updated_at_ms,headers_json) VALUES (?,?,?,?,?,?,'PENDING',?,?,?)`, id, c.RunID, key, c.RequestID, c.OrderID, c.BuyerID, string(raw), time.Now().UnixMilli(), string(headers))
	return err
}

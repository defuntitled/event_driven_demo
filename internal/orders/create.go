package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type CreateCommand struct {
	RunID      string `json:"run_id"`
	Generation int64  `json:"generation"`
	RequestID  string `json:"request_id"`
	OrderID    string `json:"order_id"`
	BuyerID    string `json:"buyer_id"`
	ConcertID  string `json:"concert_id"`
	Quantity   int    `json:"quantity"`
}

type Order struct {
	OrderID     string    `json:"order_id"`
	RunID       string    `json:"run_id"`
	BuyerID     string    `json:"buyer_id"`
	Version     int64     `json:"version"`
	Status      string    `json:"status"`
	AmountCents int       `json:"amount_cents"`
	Currency    string    `json:"currency"`
	TicketID    *string   `json:"ticket_id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func CreateOrder(ctx context.Context, db *sql.DB, c CreateCommand) (Order, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	if !contracts.ValidID(c.RunID, "run") || !contracts.ValidID(c.OrderID, "ord") || !contracts.ValidID(c.RequestID, "req") || c.Generation <= 0 || c.ConcertID != contracts.ConcertID || c.Quantity != 1 || c.BuyerID == "" || len(c.BuyerID) > 100 {
		return Order{}, contracts.Fail(400, "INVALID_REQUEST", "Некорректная команда покупки")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return Order{}, err
	}
	hash := contracts.Hash(raw)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback()
	if err = storage.CheckRun(ctx, tx, c.RunID, c.Generation, true); err != nil {
		return Order{}, err
	}
	var id, oldHash string
	err = tx.QueryRowContext(ctx, "SELECT order_id,request_hash FROM orders WHERE run_id=? AND request_id=?", c.RunID, c.RequestID).Scan(&id, &oldHash)
	if err == nil {
		if oldHash != hash {
			return Order{}, contracts.Fail(409, "IDEMPOTENCY_CONFLICT", "Ключ покупки уже использован с другими параметрами")
		}
		if err = tx.Commit(); err != nil {
			return Order{}, err
		}
		return GetOrder(ctx, db, c.RunID, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Order{}, err
	}
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO orders(order_id,run_id,request_id,request_hash,buyer_id,concert_id,quantity,amount_cents,currency,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,1,50000,'RUB',?,?)`, c.OrderID, c.RunID, c.RequestID, hash, c.BuyerID, c.ConcertID, now, now)
	if err != nil {
		return Order{}, err
	}
	e, err := contracts.NewEvent(contracts.OrderCreated, c.RunID, c.OrderID, nil, contracts.Payload{ConcertID: c.ConcertID, BuyerID: c.BuyerID, Quantity: 1, AmountCents: 50000, Currency: "RUB"})
	if err != nil {
		return Order{}, err
	}
	if err = storage.AppendEvent(ctx, tx, e, "order.created/"+c.OrderID); err != nil {
		return Order{}, err
	}
	if err = tx.Commit(); err != nil {
		return Order{}, err
	}
	return GetOrder(ctx, db, c.RunID, c.OrderID)
}

func GetOrder(ctx context.Context, db *sql.DB, run, id string) (Order, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var o Order
	var created, updated int64
	err := db.QueryRowContext(ctx, `SELECT order_id,run_id,buyer_id,version,status,amount_cents,currency,ticket_id,created_at_ms,updated_at_ms FROM orders WHERE run_id=? AND order_id=?`, run, id).Scan(&o.OrderID, &o.RunID, &o.BuyerID, &o.Version, &o.Status, &o.AmountCents, &o.Currency, &o.TicketID, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return o, contracts.Fail(404, "ORDER_NOT_FOUND", "Заказ не найден")
	}
	o.CreatedAt = time.UnixMilli(created).UTC()
	o.UpdatedAt = time.UnixMilli(updated).UTC()
	return o, err
}

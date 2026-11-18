package payments

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func Charge(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
	var concert, currency string
	var amount int
	err := tx.QueryRowContext(ctx, "SELECT concert_id,amount_cents,currency FROM payment_operations WHERE run_id=? AND order_id=? AND kind='CHARGE'", e.RunID, e.OrderID).Scan(&concert, &amount, &currency)
	if err == nil {
		if concert != e.Payload.ConcertID || amount != e.Payload.AmountCents || currency != e.Payload.Currency {
			return "", contracts.Protocol("conflicting charge")
		}
		return "DUPLICATE_BUSINESS", nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	id, err := contracts.NewID("pay")
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payment_operations(payment_id,run_id,order_id,concert_id,kind,amount_cents,currency,source_event_id,created_at_ms) VALUES (?,?,?,?,'CHARGE',50000,'RUB',?,?)`, id, e.RunID, e.OrderID, e.Payload.ConcertID, e.EventID, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}
	out, err := contracts.NewEvent(contracts.PaymentSucceeded, e.RunID, e.OrderID, new(e.EventID), contracts.Payload{ConcertID: e.Payload.ConcertID, Quantity: 1, AmountCents: 50000, Currency: "RUB", PaymentID: id})
	if err != nil {
		return "", err
	}
	if err = storage.AppendEvent(ctx, tx, out, "payment.charged/"+e.OrderID); err != nil {
		return "", err
	}
	return "PROCESSED", nil
}

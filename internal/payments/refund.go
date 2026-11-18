package payments

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func Handle(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
	switch e.Type {
	case contracts.OrderCreated:
		return Charge(ctx, tx, e)
	case contracts.SeatReservationFailed:
		return RefundPayment(ctx, tx, e)
	}
	return "", contracts.Protocol("unsupported payment event %s", e.Type)
}

func RefundPayment(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
	if e.Payload.Reason != "NO_SEATS" {
		return "", contracts.Protocol("invalid refund reason")
	}
	var run, order, concert, currency string
	var amount int
	err := tx.QueryRowContext(ctx, "SELECT run_id,order_id,concert_id,amount_cents,currency FROM payment_operations WHERE payment_id=? AND kind='CHARGE'", e.Payload.PaymentID).Scan(&run, &order, &concert, &amount, &currency)
	if errors.Is(err, sql.ErrNoRows) {
		return "", contracts.Protocol("missing original charge")
	}
	if err != nil {
		return "", err
	}
	if run != e.RunID || order != e.OrderID || concert != e.Payload.ConcertID {
		return "", contracts.Protocol("inconsistent original charge")
	}
	var original string
	err = tx.QueryRowContext(ctx, "SELECT original_payment_id FROM payment_operations WHERE run_id=? AND order_id=? AND kind='REFUND'", run, order).Scan(&original)
	if err == nil {
		if original != e.Payload.PaymentID {
			return "", contracts.Protocol("conflicting refund")
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
	_, err = tx.ExecContext(ctx, `INSERT INTO payment_operations(payment_id,run_id,order_id,concert_id,kind,amount_cents,currency,original_payment_id,source_event_id,created_at_ms) VALUES (?,?,?,?,'REFUND',?,?,?,?,?)`, id, run, order, concert, amount, currency, e.Payload.PaymentID, e.EventID, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}
	out, err := contracts.NewEvent(contracts.PaymentRefunded, run, order, new(e.EventID), contracts.Payload{ConcertID: concert, AmountCents: amount, Currency: currency, PaymentID: id, OriginalPaymentID: e.Payload.PaymentID})
	if err != nil {
		return "", err
	}
	if err = storage.AppendEvent(ctx, tx, out, "payment.refunded/"+order); err != nil {
		return "", err
	}
	return "PROCESSED", nil
}

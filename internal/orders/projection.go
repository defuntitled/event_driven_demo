package orders

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

type facts struct {
	paid, refund             int
	outcome, concert         string
	charge, ticket, refundID sql.NullString
}

func Apply(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
	var f facts
	err := tx.QueryRowContext(ctx, `SELECT payment_seen,refund_seen,reservation_outcome,charge_id,ticket_id,refund_id,concert_id FROM orders WHERE run_id=? AND order_id=?`, e.RunID, e.OrderID).Scan(&f.paid, &f.refund, &f.outcome, &f.charge, &f.ticket, &f.refundID, &f.concert)
	if errors.Is(err, sql.ErrNoRows) {
		return "", contracts.Protocol("missing order %s", e.OrderID)
	}
	if err != nil {
		return "", err
	}
	before := f
	if err = applyFact(&f, e); err != nil {
		return "", err
	}
	if before == f {
		return "DUPLICATE_BUSINESS", nil
	}
	status := "PROCESSING"
	switch {
	case f.refund == 1:
		status = "REFUNDED"
	case f.outcome == "RESERVED":
		status = "CONFIRMED"
	case f.outcome == "REJECTED":
		status = "REFUND_PENDING"
	case f.paid == 1:
		status = "PAID_PENDING_RESERVATION"
	}
	_, err = tx.ExecContext(ctx, `UPDATE orders SET payment_seen=?,refund_seen=?,reservation_outcome=?,charge_id=?,ticket_id=?,refund_id=?,status=?,version=version+1,updated_at_ms=? WHERE run_id=? AND order_id=?`, f.paid, f.refund, f.outcome, f.charge, f.ticket, f.refundID, status, time.Now().UnixMilli(), e.RunID, e.OrderID)
	return "PROCESSED", err
}

func applyFact(f *facts, e contracts.Event) error {
	charge := e.Payload.PaymentID
	if e.Type == contracts.PaymentRefunded {
		charge = e.Payload.OriginalPaymentID
	}
	if f.concert != e.Payload.ConcertID || f.charge.Valid && f.charge.String != charge {
		return contracts.Protocol("inconsistent payment reference")
	}
	f.charge = sql.NullString{String: charge, Valid: true}
	switch e.Type {
	case contracts.PaymentSucceeded:
		f.paid = 1
	case contracts.SeatReserved:
		if f.outcome == "REJECTED" || f.refund != 0 || f.ticket.Valid && f.ticket.String != e.Payload.TicketID {
			return contracts.Protocol("inconsistent reservation")
		}
		f.outcome = "RESERVED"
		f.ticket = sql.NullString{String: e.Payload.TicketID, Valid: true}
	case contracts.SeatReservationFailed:
		if f.outcome == "RESERVED" {
			return contracts.Protocol("conflicting reservation outcome")
		}
		f.outcome = "REJECTED"
	case contracts.PaymentRefunded:
		if f.outcome == "RESERVED" || f.refundID.Valid && f.refundID.String != e.Payload.PaymentID {
			return contracts.Protocol("conflicting refund")
		}
		f.refund = 1
		f.refundID = sql.NullString{String: e.Payload.PaymentID, Valid: true}
	default:
		return contracts.Protocol("unsupported projection event %s", e.Type)
	}
	return nil
}

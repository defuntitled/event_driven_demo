package reservations

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func ReserveSeat(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
	var concert, payment string
	err := tx.QueryRowContext(ctx, "SELECT concert_id,payment_id FROM reservation_decisions WHERE run_id=? AND order_id=?", e.RunID, e.OrderID).Scan(&concert, &payment)
	if err == nil {
		if concert != e.Payload.ConcertID || payment != e.Payload.PaymentID {
			return "", contracts.Protocol("conflicting reservation")
		}
		return "DUPLICATE_BUSINESS", nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	result, err := tx.ExecContext(ctx, "UPDATE inventory SET available=available-1 WHERE run_id=? AND concert_id=? AND available>=1", e.RunID, e.Payload.ConcertID)
	if err != nil {
		return "", err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	outcome, typ := "RESERVED", contracts.SeatReserved
	var ticket, reason *string
	if n == 1 {
		id, err := contracts.NewID("tkt")
		if err != nil {
			return "", err
		}
		ticket = new(id)
	} else {
		var available int
		if err = tx.QueryRowContext(ctx, "SELECT available FROM inventory WHERE run_id=? AND concert_id=?", e.RunID, e.Payload.ConcertID).Scan(&available); err != nil {
			return "", contracts.Protocol("missing inventory: %s", err)
		}
		if available != 0 {
			return "", contracts.Protocol("inconsistent inventory")
		}
		outcome, typ = "REJECTED", contracts.SeatReservationFailed
		reason = new("NO_SEATS")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO reservation_decisions(run_id,order_id,concert_id,payment_id,outcome,ticket_id,reason,source_event_id,created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)`, e.RunID, e.OrderID, e.Payload.ConcertID, e.Payload.PaymentID, outcome, ticket, reason, e.EventID, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}
	payload := contracts.Payload{ConcertID: e.Payload.ConcertID, Quantity: 1, PaymentID: e.Payload.PaymentID}
	if ticket != nil {
		payload.TicketID = *ticket
	}
	if reason != nil {
		payload.Reason = *reason
	}
	out, err := contracts.NewEvent(typ, e.RunID, e.OrderID, new(e.EventID), payload)
	if err != nil {
		return "", err
	}
	if err = storage.AppendEvent(ctx, tx, out, "reservation.decided/"+e.OrderID); err != nil {
		return "", err
	}
	return "PROCESSED", nil
}

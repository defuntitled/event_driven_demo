package reservations

import (
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestReserveAtomic(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "reserve.db"), "reservations")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); INSERT INTO inventory VALUES (?,'concert-1',1,1)", run, run)
	if err != nil {
		t.Fatal(err)
	}
	e, err := contracts.NewEvent(contracts.PaymentSucceeded, run, "ord_0123456789abcdef0123456789abcdef", new("evt_0123456789abcdef0123456789abcdef"), contracts.Payload{ConcertID: contracts.ConcertID, Quantity: 1, AmountCents: 50000, Currency: "RUB", PaymentID: "pay_0123456789abcdef0123456789abcdef"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ReserveSeat(t.Context(), tx, e); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT available FROM inventory").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("inventory escaped rollback")
	}
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ReserveSeat(t.Context(), tx, e); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT available FROM inventory").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("seat not reserved")
	}
	var ticket string
	if err = db.QueryRowContext(t.Context(), "SELECT ticket_id FROM reservation_decisions WHERE outcome='RESERVED'").Scan(&ticket); err != nil {
		t.Fatal(err)
	}
	if !contracts.ValidID(ticket, "tkt") {
		t.Fatal(ticket)
	}
}

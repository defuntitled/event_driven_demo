package orders

import (
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestConfirmedProjection(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "orders.db"), "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1,admission='OPEN'", run, run)
	if err != nil {
		t.Fatal(err)
	}
	c := CreateCommand{RunID: run, Generation: 1, RequestID: "req_0123456789abcdef0123456789abcdef", OrderID: "ord_0123456789abcdef0123456789abcdef", BuyerID: "presenter", ConcertID: contracts.ConcertID, Quantity: 1}
	if _, err = CreateOrder(t.Context(), db, c); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{contracts.PaymentSucceeded, contracts.SeatReserved} {
		e, err := contracts.NewEvent(typ, run, c.OrderID, new("evt_0123456789abcdef0123456789abcdef"), contracts.Payload{ConcertID: contracts.ConcertID, PaymentID: "pay_0123456789abcdef0123456789abcdef", TicketID: "tkt_0123456789abcdef0123456789abcdef", Quantity: 1, AmountCents: 50000, Currency: "RUB"})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = Apply(t.Context(), tx, e); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	o, err := GetOrder(t.Context(), db, run, c.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != "CONFIRMED" || o.TicketID == nil || o.Version != 3 {
		t.Fatalf("projection=%+v", o)
	}
}

package payments

import (
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestChargeAtomic(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "payments.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0)", run)
	if err != nil {
		t.Fatal(err)
	}
	e, err := contracts.NewEvent(contracts.OrderCreated, run, "ord_0123456789abcdef0123456789abcdef", nil, contracts.Payload{ConcertID: contracts.ConcertID, BuyerID: "presenter", Quantity: 1, AmountCents: 50000, Currency: "RUB"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Charge(t.Context(), tx, e); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM payment_operations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("charge escaped rollback")
	}
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Charge(t.Context(), tx, e); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM payment_operations WHERE kind='CHARGE' AND amount_cents=50000").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("charges=%d", n)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM outbox WHERE event_type='PaymentSucceeded'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("missing atomic outbox")
	}
}

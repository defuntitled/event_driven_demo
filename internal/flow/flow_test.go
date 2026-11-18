package flow_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/reservations"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

const runID = "run_0123456789abcdef0123456789abcdef"

type fixture struct {
	db    map[string]*sql.DB
	order orders.Order
}

func setup(t *testing.T, seats int) fixture {
	t.Helper()
	f := fixture{db: make(map[string]*sql.DB, 3)}
	for _, role := range contracts.Services {
		db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), role+".db"), role)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		f.db[role] = db
		_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',?,0); UPDATE service_state SET active_run_id=?,generation=1,admission='OPEN'; UPDATE worker_control SET run_id=?,generation=1,desired_state='RUNNING'", runID, seats, runID, runID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db["reservations"].ExecContext(t.Context(), "INSERT INTO inventory VALUES (?,'concert-1',?,?)", runID, seats, seats); err != nil {
		t.Fatal(err)
	}
	o, err := orders.CreateOrder(t.Context(), f.db["orders"], orders.CreateCommand{RunID: runID, Generation: 1, RequestID: "req_0123456789abcdef0123456789abcdef", OrderID: "ord_0123456789abcdef0123456789abcdef", BuyerID: "presenter", ConcertID: contracts.ConcertID, Quantity: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.order = o
	return f
}
func event(t *testing.T, db *sql.DB, typ string) contracts.Event {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(t.Context(), "SELECT envelope_json FROM outbox WHERE event_type=?", typ).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var e contracts.Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	return e
}
func apply(t *testing.T, db *sql.DB, role string, e contracts.Event) (storage.Result, error) {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return storage.Process(t.Context(), db, role, runID, 1, "test-worker", contracts.Record{Topic: e.Producer + ".events", Key: e.OrderID, Value: raw, Offset: 42}, messaging.Handler(role))
}
func mustApply(t *testing.T, db *sql.DB, role string, e contracts.Event) {
	t.Helper()
	if _, err := apply(t, db, role, e); err != nil {
		t.Fatal(err)
	}
}
func count(t *testing.T, db *sql.DB, q string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(t.Context(), q).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", q, got, want)
	}
}

func TestNoSeatsRefundAndLateFacts(t *testing.T) {
	t.Parallel()
	f := setup(t, 0)
	created := event(t, f.db["orders"], contracts.OrderCreated)
	mustApply(t, f.db["payments"], "payments", created)
	paid := event(t, f.db["payments"], contracts.PaymentSucceeded)
	mustApply(t, f.db["reservations"], "reservations", paid)
	rejected := event(t, f.db["reservations"], contracts.SeatReservationFailed)
	mustApply(t, f.db["payments"], "payments", rejected)
	refunded := event(t, f.db["payments"], contracts.PaymentRefunded)
	for _, e := range []contracts.Event{refunded, rejected, paid} {
		mustApply(t, f.db["orders"], "orders", e)
		o, err := orders.GetOrder(t.Context(), f.db["orders"], runID, f.order.OrderID)
		if err != nil {
			t.Fatal(err)
		}
		if o.Status != "REFUNDED" {
			t.Fatalf("late %s changed status to %s", e.Type, o.Status)
		}
	}
	count(t, f.db["payments"], "SELECT count(*) FROM payment_operations", 2)
	count(t, f.db["payments"], "SELECT sum(CASE kind WHEN 'CHARGE' THEN amount_cents ELSE -amount_cents END) FROM payment_operations", 0)
}

func TestDuplicateBusinessAndProtocolConflict(t *testing.T) {
	t.Parallel()
	f := setup(t, 1)
	e := event(t, f.db["orders"], contracts.OrderCreated)
	mustApply(t, f.db["payments"], "payments", e)
	same := e
	e.EventID = "evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	r, err := apply(t, f.db["payments"], "payments", e)
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "DUPLICATE_BUSINESS" {
		t.Fatal(r)
	}
	count(t, f.db["payments"], "SELECT count(*) FROM payment_operations", 1)
	count(t, f.db["payments"], "SELECT count(*) FROM outbox", 1)
	same.Payload.BuyerID = "changed"
	if _, err = apply(t, f.db["payments"], "payments", same); err == nil {
		t.Fatal("conflicting event bytes accepted")
	}
}

func TestLatePaymentAndContradiction(t *testing.T) {
	t.Parallel()
	f := setup(t, 1)
	mustApply(t, f.db["payments"], "payments", event(t, f.db["orders"], contracts.OrderCreated))
	paid := event(t, f.db["payments"], contracts.PaymentSucceeded)
	mustApply(t, f.db["reservations"], "reservations", paid)
	reserved := event(t, f.db["reservations"], contracts.SeatReserved)
	mustApply(t, f.db["orders"], "orders", reserved)
	mustApply(t, f.db["orders"], "orders", paid)
	bad := reserved
	bad.EventID = "evt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	bad.Type = contracts.SeatReservationFailed
	bad.Payload.Reason = "NO_SEATS"
	bad.Payload.TicketID = ""
	if _, err := apply(t, f.db["orders"], "orders", bad); err == nil {
		t.Fatal("contradiction accepted")
	}
	o, err := orders.GetOrder(t.Context(), f.db["orders"], runID, f.order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != "CONFIRMED" {
		t.Fatal(o.Status)
	}
}

func TestConcurrentReserveConnections(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "reserve.db")
	db, err := storage.Open(t.Context(), path, "reservations")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); INSERT INTO inventory VALUES (?,'concert-1',1,1)", runID, runID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 2 {
		wg.Go(func() {
			other, err := storage.Open(t.Context(), path, "reservations")
			if err != nil {
				t.Error(err)
				return
			}
			defer other.Close()
			order, err := contracts.NewID("ord")
			if err != nil {
				t.Error(err)
				return
			}
			payment, err := contracts.NewID("pay")
			if err != nil {
				t.Error(err)
				return
			}
			e, err := contracts.NewEvent(contracts.PaymentSucceeded, runID, order, new("evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), contracts.Payload{ConcertID: contracts.ConcertID, Quantity: 1, AmountCents: 50000, Currency: "RUB", PaymentID: payment})
			if err != nil {
				t.Error(err)
				return
			}
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), storage.Timeout)
			defer cancel()
			tx, err := other.BeginTx(ctx, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback()
			if _, err = reservations.ReserveSeat(ctx, tx, e); err != nil {
				t.Error(err)
				return
			}
			if err = tx.Commit(); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	count(t, db, "SELECT available FROM inventory", 0)
	count(t, db, "SELECT count(*) FROM reservation_decisions WHERE outcome='RESERVED'", 1)
	count(t, db, "SELECT count(*) FROM reservation_decisions WHERE outcome='REJECTED'", 1)
}

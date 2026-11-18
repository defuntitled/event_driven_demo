package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func TestBusyRetriesSameAttempt(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "payments.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1; UPDATE worker_control SET run_id=?,generation=1,desired_state='RUNNING'", run, run, run)
	if err != nil {
		t.Fatal(err)
	}
	e, err := contracts.NewEvent(contracts.OrderCreated, run, "ord_0123456789abcdef0123456789abcdef", nil, contracts.Payload{ConcertID: contracts.ConcertID, BuyerID: "presenter", Quantity: 1, AmountCents: 50000, Currency: "RUB"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := func(context.Context, *sql.Tx, contracts.Event) (string, error) {
		calls++
		if calls < 3 {
			return "", sqlite3.Error{Code: sqlite3.ErrBusy}
		}
		return "PROCESSED", nil
	}
	result, err := Process(t.Context(), db, "payments", run, 1, "test-worker", contracts.Record{Topic: "orders.events", Key: e.OrderID, Value: raw}, handler)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "PROCESSED" {
		t.Fatal(result)
	}
	var retries, n int
	if err = db.QueryRowContext(t.Context(), "SELECT database_retries FROM processing_attempts WHERE attempt_id=?", result.AttemptID).Scan(&retries); err != nil {
		t.Fatal(err)
	}
	if retries != 2 {
		t.Fatalf("retries=%d", retries)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM processing_attempts").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal(n)
	}
}

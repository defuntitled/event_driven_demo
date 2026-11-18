package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func TestProcessAtomic(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "payment.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1,admission='OPEN'; UPDATE worker_control SET run_id=?,generation=1,desired_state='RUNNING'", run, run, run)
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
	r := contracts.Record{Topic: "orders.events", Key: e.OrderID, Value: raw, Offset: 42}
	handler := func(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
		return "", errors.New("injected rollback")
	}
	if _, err = Process(t.Context(), db, "payments", run, 1, "worker-1", r, handler); err == nil {
		t.Fatal("handler failure swallowed")
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM processed_events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("inbox escaped rollback")
	}
	calls := 0
	handler = func(ctx context.Context, tx *sql.Tx, e contracts.Event) (string, error) {
		calls++
		return "PROCESSED", nil
	}
	result, err := Process(t.Context(), db, "payments", run, 1, "worker-1", r, handler)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "PROCESSED" {
		t.Fatal(result)
	}
	result, err = Process(t.Context(), db, "payments", run, 1, "worker-2", r, handler)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "DUPLICATE_EVENT" || calls != 1 {
		t.Fatalf("result=%+v calls=%d", result, calls)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM processing_attempts WHERE offset_value=42").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("attempts=%d", n)
	}
}

func TestMalformedRecordVisibleInActiveRun(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "bad.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = Process(t.Context(), db, "payments", run, 1, "worker", contracts.Record{Topic: "orders.events", Partition: 1, Offset: 42, Value: []byte("not-json")}, nil)
	if err == nil {
		t.Fatal("malformed record accepted")
	}
	p, err := Records(t.Context(), db, "payments", "processing_attempts", run, "", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 || p.Items[0]["raw_json"] != "not-json" {
		t.Fatalf("poison diagnostics hidden: %+v", p)
	}
}

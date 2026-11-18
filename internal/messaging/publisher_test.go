package messaging

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestPublishOutsideTransaction(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "orders.db"), "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := "run_0123456789abcdef0123456789abcdef"
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES (?,1,'happy_path',1,0); UPDATE service_state SET active_run_id=?,generation=1; UPDATE worker_control SET desired_state='RUNNING',run_id=?,generation=1", run, run, run)
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
	if err = storage.AppendEvent(t.Context(), tx, e, "order.created/"+e.OrderID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var first []byte
	send := func(ctx context.Context, r contracts.Record) (int32, int64, error) {
		first = append([]byte{}, r.Value...)
		var n int
		err := db.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE status='PENDING'").Scan(&n)
		if err != nil {
			return 0, 0, err
		}
		if n != 1 {
			t.Fatal("SENT before ack")
		}
		return 0, 0, errors.New("lost ack")
	}
	if _, err = PublishPending(t.Context(), db, run, 1, send); err == nil {
		t.Fatal("lost ack swallowed")
	}
	_, err = db.ExecContext(t.Context(), "UPDATE outbox SET next_retry_at_ms=0")
	if err != nil {
		t.Fatal(err)
	}
	send = func(ctx context.Context, r contracts.Record) (int32, int64, error) {
		if string(first) != string(r.Value) {
			t.Fatal("retry changed payload")
		}
		return 1, 42, nil
	}
	if _, err = PublishPending(t.Context(), db, run, 1, send); err != nil {
		t.Fatal(err)
	}
	var status string
	var offset sql.NullInt64
	if err = db.QueryRowContext(t.Context(), "SELECT status,kafka_offset FROM outbox").Scan(&status, &offset); err != nil {
		t.Fatal(err)
	}
	if status != "SENT" || offset.Int64 != 42 {
		t.Fatalf("status=%s offset=%v", status, offset)
	}
}

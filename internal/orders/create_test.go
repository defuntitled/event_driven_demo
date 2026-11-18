package orders

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestCreateOrderIdempotency(t *testing.T) {
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
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			o, err := CreateOrder(t.Context(), db, c)
			if err != nil {
				t.Error(err)
				return
			}
			if o.OrderID != c.OrderID || o.Status != "PROCESSING" {
				t.Errorf("unexpected order %+v", o)
			}
		})
	}
	wg.Wait()
	var count int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM outbox").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("outbox count=%d", count)
	}
	var raw string
	if err = db.QueryRowContext(t.Context(), "SELECT envelope_json FROM outbox").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var e contracts.Event
	if err = json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if err = e.Validate("orders.events", c.OrderID); err != nil {
		t.Fatal(err)
	}
	c.BuyerID = "different"
	if _, err = CreateOrder(t.Context(), db, c); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	c.Generation = 0
	if _, err = CreateOrder(t.Context(), db, c); err == nil {
		t.Fatal("stale request accepted")
	}
}

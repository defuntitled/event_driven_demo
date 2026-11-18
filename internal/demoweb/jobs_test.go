package demoweb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func purchaseApp(t *testing.T, scenario string) *App {
	t.Helper()
	path := filepath.Join(t.TempDir(), "demo.db")
	db, err := storage.Open(t.Context(), path, "demo-web")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	run := "run_0123456789abcdef0123456789abcdef"
	if _, err = db.ExecContext(t.Context(), "INSERT INTO demo_runs(run_id,generation,scenario,initial_seats,created_at_ms) VALUES (?,1,?,1,0); INSERT INTO demo_state(singleton,active_run_id,state) VALUES (1,?,'READY')", run, scenario, run); err != nil {
		t.Fatal(err)
	}
	return New(db, config.Config{InternalTimeout: time.Second, DatabasePath: path})
}
func TestRaceCreatesPairOnlyOnPurchase(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "race_last_ticket")
	var n int
	if err := a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal(n)
	}
	p := Purchase{RunID: "run_0123456789abcdef0123456789abcdef", ConcertID: contracts.ConcertID, Quantity: 1}
	first, err := a.savePurchase(t.Context(), "key1", p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := a.savePurchase(t.Context(), "key1", p)
	if err != nil {
		t.Fatal(err)
	}
	if first.OrderID != again.OrderID {
		t.Fatal("retry changed primary id")
	}
	if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pair jobs=%d", n)
	}
	if _, err = a.savePurchase(t.Context(), "key2", p); err != nil {
		t.Fatal(err)
	}
	if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("second purchase created extra competitor: %d", n)
	}
}
func TestPartialPairSurvivesRestart(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "race_last_ticket")
	var mu sync.Mutex
	seen := map[string]string{}
	competitorFails := true
	acceptedPrimary := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c orders.CreateCommand
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if previous := seen[c.BuyerID]; previous != "" && previous != c.OrderID {
			t.Error("delivery changed identity")
		}
		seen[c.BuyerID] = c.OrderID
		if c.BuyerID == "demo-competitor" && competitorFails {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":{"code":"RETRY","message":"retry"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"order_id": c.OrderID, "run_id": c.RunID})
		if c.BuyerID == "presenter" {
			select {
			case acceptedPrimary <- struct{}{}:
			default:
			}
		}
	}))
	defer server.Close()
	a.Config.URLs = map[string]string{"orders": server.URL}
	_, err := a.savePurchase(t.Context(), "pair", Purchase{RunID: "run_0123456789abcdef0123456789abcdef", ConcertID: contracts.ConcertID, Quantity: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.Dispatch(ctx); close(done) }()
	select {
	case <-acceptedPrimary:
	case <-time.After(3 * time.Second):
		t.Fatal("primary not delivered")
	}
	cancel()
	<-done
	mu.Lock()
	competitorFails = false
	mu.Unlock()
	if err = a.DB.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.Open(t.Context(), a.Config.DatabasePath, "demo-web")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	a.DB = reopened
	restarted := New(reopened, a.Config)
	ctx2, cancel2 := context.WithCancel(t.Context())
	done2 := make(chan struct{})
	go func() { restarted.Dispatch(ctx2); close(done2) }()
	defer func() { cancel2(); <-done2 }()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs WHERE state='ACCEPTED'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 2 {
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 2 {
				t.Fatal(seen)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pending competitor did not recover")
}

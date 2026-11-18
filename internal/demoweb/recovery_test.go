package demoweb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func TestControllerRestartBetweenInitializeAndStart(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var mu sync.Mutex
	seen := map[string]int{}
	first := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var run contracts.Run
		if err := json.NewDecoder(r.Body).Decode(&run); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if run.Phase == 3 && first {
			first = false
			cancel()
			w.WriteHeader(503)
			return
		}
		seen[run.RunID]++
		w.WriteHeader(200)
	}))
	defer server.Close()
	a.Config.URLs = map[string]string{"orders": server.URL, "payments": server.URL, "reservations": server.URL}
	op, err := a.BeginOperation(t.Context(), "restart-mid-prepare", ControlCommand{Kind: "prepare", Scenario: "happy_path", InitialSeats: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = a.executeOperation(ctx, op); err == nil {
		t.Fatal("injected interruption not observed")
	}
	saved, err := a.Operation(t.Context(), op.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Stage != "START" || saved.State != "RUNNING" {
		t.Fatalf("checkpoint lost: %+v", saved)
	}
	restarted := New(a.DB, a.Config)
	if err = restarted.executeOperation(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Session(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !result.CanPurchase || result.RunID != op.RunID || result.Generation != op.Generation {
		t.Fatal(result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatal("controller created a second run", seen)
	}
}

func TestConcurrentPurchaseRetryAndReset(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	p := Purchase{RunID: "run_0123456789abcdef0123456789abcdef", ConcertID: contracts.ConcertID, Quantity: 1}
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			c, err := a.savePurchase(t.Context(), "same", p)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- c.OrderID
		})
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first != "" && id != first {
			t.Fatal("retry changed id")
		}
		first = id
	}
	reset, err := a.BeginOperation(t.Context(), "reset", ControlCommand{Kind: "reset", Scenario: "happy_path", InitialSeats: 1})
	if err != nil {
		t.Fatal(err)
	}
	if reset.RunID == p.RunID {
		t.Fatal("reset reused run")
	}
	if _, err = a.savePurchase(t.Context(), "late", p); err == nil {
		t.Fatal("purchase admitted after reset began")
	}
	var n int
	if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&n); err != nil || n != 1 {
		t.Fatalf("jobs=%d %v", n, err)
	}
}

func TestResetDrainsInflight(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/orders" {
			close(entered)
			<-release
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	a.Config.URLs = map[string]string{"orders": server.URL, "payments": server.URL, "reservations": server.URL}
	_, err := a.savePurchase(t.Context(), "blocked", Purchase{RunID: "run_0123456789abcdef0123456789abcdef", ConcertID: contracts.ConcertID, Quantity: 1})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := a.pending(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{})
	go func() { a.deliver(t.Context(), jobs[0]); close(delivered) }()
	<-entered
	op, err := a.BeginOperation(t.Context(), "reset", ControlCommand{Kind: "reset", Scenario: "happy_path", InitialSeats: 1})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- a.executeOperation(t.Context(), op) }()
	select {
	case <-finished:
		t.Fatal("reset did not wait for in-flight delivery")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-delivered
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
	var state string
	if err = a.DB.QueryRowContext(t.Context(), "SELECT state FROM purchase_jobs").Scan(&state); err != nil || state != "CANCELED" {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

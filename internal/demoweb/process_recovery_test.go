package demoweb

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/orders"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestControllerProcessHelper(t *testing.T) {
	t.Parallel()
	if os.Getenv("LAST_TICKET_CONTROL_CHILD") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	db, err := storage.Open(ctx, os.Getenv("LAST_TICKET_CONTROL_DB"), "demo-web")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := os.Getenv("LAST_TICKET_CONTROL_URL")
	a := New(db, config.Config{InternalTimeout: time.Second, URLs: map[string]string{"orders": target, "payments": target, "reservations": target}})
	if os.Getenv("LAST_TICKET_CONTROL_MODE") == "dispatch" {
		a.Dispatch(ctx)
		return
	}
	a.ControlLoop(ctx)
}
func TestControllerProcessKillAtStart(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	var blocked atomic.Bool
	blocked.Store(true)
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var run contracts.Run
		if err := json.NewDecoder(r.Body).Decode(&run); err != nil {
			t.Error(err)
			return
		}
		if run.Phase == 3 && blocked.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	op, err := a.BeginOperation(t.Context(), "process-kill", ControlCommand{Kind: "prepare", Scenario: "happy_path", InitialSeats: 1})
	if err != nil {
		t.Fatal(err)
	}
	child := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestControllerProcessHelper$")
		cmd.Env = append(os.Environ(), "LAST_TICKET_CONTROL_CHILD=1", "LAST_TICKET_CONTROL_DB="+a.Config.DatabasePath, "LAST_TICKET_CONTROL_URL="+server.URL)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	first := child()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		_ = first.Process.Kill()
		_ = first.Wait()
		t.Fatal("child did not reach START")
	}
	if err = first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = first.Wait(); err == nil {
		t.Fatal("child did not crash")
	}
	saved, err := a.Operation(t.Context(), op.OperationID)
	if err != nil || saved.Stage != "START" || saved.State != "RUNNING" {
		t.Fatalf("checkpoint: %+v %v", saved, err)
	}
	blocked.Store(false)
	second := child()
	defer func() {
		_ = second.Process.Signal(syscall.SIGTERM)
		if err := second.Wait(); err != nil {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, err := a.Session(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if state.State == "READY" {
			if state.RunID != op.RunID || state.Generation != op.Generation {
				t.Fatal("restart changed run")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("new controller process did not resume saved operation")
}

func TestPairProcessKillAfterFirstAcceptance(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "race_last_ticket")
	_, err := a.savePurchase(t.Context(), "pair-process", Purchase{RunID: "run_0123456789abcdef0123456789abcdef", ConcertID: contracts.ConcertID, Quantity: 1})
	if err != nil {
		t.Fatal(err)
	}
	var blocked atomic.Bool
	blocked.Store(true)
	var mu sync.Mutex
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c orders.CreateCommand
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		previous := seen[c.BuyerID]
		seen[c.BuyerID] = c.OrderID
		mu.Unlock()
		if previous != "" && previous != c.OrderID {
			t.Error("restart changed order id")
		}
		if c.BuyerID == "demo-competitor" && blocked.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"run_id": c.RunID, "order_id": c.OrderID})
	}))
	defer server.Close()
	child := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestControllerProcessHelper$")
		cmd.Env = append(os.Environ(), "LAST_TICKET_CONTROL_CHILD=1", "LAST_TICKET_CONTROL_MODE=dispatch", "LAST_TICKET_CONTROL_DB="+a.Config.DatabasePath, "LAST_TICKET_CONTROL_URL="+server.URL)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	await := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var count int
			if err := a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs WHERE state='ACCEPTED'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count == n {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("expected %d accepted jobs", n)
	}
	first := child()
	defer func() {
		if first.ProcessState == nil {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	}()
	await(1)
	if err = first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = first.Wait(); err == nil {
		t.Fatal("first dispatcher did not crash")
	}
	blocked.Store(false)
	second := child()
	defer func() {
		_ = second.Process.Signal(syscall.SIGTERM)
		if err := second.Wait(); err != nil {
			t.Error(err)
		}
	}()
	await(2)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatal("extra identity", seen)
	}
	var count int
	if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&count); err != nil || count != 2 {
		t.Fatalf("jobs=%d err=%v", count, err)
	}
}

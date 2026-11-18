//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/demoweb"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

var stand sync.Mutex

type harness struct {
	t          *testing.T
	base       string
	client     *http.Client
	run        string
	generation int64
	order      string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	base := os.Getenv("DEMO_BASE_URL")
	if base == "" {
		t.Fatal("DEMO_BASE_URL must identify the isolated test stand")
	}
	h := &harness{t: t, base: base, client: &http.Client{Timeout: 8 * time.Second}}
	t.Cleanup(func() {
		if t.Failed() {
			h.snapshot()
		}
	})
	return h
}
func (h *harness) call(method, path string, body, out any) int {
	h.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.t.Context(), method, h.base+path, bytes.NewReader(raw))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	key, err := contracts.NewID("req")
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", key)
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if resp.StatusCode >= 400 {
		h.t.Fatalf("%s %s status=%d: %s", method, path, resp.StatusCode, data)
	}
	if out != nil {
		if err = json.Unmarshal(data, out); err != nil {
			h.t.Fatalf("decode %s: %v body=%s", path, err, data)
		}
	}
	return resp.StatusCode
}
func (h *harness) await(label string, predicate func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		select {
		case <-h.t.Context().Done():
			h.t.Fatal(h.t.Context().Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	h.t.Fatalf("timeout waiting for %s run=%s order=%s", label, h.run, h.order)
}
func (h *harness) prepare(scenario string) {
	var op demoweb.Operation
	h.call("POST", "/api/presenter/prepare", map[string]any{"scenario": scenario}, &op)
	h.run = op.RunID
	h.generation = op.Generation
	h.await("prepare READY", func() bool {
		h.call("GET", "/api/operations/"+op.OperationID, nil, &op)
		if op.State == "FAILED" {
			h.t.Fatalf("prepare failed: %+v", op)
		}
		return op.State == "SUCCEEDED"
	})
}
func (h *harness) buy() {
	var o orders.Order
	h.call("POST", "/api/orders", map[string]any{"run_id": h.run, "concert_id": "concert-1", "quantity": 1}, &o)
	h.order = o.OrderID
	if h.order == "" {
		h.t.Fatal("no order id")
	}
}
func (h *harness) status(want string) {
	h.await(want, func() bool {
		var o orders.Order
		h.call("GET", "/api/orders/"+h.order+"?run_id="+h.run, nil, &o)
		return o.Status == want
	})
}
func (h *harness) action(action string) {
	var op demoweb.Operation
	h.call("POST", "/api/presenter/actions", map[string]any{"run_id": h.run, "generation": h.generation, "action": action}, &op)
	h.await(action, func() bool {
		h.call("GET", "/api/operations/"+op.OperationID, nil, &op)
		if op.State == "FAILED" {
			h.t.Fatalf("action failed: %+v", op)
		}
		return op.State == "SUCCEEDED"
	})
}
func (h *harness) databases() map[string]demoweb.Component {
	var v struct {
		Components map[string]demoweb.Component `json:"components"`
	}
	h.call("GET", "/api/presenter/databases?run_id="+h.run+"&order_id="+h.order, nil, &v)
	for role, c := range v.Components {
		if !c.OK {
			h.t.Fatalf("source %s: %+v", role, c.Error)
		}
	}
	return v.Components
}

func TestHappy(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("happy_path")
	h.buy()
	h.status("CONFIRMED")
	db := h.databases()
	if len(db["payments"].Data["payment_operations"]) != 1 || len(db["reservations"].Data["reservation_decisions"]) != 1 {
		t.Fatal(db)
	}
	if db["reservations"].Data["inventory"][0]["available"] != float64(0) {
		t.Fatal("seat not consumed")
	}
}
func TestReservationPaused(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("reservation_paused")
	h.buy()
	h.status("PAID_PENDING_RESERVATION")
	db := h.databases()
	if len(db["reservations"].Data["reservation_decisions"]) != 0 {
		t.Fatal("reserved while paused")
	}
	h.journalCaughtUp()
	paid := false
	for _, e := range h.events() {
		if e["event_type"] == "PaymentSucceeded" {
			paid = true
		}
	}
	if !paid {
		t.Fatal("journal did not see payment while reservation paused")
	}
	h.action("resume_reservations")
	h.status("CONFIRMED")
}
func TestPaymentCrash(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("payment_crash")
	h.buy()
	var state control.ServiceState
	h.await("exit73", func() bool {
		var response struct {
			Components map[string]struct {
				Data control.ServiceState `json:"data"`
			} `json:"components"`
		}
		h.call("GET", "/api/presenter/state", nil, &response)
		state = response.Components["payments"].Data
		w := state.Workers["consumer"]
		return w.DesiredState == "FAULTED" && w.ActualState == "EXITED" && w.ExitCode != nil && *w.ExitCode == 73
	})
	oldPID := state.Workers["consumer"].PID
	db := h.databases()
	attempts := db["payments"].Data["processing_attempts"]
	if len(attempts) != 1 || attempts[0]["outcome"] != "PROCESSED" || attempts[0]["commit_status"] != "NOT_ATTEMPTED" {
		t.Fatal(attempts)
	}
	first := attempts[0]
	var before messaging.KafkaSnapshot
	h.await("offset sample after crash", func() bool {
		h.call("GET", "/api/presenter/kafka", nil, &before)
		return before.OK && before.SampledAtMS > int64(first["finished_at_ms"].(float64))
	})
	for _, i := range before.Items {
		if i.Group == "demo.payments" && i.Topic == first["topic"] && float64(i.Partition) == first["partition_id"] && i.CommittedNext != nil && float64(*i.CommittedNext) > first["offset_value"].(float64) {
			t.Fatal("fault advanced committed offset")
		}
	}
	h.action("restore_payments")
	h.status("CONFIRMED")
	if h.service("payments").Workers["consumer"].PID == oldPID {
		t.Fatal("consumer pid did not change")
	}
	h.await("committed offset advanced", func() bool {
		var now messaging.KafkaSnapshot
		h.call("GET", "/api/presenter/kafka", nil, &now)
		for _, i := range now.Items {
			if i.Group == "demo.payments" && i.Topic == first["topic"] && float64(i.Partition) == first["partition_id"] && i.CommittedNext != nil && float64(*i.CommittedNext) > first["offset_value"].(float64) {
				return true
			}
		}
		return false
	})
	h.await("duplicate attempt", func() bool {
		db = h.databases()
		for _, a := range db["payments"].Data["processing_attempts"] {
			if a["outcome"] == "DUPLICATE_EVENT" && a["commit_status"] == "ACKNOWLEDGED" {
				for _, k := range []string{"event_id", "topic", "partition_id", "offset_value"} {
					if a[k] != first[k] {
						t.Fatalf("mismatched %s", k)
					}
				}
				return true
			}
		}
		return false
	})
	if len(db["payments"].Data["payment_operations"]) != 1 || len(db["payments"].Data["outbox"]) != 1 {
		t.Fatal("duplicate effect")
	}
	var events storage.Page
	h.call("GET", "/api/presenter/events?run_id="+h.run+"&order_id="+h.order, nil, &events)
	created := 0
	for _, e := range events.Items {
		if e["event_type"] == "OrderCreated" {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("physical order records=%d", created)
	}
	t.Logf("crash observed old_pid=%d source=%v:%v:%v", oldPID, first["topic"], first["partition_id"], first["offset_value"])
}
func TestOrdersOutboxPaused(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("orders_outbox_paused")
	h.buy()
	db := h.databases()
	if len(db["orders"].Data["outbox"]) != 1 || db["orders"].Data["outbox"][0]["status"] != "PENDING" {
		t.Fatal("missing pending outbox")
	}
	if len(db["payments"].Data["payment_operations"]) != 0 {
		t.Fatal("charged before publication")
	}
	h.journalCaughtUp()
	if len(h.events()) != 0 {
		t.Fatal("outbox published while paused")
	}
	h.action("resume_orders_outbox")
	h.status("CONFIRMED")
}

func (h *harness) snapshot() {
	h.t.Helper()
	data := map[string]any{"run_id": h.run, "order_id": h.order}
	for _, path := range []string{"/api/presenter/state", "/api/presenter/kafka", "/api/presenter/databases?run_id=" + h.run, "/api/presenter/events?run_id=" + h.run} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, "GET", h.base+path, nil)
		if err == nil {
			resp, callErr := h.client.Do(req)
			if callErr == nil {
				raw, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr == nil {
					data[path] = json.RawMessage(raw)
				}
			} else {
				data[path] = callErr.Error()
			}
		}
		cancel()
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		h.t.Log(err)
		return
	}
	if err = os.WriteFile(filepath.Join("..", "..", "artifacts", "go-failure-"+h.t.Name()+".json"), raw, 0600); err != nil {
		h.t.Log(err)
	}
}

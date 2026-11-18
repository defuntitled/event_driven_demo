//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/demoweb"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func (h *harness) request(path, key string, body any) (int, map[string]any) {
	h.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.t.Context(), "POST", h.base+path, bytes.NewReader(raw))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatal(err)
	}
	return resp.StatusCode, out
}
func TestConcurrentHTTPRetry(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("happy_path")
	key, err := contracts.NewID("request")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			status, out := h.request("/api/orders", key, map[string]any{"run_id": h.run, "concert_id": contracts.ConcertID, "quantity": 1})
			if status != 202 {
				t.Errorf("status=%d %+v", status, out)
				return
			}
			ids <- out["order_id"].(string)
		})
	}
	wg.Wait()
	close(ids)
	for id := range ids {
		if h.order != "" && h.order != id {
			t.Fatal("retry changed order")
		}
		h.order = id
	}
	h.status("CONFIRMED")
	db := h.databases()
	if len(db["orders"].Data["orders"]) != 1 || len(db["orders"].Data["outbox"]) != 1 {
		t.Fatal("duplicate order or outbox")
	}
	row := db["orders"].Data["orders"][0]
	c := orders.CreateCommand{RunID: h.run, Generation: h.generation, OrderID: h.order, RequestID: row["request_id"].(string), BuyerID: "conflicting", ConcertID: contracts.ConcertID, Quantity: 1}
	internal := *h
	internal.base = internalURL("orders")
	status, _ := internal.request("/internal/orders", "not-used", c)
	if status != 409 {
		t.Fatalf("conflicting retry status=%d", status)
	}
}
func TestHTTPResetRace(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("happy_path")
	old := h.run
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		status, v := h.request("/api/orders", "purchase-reset-race", map[string]any{"run_id": old, "concert_id": contracts.ConcertID, "quantity": 1})
		if status == 202 && v["run_id"] != old {
			t.Error("purchase attributed to new run")
		}
		if status != 202 && status != 409 && status != 503 {
			t.Errorf("unexpected response %d %v", status, v)
		}
	})
	var op demoweb.Operation
	wg.Go(func() { <-start; h.call("POST", "/api/presenter/reset", map[string]any{}, &op) })
	close(start)
	wg.Wait()
	h.run = op.RunID
	h.generation = op.Generation
	h.order = ""
	h.await("reset race complete", func() bool {
		h.call("GET", "/api/operations/"+op.OperationID, nil, &op)
		return op.State == "SUCCEEDED"
	})
	var rows storage.Page
	h.call("GET", "/api/presenter/orders?run_id="+h.run, nil, &rows)
	if len(rows.Items) != 0 {
		t.Fatal("old purchase leaked into new run")
	}
	db := h.databases()
	if db["reservations"].Data["inventory"][0]["available"] != float64(1) || len(db["payments"].Data["payment_operations"]) != 0 {
		t.Fatal("old effects changed new run")
	}
}

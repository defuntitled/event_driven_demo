//go:build integration

package integration

import (
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestTwoBuyersOneSeat(t *testing.T) {
	t.Parallel()
	stand.Lock()
	defer stand.Unlock()
	h := newHarness(t)
	h.prepare("race_last_ticket")
	var list storage.Page
	h.call("GET", "/api/presenter/orders?run_id="+h.run, nil, &list)
	if len(list.Items) != 0 {
		t.Fatal("orders existed before buyer click")
	}
	h.buy()
	h.await("two final orders", func() bool {
		h.call("GET", "/api/presenter/orders?run_id="+h.run, nil, &list)
		if len(list.Items) != 2 {
			return false
		}
		states := map[string]int{}
		for _, o := range list.Items {
			states[o["status"].(string)]++
		}
		return states["CONFIRMED"] == 1 && states["REFUNDED"] == 1
	})
	h.order = ""
	db := h.databases()
	net := float64(0)
	for _, p := range db["payments"].Data["payment_operations"] {
		amount := p["amount_cents"].(float64)
		if p["kind"] == "REFUND" {
			amount = -amount
		}
		net += amount
	}
	if net != 50000 {
		t.Fatalf("net=%v", net)
	}
	if len(db["reservations"].Data["reservation_decisions"]) != 2 || db["reservations"].Data["inventory"][0]["available"] != float64(0) {
		t.Fatal("incorrect inventory decisions")
	}
}

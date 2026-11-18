package contracts

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeValidation(t *testing.T) {
	t.Parallel()
	valid := Event{SchemaVersion: 1, EventID: "evt_0123456789abcdef0123456789abcdef", RunID: "run_0123456789abcdef0123456789abcdef", OrderID: "ord_0123456789abcdef0123456789abcdef", Type: OrderCreated, Producer: "orders", OccurredAt: time.Now().UTC(), Payload: Payload{ConcertID: ConcertID, BuyerID: "presenter", Quantity: 1, AmountCents: 50000, Currency: "RUB"}}
	for _, tc := range []struct {
		name       string
		mutate     func(*Event)
		topic, key string
		wantError  bool
	}{
		{"valid", func(*Event) {}, "orders.events", valid.OrderID, false},
		{"version", func(e *Event) { e.SchemaVersion = 2 }, "orders.events", valid.OrderID, true},
		{"producer", func(e *Event) { e.Producer = "payments" }, "orders.events", valid.OrderID, true},
		{"amount", func(e *Event) { e.Payload.AmountCents = 1 }, "orders.events", valid.OrderID, true},
		{"quantity", func(e *Event) { e.Payload.Quantity = 2 }, "orders.events", valid.OrderID, true},
		{"currency", func(e *Event) { e.Payload.Currency = "USD" }, "orders.events", valid.OrderID, true},
		{"topic", func(*Event) {}, "payments.events", valid.OrderID, true},
		{"key", func(*Event) {}, "orders.events", "wrong", true},
		{"unknown", func(e *Event) { e.Type = "Unknown" }, "orders.events", valid.OrderID, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := valid
			tc.mutate(&e)
			b, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Decode(b, tc.topic, tc.key)
			if (err != nil) != tc.wantError {
				t.Fatalf("decode error=%v, wantError=%v", err, tc.wantError)
			}
		})
	}
}

func TestNewID(t *testing.T) {
	t.Parallel()
	a, err := NewID("evt")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewID("evt")
	if err != nil {
		t.Fatal(err)
	}
	if a == b || len(a) != 36 || !ValidID(a, "evt") {
		t.Fatalf("invalid distinct IDs: %q %q", a, b)
	}
}

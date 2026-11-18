package contracts

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	ConcertID             = "concert-1"
	AmountCents           = 50000
	OrderCreated          = "OrderCreated"
	PaymentSucceeded      = "PaymentSucceeded"
	PaymentRefunded       = "PaymentRefunded"
	SeatReserved          = "SeatReserved"
	SeatReservationFailed = "SeatReservationFailed"
)

var Services = []string{"orders", "payments", "reservations"}
var Topics = []string{"orders.events", "payments.events", "reservations.events"}

type Payload struct {
	ConcertID         string `json:"concert_id"`
	BuyerID           string `json:"buyer_id,omitempty"`
	Quantity          int    `json:"quantity,omitzero"`
	AmountCents       int    `json:"amount_cents,omitzero"`
	Currency          string `json:"currency,omitempty"`
	PaymentID         string `json:"payment_id,omitempty"`
	OriginalPaymentID string `json:"original_payment_id,omitempty"`
	TicketID          string `json:"ticket_id,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type Event struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	RunID         string    `json:"run_id"`
	OrderID       string    `json:"order_id"`
	Type          string    `json:"type"`
	Producer      string    `json:"producer"`
	OccurredAt    time.Time `json:"occurred_at"`
	CausationID   *string   `json:"causation_id"`
	Payload       Payload   `json:"payload"`
}

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
}

type Run struct {
	OperationID  string `json:"operation_id"`
	RunID        string `json:"run_id"`
	Generation   int64  `json:"generation"`
	Phase        int    `json:"phase"`
	Scenario     string `json:"scenario"`
	InitialSeats int    `json:"initial_seats"`
	CreatedAtMS  int64  `json:"created_at_ms"`
}

func NewID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b[:]), nil
}

func ValidID(id, prefix string) bool {
	v, ok := strings.CutPrefix(id, prefix+"_")
	if !ok || len(v) != 32 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

func Hash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

func Producer(eventType string) string {
	switch eventType {
	case OrderCreated:
		return "orders"
	case PaymentSucceeded, PaymentRefunded:
		return "payments"
	case SeatReserved, SeatReservationFailed:
		return "reservations"
	}
	return ""
}

func Subscriptions(service string) []string {
	switch service {
	case "orders":
		return []string{"payments.events", "reservations.events"}
	case "payments":
		return []string{"orders.events", "reservations.events"}
	case "reservations":
		return []string{"payments.events"}
	}
	return Topics
}

func Handles(service, typ string) bool {
	switch service {
	case "orders":
		return typ != OrderCreated && Producer(typ) != ""
	case "payments":
		return typ == OrderCreated || typ == SeatReservationFailed
	case "reservations":
		return typ == PaymentSucceeded
	}
	return false
}

func NewEvent(typ, run, order string, cause *string, payload Payload) (Event, error) {
	id, err := NewID("evt")
	if err != nil {
		return Event{}, err
	}
	return Event{SchemaVersion: 1, EventID: id, RunID: run, OrderID: order, Type: typ, Producer: Producer(typ), OccurredAt: time.Now().UTC(), CausationID: cause, Payload: payload}, nil
}

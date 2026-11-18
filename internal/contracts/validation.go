package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func Decode(raw []byte, topic, key string) (Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, fmt.Errorf("invalid json: %w", err)
	}
	if err := e.Validate(topic, key); err != nil {
		return e, err
	}
	return e, nil
}

func (e Event) Validate(topic, key string) error {
	if e.SchemaVersion != 1 || Producer(e.Type) == "" {
		return errors.New("unknown schema version or event type")
	}
	if e.Producer != Producer(e.Type) || topic != e.Producer+".events" || key != e.OrderID {
		return errors.New("invalid event route")
	}
	if !ValidID(e.EventID, "evt") || !ValidID(e.RunID, "run") || !ValidID(e.OrderID, "ord") || e.OccurredAt.IsZero() {
		return errors.New("invalid identity or timestamp")
	}
	if e.Type == OrderCreated {
		if e.CausationID != nil || e.Payload.BuyerID == "" || len(e.Payload.BuyerID) > 100 {
			return errors.New("invalid order origin")
		}
	} else if e.CausationID == nil || !ValidID(*e.CausationID, "evt") {
		return errors.New("missing causation id")
	}
	p := e.Payload
	if p.ConcertID != ConcertID {
		return errors.New("invalid concert")
	}
	if e.Type != PaymentRefunded && p.Quantity != 1 {
		return errors.New("invalid quantity")
	}
	if e.Type == OrderCreated || e.Type == PaymentSucceeded || e.Type == PaymentRefunded {
		if p.AmountCents != AmountCents || p.Currency != "RUB" {
			return errors.New("invalid money")
		}
	}
	if e.Type != OrderCreated && !ValidID(p.PaymentID, "pay") {
		return errors.New("invalid payment id")
	}
	if e.Type == SeatReserved && !ValidID(p.TicketID, "tkt") {
		return errors.New("invalid ticket id")
	}
	if e.Type == SeatReservationFailed && p.Reason != "NO_SEATS" {
		return errors.New("invalid rejection reason")
	}
	if e.Type == PaymentRefunded && !ValidID(p.OriginalPaymentID, "pay") {
		return errors.New("invalid original payment id")
	}
	return nil
}

func DecodeCommand(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("expected one json object")
	}
	return nil
}

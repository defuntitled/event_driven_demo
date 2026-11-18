package demoweb

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestJournalPhysicalRecords(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "journal.db"), "demo-web")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	e, err := contracts.NewEvent(contracts.OrderCreated, "run_0123456789abcdef0123456789abcdef", "ord_0123456789abcdef0123456789abcdef", nil, contracts.Payload{ConcertID: contracts.ConcertID, BuyerID: "presenter", Quantity: 1, AmountCents: 50000, Currency: "RUB"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{42, 42, 43} {
		if err = SaveRecord(t.Context(), db, contracts.Record{Topic: "orders.events", Partition: 1, Offset: offset, Key: e.OrderID, Value: raw}); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM kafka_records").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("physical records=%d", n)
	}
	if err = SaveRecord(t.Context(), db, contracts.Record{Topic: "orders.events", Offset: 44, Value: []byte("invalid")}); err != nil {
		t.Fatal(err)
	}
	var parseError string
	if err = db.QueryRowContext(t.Context(), "SELECT parse_error FROM kafka_records WHERE offset_value=44").Scan(&parseError); err != nil {
		t.Fatal(err)
	}
	if parseError == "" {
		t.Fatal("invalid JSON not diagnosed")
	}
}

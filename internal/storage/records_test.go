package storage

import (
	"path/filepath"
	"testing"
)

func TestRecordsAllowlistAndCursor(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "orders.db"), "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = Records(t.Context(), db, "orders", "payment_operations", "run1", "", 0, 100); err == nil {
		t.Fatal("foreign table allowed")
	}
	if _, err = Records(t.Context(), db, "orders", "orders;DROP TABLE orders", "run1", "", 0, 100); err == nil {
		t.Fatal("table injection allowed")
	}
	for _, id := range []string{"1", "2"} {
		_, err = db.ExecContext(t.Context(), "INSERT INTO processing_attempts(worker_instance_id,handler,run_id,topic,partition_id,offset_value,started_at_ms,outcome) VALUES (?,'orders','run1','orders.events',0,0,0,'STARTED')", id)
		if err != nil {
			t.Fatal(err)
		}
	}
	p, err := Records(t.Context(), db, "orders", "processing_attempts", "run1", "", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 || !p.HasMore {
		t.Fatalf("page=%+v", p)
	}
	q, err := Records(t.Context(), db, "orders", "processing_attempts", "run1", "", p.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Items) != 1 || q.HasMore || q.NextCursor <= p.NextCursor {
		t.Fatalf("page=%+v", q)
	}
}

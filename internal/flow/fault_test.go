package flow_test

import (
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func TestCrashArmedOnce(t *testing.T) {
	t.Parallel()
	f := setup(t, 1)
	db := f.db["payments"]
	if _, err := db.ExecContext(t.Context(), "INSERT INTO fault_injections(run_id,name,remaining) VALUES (?,'crash_after_payment_tx',1)", runID); err != nil {
		t.Fatal(err)
	}
	created := event(t, f.db["orders"], contracts.OrderCreated)
	first, err := apply(t, db, "payments", created)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Crash {
		t.Fatal("armed crash did not trigger")
	}
	count(t, db, "SELECT remaining FROM fault_injections", 0)
	count(t, db, "SELECT count(*) FROM payment_operations", 1)
	count(t, db, "SELECT count(*) FROM outbox", 1)
	var state, commit string
	if err = db.QueryRowContext(t.Context(), "SELECT desired_state FROM worker_control WHERE worker='consumer'").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "FAULTED" {
		t.Fatal(state)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT commit_status FROM processing_attempts WHERE attempt_id=?", first.AttemptID).Scan(&commit); err != nil {
		t.Fatal(err)
	}
	if commit != "NOT_ATTEMPTED" {
		t.Fatal(commit)
	}
	if _, err = db.ExecContext(t.Context(), "UPDATE worker_control SET desired_state='RUNNING' WHERE worker='consumer'"); err != nil {
		t.Fatal(err)
	}
	second, err := apply(t, db, "payments", created)
	if err != nil {
		t.Fatal(err)
	}
	if second.Crash || second.Outcome != "DUPLICATE_EVENT" {
		t.Fatalf("result=%+v", second)
	}
	count(t, db, "SELECT count(*) FROM payment_operations", 1)
}

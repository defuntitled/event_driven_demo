package control

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/storage"
	"github.com/defuntitled/event_driven_demo/internal/workers"
)

type immediateWorkers struct{ db *sql.DB }

func (s immediateWorkers) Snapshot(ctx context.Context) (map[string]workers.State, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT worker,desired_state,run_id,generation FROM worker_control")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]workers.State{}
	for rows.Next() {
		var v workers.State
		if err = rows.Scan(&v.Worker, &v.DesiredState, &v.RunID, &v.Generation); err != nil {
			return nil, err
		}
		v.ActualState = "STOPPED"
		if v.DesiredState == "RUNNING" {
			v.ActualState = "RUNNING"
		}
		out[v.Worker] = v
	}
	return out, rows.Err()
}

func TestPhasesRetryAndLateQuiesce(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "reserve.db"), "reservations")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	controller := &ServiceControl{DB: db, Role: "reservations", Workers: immediateWorkers{db}}
	r := contracts.Run{OperationID: "op_0123456789abcdef0123456789abcdef", RunID: "run_0123456789abcdef0123456789abcdef", Generation: 1, Scenario: "happy_path", InitialSeats: 1, CreatedAtMS: 1}
	for phase := 1; phase <= 4; phase++ {
		r.Phase = phase
		if err = controller.Apply(t.Context(), r); err != nil {
			t.Fatalf("phase %d: %v", phase, err)
		}
	}
	_, err = db.ExecContext(t.Context(), "UPDATE inventory SET available=0")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []int{2, 1, 3, 4} {
		r.Phase = phase
		if err = controller.Apply(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	var available, phase int
	var admission, desired string
	if err = db.QueryRowContext(t.Context(), "SELECT available FROM inventory").Scan(&available); err != nil {
		t.Fatal(err)
	}
	if available != 0 {
		t.Fatal("retry replenished inventory")
	}
	if err = db.QueryRowContext(t.Context(), "SELECT control_phase,admission FROM service_state").Scan(&phase, &admission); err != nil {
		t.Fatal(err)
	}
	if phase != 4 || admission != "OPEN" {
		t.Fatalf("phase=%d admission=%s", phase, admission)
	}
	if err = db.QueryRowContext(t.Context(), "SELECT desired_state FROM worker_control WHERE worker='consumer'").Scan(&desired); err != nil {
		t.Fatal(err)
	}
	if desired != "RUNNING" {
		t.Fatal("late quiesce stopped worker")
	}
	r.Scenario = "payment_crash"
	if err = controller.Apply(t.Context(), r); err == nil {
		t.Fatal("same generation parameters changed")
	}
	r.Scenario = "happy_path"
	r.Generation = 0
	if err = controller.Apply(t.Context(), r); err == nil {
		t.Fatal("stale generation accepted")
	}
}

func TestPrepareDoesNotBuy(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "orders.db"), "orders")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	controller := &ServiceControl{DB: db, Role: "orders", Workers: immediateWorkers{db}}
	r := contracts.Run{OperationID: "op_0123456789abcdef0123456789abcdef", RunID: "run_0123456789abcdef0123456789abcdef", Generation: 1, Scenario: "race_last_ticket", InitialSeats: 1, CreatedAtMS: 1}
	for phase := 1; phase <= 4; phase++ {
		r.Phase = phase
		if err = controller.Apply(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("prepare created an order")
	}
}

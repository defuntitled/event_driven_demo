package demoweb

import (
	"path/filepath"
	"testing"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestControlIdempotencyAndAdmission(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "demo.db"), "demo-web")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	app := New(db, config.Config{})
	cmd := ControlCommand{Kind: "prepare", Scenario: "race_last_ticket", InitialSeats: 1}
	first, err := app.BeginOperation(t.Context(), "key1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	again, err := app.BeginOperation(t.Context(), "key1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID != again.OperationID || first.RunID != again.RunID {
		t.Fatal("retry changed identity")
	}
	if _, err = app.BeginOperation(t.Context(), "key2", cmd); err == nil {
		t.Fatal("concurrent control accepted")
	}
	cmd.Scenario = "happy_path"
	if _, err = app.BeginOperation(t.Context(), "key1", cmd); err == nil {
		t.Fatal("conflicting key accepted")
	}
	s, err := app.Session(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.CanPurchase || s.State != "PREPARING" {
		t.Fatal(s)
	}
	var n int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM purchase_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("prepare bought tickets")
	}
	if err = app.failOperation(t.Context(), first.OperationID, "reservations unavailable"); err != nil {
		t.Fatal(err)
	}
	if err = app.RetryOperation(t.Context(), first.OperationID); err != nil {
		t.Fatal(err)
	}
	again, err = app.Operation(t.Context(), first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Generation != first.Generation || again.RunID != first.RunID || again.State != "RUNNING" {
		t.Fatal(again)
	}
}

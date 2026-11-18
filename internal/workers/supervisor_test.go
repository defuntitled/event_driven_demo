package workers

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

func TestWorkerHelper(t *testing.T) {
	t.Parallel()
	if os.Getenv("LAST_TICKET_TEST_CHILD") != "1" {
		return
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"msg": "worker_ready", "run_id": "run1", "generation": 1, "worker_instance_id": os.Getenv("LAST_TICKET_TEST_INSTANCE")}); err != nil {
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}

func TestPauseWaitsForExit(t *testing.T) {
	t.Parallel()
	db, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "test.db"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.ExecContext(t.Context(), "INSERT INTO runs VALUES ('run1',1,'happy_path',1,0); UPDATE service_state SET active_run_id='run1',generation=1; UPDATE worker_control SET desired_state='RUNNING',run_id='run1',generation=1 WHERE worker='consumer'")
	if err != nil {
		t.Fatal(err)
	}
	s := New(db, config.Config{Service: "payments"})
	s.Command = func(mode, run string, gen int64, instance string) (*exec.Cmd, error) {
		cmd := exec.Command(os.Args[0], "-test.run=TestWorkerHelper")
		cmd.Env = append(os.Environ(), "LAST_TICKET_TEST_CHILD=1", "LAST_TICKET_TEST_INSTANCE="+instance)
		return cmd, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	var oldPID int
	for time.Now().Before(deadline) {
		snapshot, err := s.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot["consumer"].ActualState == "RUNNING" {
			oldPID = snapshot["consumer"].PID
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if oldPID == 0 {
		t.Fatal("child not ready")
	}
	if _, err = db.ExecContext(t.Context(), "UPDATE worker_control SET desired_state='PAUSED' WHERE worker='consumer'"); err != nil {
		t.Fatal(err)
	}
	stopped := false
	for time.Now().Before(deadline) {
		snapshot, err := s.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot["consumer"].ActualState == "STOPPED" {
			if snapshot["consumer"].PID != oldPID {
				t.Fatal("lost observed PID")
			}
			stopped = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !stopped {
		t.Fatal("pause did not collect child exit")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("supervisor leaked child")
	}
}

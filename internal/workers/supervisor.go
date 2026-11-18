package workers

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

type State struct {
	Worker       string `json:"worker"`
	DesiredState string `json:"desired_state"`
	ActualState  string `json:"actual_state"`
	RunID        string `json:"run_id"`
	Generation   int64  `json:"generation"`
	PID          int    `json:"pid"`
	InstanceID   string `json:"worker_instance_id"`
	ExitCode     *int   `json:"exit_code"`
	Restarts     int    `json:"restarts"`
	UpdatedAtMS  int64  `json:"updated_at_ms"`
	LastError    string `json:"last_error,omitempty"`
	StopMethod   string `json:"stop_method,omitempty"`
}
type child struct {
	state   State
	cmd     *exec.Cmd
	done    bool
	stopAt  time.Time
	retryAt time.Time
	killed  bool
}
type Supervisor struct {
	DB       *sql.DB
	Config   config.Config
	Command  func(mode, run string, gen int64, instance string) (*exec.Cmd, error)
	mu       sync.Mutex
	children map[string]*child
}

func New(db *sql.DB, c config.Config) *Supervisor {
	s := &Supervisor{DB: db, Config: c, children: make(map[string]*child, 2)}
	s.Command = func(mode, run string, gen int64, instance string) (*exec.Cmd, error) {
		path, err := os.Executable()
		if err != nil {
			return nil, err
		}
		return exec.Command(path, "--service="+c.Service, "--mode="+mode, "--run-id="+run, "--generation="+strconv.FormatInt(gen, 10), "--worker-instance-id="+instance), nil
	}
	return s
}

func (s *Supervisor) desired(ctx context.Context) (map[string]State, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, "SELECT worker,desired_state,run_id,generation,last_error,updated_at_ms FROM worker_control")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]State, 2)
	for rows.Next() {
		var st State
		var run, last sql.NullString
		if err = rows.Scan(&st.Worker, &st.DesiredState, &run, &st.Generation, &last, &st.UpdatedAtMS); err != nil {
			return nil, err
		}
		st.RunID = run.String
		st.LastError = last.String
		st.ActualState = "STOPPED"
		out[st.Worker] = st
	}
	return out, rows.Err()
}

func (s *Supervisor) Snapshot(ctx context.Context) (map[string]State, error) {
	desired, err := s.desired(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for mode, want := range desired {
		if c := s.children[mode]; c != nil {
			st := c.state
			st.DesiredState = want.DesiredState
			if want.LastError != "" {
				st.LastError = want.LastError
			}
			if c.done && (want.DesiredState == "PAUSED" || want.DesiredState == "STOPPED") {
				st.ActualState = "STOPPED"
			}
			desired[mode] = st
		}
	}
	return desired, nil
}

func (s *Supervisor) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			s.shutdown()
			return
		}
		desired, err := s.desired(ctx)
		if err != nil {
			slog.Error("read worker control", "error", err)
		} else {
			s.reconcile(desired)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) reconcile(desired map[string]State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for mode, want := range desired {
		c := s.children[mode]
		if c != nil && !c.done {
			outdated := c.state.RunID != want.RunID || c.state.Generation != want.Generation
			if outdated || want.DesiredState == "PAUSED" || want.DesiredState == "STOPPED" || want.DesiredState == "ERROR" {
				s.stop(c)
			}
			continue
		}
		if want.DesiredState != "RUNNING" || want.RunID == "" || c != nil && time.Now().Before(c.retryAt) {
			continue
		}
		restarts := 0
		if c != nil {
			restarts = c.state.Restarts + 1
		}
		id, err := contracts.NewID("worker")
		if err != nil {
			slog.Error("worker identity", "error", err)
			continue
		}
		want.InstanceID = id
		want.Restarts = restarts
		want.ActualState = "STARTING"
		want.UpdatedAtMS = time.Now().UnixMilli()
		next := &child{state: want}
		s.children[mode] = next
		if err = s.start(next); err != nil {
			next.done = true
			next.state.ActualState = "EXITED"
			next.state.LastError = err.Error()
			next.retryAt = time.Now().Add(time.Second)
		}
	}
}

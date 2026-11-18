package workers

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"syscall"
	"time"
)

func (s *Supervisor) start(c *child) error {
	cmd, err := s.Command(c.state.Worker, c.state.RunID, c.state.Generation, c.state.InstanceID)
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stdout
	if err = cmd.Start(); err != nil {
		stdout.Close()
		return err
	}
	c.cmd = cmd
	c.state.PID = cmd.Process.Pid
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			line := scanner.Bytes()
			if _, err := os.Stdout.Write(append(append([]byte{}, line...), '\n')); err != nil {
				slog.Error("forward worker output", "error", err)
			}
			s.ready(c, line)
		}
		scanErr := scanner.Err()
		waitErr := cmd.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		c.done = true
		c.state.ActualState = "EXITED"
		c.state.UpdatedAtMS = time.Now().UnixMilli()
		c.state.ExitCode = new(cmd.ProcessState.ExitCode())
		c.retryAt = time.Now().Add(time.Second)
		if err := errors.Join(scanErr, waitErr); err != nil {
			c.state.LastError = err.Error()
		}
		slog.Info("worker exited", "service", s.Config.Service, "mode", c.state.Worker, "pid", c.state.PID, "worker_instance_id", c.state.InstanceID, "run_id", c.state.RunID, "generation", c.state.Generation, "exit_code", *c.state.ExitCode)
	}()
	return nil
}

func (s *Supervisor) ready(c *child, line []byte) {
	var msg struct {
		Message    string `json:"msg"`
		RunID      string `json:"run_id"`
		Generation int64  `json:"generation"`
		Instance   string `json:"worker_instance_id"`
	}
	if json.Unmarshal(line, &msg) != nil || msg.Message != "worker_ready" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.done || !c.stopAt.IsZero() || msg.RunID != c.state.RunID || msg.Generation != c.state.Generation || msg.Instance != c.state.InstanceID {
		return
	}
	c.state.ActualState = "RUNNING"
	c.state.UpdatedAtMS = time.Now().UnixMilli()
}

func (s *Supervisor) stop(c *child) {
	if c.done || c.cmd == nil {
		return
	}
	if c.stopAt.IsZero() {
		c.stopAt = time.Now()
		c.state.ActualState = "STOPPING"
		c.state.StopMethod = "SIGTERM"
		if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			c.state.LastError = err.Error()
		}
		return
	}
	if time.Since(c.stopAt) >= 5*time.Second && !c.killed {
		c.killed = true
		c.state.StopMethod = "SIGKILL"
		if err := c.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			c.state.LastError = err.Error()
		}
	}
}

func (s *Supervisor) shutdown() {
	for {
		s.mu.Lock()
		active := false
		for _, c := range s.children {
			if !c.done {
				active = true
				s.stop(c)
			}
		}
		s.mu.Unlock()
		if !active {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

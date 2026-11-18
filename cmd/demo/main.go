package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/demoweb"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/service"
	"github.com/defuntitled/event_driven_demo/internal/storage"
	"github.com/defuntitled/event_driven_demo/internal/workers"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("process failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	role := flag.String("service", "demo-web", "service role")
	mode := flag.String("mode", "server", "server, consumer, publisher or probe")
	runID := flag.String("run-id", "", "worker run")
	generation := flag.Int64("generation", 0, "worker generation")
	instance := flag.String("worker-instance-id", "", "worker instance")
	flag.Parse()
	c, err := config.Load(*role, os.Getenv)
	if err != nil {
		return err
	}
	if *mode == "probe" {
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://127.0.0.1" + c.HTTPAddr + "/readyz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("not ready: %d", resp.StatusCode)
		}
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = os.MkdirAll(filepath.Dir(c.DatabasePath), 0700); err != nil {
		return err
	}
	db, err := storage.Open(ctx, c.DatabasePath, c.Service)
	if err != nil {
		return err
	}
	defer db.Close()
	if *mode != "server" {
		return runWorker(ctx, c, db, *mode, *runID, *generation, *instance)
	}
	var background sync.WaitGroup
	defer func() { stop(); background.Wait() }()
	mux := http.NewServeMux()
	if c.Service == "demo-web" {
		app := demoweb.New(db, c)
		app.Register(mux)
		background.Go(func() { app.ControlLoop(ctx) })
		background.Go(func() { app.Journal(ctx) })
		background.Go(func() { app.Metrics(ctx) })
		background.Go(func() { app.Dispatch(ctx) })
	} else {
		supervisor := workers.New(db, c)
		service.Register(mux, c, db, supervisor)
		background.Go(func() { supervisor.Run(ctx) })
	}
	for _, path := range []string{"GET /healthz", "GET /readyz"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if ctx.Err() != nil {
				w.WriteHeader(503)
			}
			_, _ = w.Write([]byte(`{"ready":true}`))
		})
	}
	srv := http.Server{Addr: c.HTTPAddr, Handler: httpapi.Middleware(mux, c.Service == "demo-web"), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			slog.Error("http shutdown failed", "error", err)
		}
	}()
	slog.Info("server starting", "service", c.Service, "addr", c.HTTPAddr)
	if err = srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWorker(ctx context.Context, c config.Config, db *sql.DB, mode, run string, gen int64, instance string) error {
	if run == "" || gen < 1 || instance == "" {
		return fmt.Errorf("worker requires run, generation and identity")
	}
	ready := func() {
		slog.Info("worker_ready", "service", c.Service, "mode", mode, "run_id", run, "generation", gen, "worker_instance_id", instance)
	}
	switch mode {
	case "consumer":
		local, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := db.ExecContext(local, "UPDATE processing_attempts SET outcome='INTERRUPTED',finished_at_ms=?,error_text='previous consumer exited before transaction completion' WHERE outcome='STARTED'", time.Now().UnixMilli())
		cancel()
		if err != nil {
			return err
		}
		return messaging.Consumer(ctx, c, db, run, gen, instance, ready)
	case "publisher":
		return messaging.Publisher(ctx, c, db, run, gen, ready)
	}
	return fmt.Errorf("unsupported mode %q", mode)
}

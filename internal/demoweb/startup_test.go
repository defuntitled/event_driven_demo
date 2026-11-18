package demoweb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartupRetainsRun(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"run_id":"run_0123456789abcdef0123456789abcdef","generation":1,"admission":"OPEN"}`))
	}))
	defer server.Close()
	a.Config.URLs = map[string]string{"orders": server.URL, "payments": server.URL, "reservations": server.URL}
	if err := a.reconcileStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := a.PublicSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !s.CanPurchase || s.Generation != 1 {
		t.Fatal(s)
	}
	var n int
	if err = a.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM demo_runs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("restart created a new run")
	}
	server.Close()
	s, err = a.PublicSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.CanPurchase {
		t.Fatal("purchase advertised while orders unavailable")
	}
}

func TestStartupRetriesAfterDependencyRecovers(t *testing.T) {
	t.Parallel()
	a := purchaseApp(t, "happy_path")
	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{"run_id":"run_0123456789abcdef0123456789abcdef","generation":1,"admission":"OPEN"}`))
	}))
	defer server.Close()
	a.Config.URLs = map[string]string{"orders": server.URL, "payments": server.URL, "reservations": server.URL}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.ControlLoop(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(21 * time.Second)
	available.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, err := a.Session(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if s.CanPurchase && s.Generation == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("saved run stayed PREPARING after dependency recovered")
}

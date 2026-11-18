package demoweb

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/messaging"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
	"github.com/defuntitled/event_driven_demo/internal/web"
)

type App struct {
	jobsMu       sync.Mutex
	inflight     map[string]bool
	deliveryGate sync.RWMutex
	metricsMu    sync.RWMutex
	metrics      messaging.KafkaSnapshot
	DB           *sql.DB
	Config       config.Config
	Client       *http.Client
}
type Session struct {
	RunID             string  `json:"run_id"`
	Generation        int64   `json:"generation"`
	Scenario          string  `json:"scenario"`
	State             string  `json:"state"`
	CanPurchase       bool    `json:"can_purchase"`
	ActiveOperationID *string `json:"active_operation_id"`
}

func New(db *sql.DB, c config.Config) *App {
	return &App{inflight: make(map[string]bool), DB: db, Config: c, Client: &http.Client{Timeout: c.InternalTimeout}}
}

func (a *App) Session(ctx context.Context) (Session, error) {
	ctx, cancel := context.WithTimeout(ctx, storage.Timeout)
	defer cancel()
	var s Session
	err := a.DB.QueryRowContext(ctx, `SELECT r.run_id,r.generation,r.scenario,s.state,s.active_operation_id FROM demo_state s JOIN demo_runs r ON r.run_id=s.active_run_id WHERE s.singleton=1`).Scan(&s.RunID, &s.Generation, &s.Scenario, &s.State, &s.ActiveOperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{State: "PREPARING"}, nil
	}
	s.CanPurchase = s.State == "READY"
	return s, err
}

func (a *App) Register(mux *http.ServeMux) {
	a.registerDiagnostics(mux)
	a.registerControl(mux)
	mux.Handle("GET /", web.Handler())
	mux.HandleFunc("GET /api/session", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.PublicSession(r.Context())
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, s)
	})
	mux.HandleFunc("POST /api/orders", a.purchase)
	mux.HandleFunc("GET /api/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Session(r.Context())
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		if s.RunID != r.URL.Query().Get("run_id") {
			httpapi.Error(w, contracts.Fail(409, "STALE_RUN", "Демо переключено на другой прогон"))
			return
		}
		var o orders.Order
		err = httpapi.Call(r.Context(), a.Client, "GET", a.Config.URLs["orders"]+"/internal/orders/"+url.PathEscape(r.PathValue("id"))+"?run_id="+url.QueryEscape(s.RunID), nil, &o)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, o)
	})
	mux.HandleFunc("GET /api/concert", a.concert)
}

func (a *App) concert(w http.ResponseWriter, r *http.Request) {
	s, err := a.Session(r.Context())
	if err != nil {
		httpapi.Error(w, err)
		return
	}
	if s.RunID != r.URL.Query().Get("run_id") {
		httpapi.Error(w, contracts.Fail(409, "STALE_RUN", "Демо переключено на другой прогон"))
		return
	}
	var v storage.Page
	err = httpapi.Call(r.Context(), a.Client, "GET", a.Config.URLs["reservations"]+"/internal/records/inventory?run_id="+url.QueryEscape(s.RunID), nil, &v)
	out := map[string]any{"run_id": s.RunID, "concert_id": contracts.ConcertID, "title": "Последний билет", "amount_cents": 50000, "currency": "RUB", "available": nil, "sampled_at_ms": v.SampledAtMS}
	if len(v.Items) > 0 {
		out["available"] = v.Items[0]["available"]
	}
	if err != nil {
		out["available"] = nil
		out["error"] = err.Error()
	}
	httpapi.Write(w, 200, out)
}

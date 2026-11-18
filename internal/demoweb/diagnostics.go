package demoweb

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/storage"
)

type Component struct {
	OK          bool                        `json:"ok"`
	SampledAtMS int64                       `json:"sampled_at_ms"`
	Data        map[string][]map[string]any `json:"data"`
	Error       *contracts.Failure          `json:"error"`
}

func (a *App) Databases(ctx context.Context, run, order string) map[string]Component {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result := make(map[string]Component, 3)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, role := range contracts.Services {
		wg.Go(func() {
			c := Component{OK: true, Data: make(map[string][]map[string]any)}
			for _, table := range storage.Tables(role) {
				items, sampled, err := a.readTable(ctx, role, table, run, order)
				if err != nil {
					c.OK = false
					c.Data = nil
					c.Error = &contracts.Failure{Code: "DEPENDENCY_UNAVAILABLE", Message: err.Error(), Retryable: true}
					break
				}
				c.Data[table] = items
				c.SampledAtMS = sampled
			}
			mu.Lock()
			result[role] = c
			mu.Unlock()
		})
	}
	wg.Wait()
	return result
}

func (a *App) registerDiagnostics(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/presenter/databases", func(w http.ResponseWriter, r *http.Request) {
		if !a.checkRun(w, r) {
			return
		}
		run, order := r.URL.Query().Get("run_id"), r.URL.Query().Get("order_id")
		httpapi.Write(w, 200, map[string]any{"run_id": run, "order_id": order, "components": a.Databases(r.Context(), run, order)})
	})
	mux.HandleFunc("GET /api/presenter/events", func(w http.ResponseWriter, r *http.Request) {
		if !a.checkRun(w, r) {
			return
		}
		after, limit, err := cursor(r)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		p, err := storage.Records(r.Context(), a.DB, "demo-web", "kafka_records", r.URL.Query().Get("run_id"), r.URL.Query().Get("order_id"), after, limit)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, p)
	})
	mux.HandleFunc("GET /api/presenter/orders", func(w http.ResponseWriter, r *http.Request) { a.proxyRecords(w, r, "orders", "orders") })
	mux.HandleFunc("GET /api/presenter/attempts", func(w http.ResponseWriter, r *http.Request) {
		role := r.URL.Query().Get("service")
		if a.Config.URLs[role] == "" {
			httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Неизвестный сервис"))
			return
		}
		a.proxyRecords(w, r, role, "processing_attempts")
	})
	mux.HandleFunc("GET /api/presenter/kafka", func(w http.ResponseWriter, r *http.Request) {
		a.metricsMu.RLock()
		snapshot := a.metrics
		a.metricsMu.RUnlock()
		httpapi.Write(w, 200, snapshot)
	})
}

func (a *App) checkRun(w http.ResponseWriter, r *http.Request) bool {
	s, err := a.Session(r.Context())
	if err != nil {
		httpapi.Error(w, err)
		return false
	}
	if s.RunID == "" || s.RunID != r.URL.Query().Get("run_id") {
		httpapi.Error(w, contracts.Fail(409, "STALE_RUN", "Демо переключено на другой прогон"))
		return false
	}
	return true
}
func cursor(r *http.Request) (int64, int, error) {
	after := int64(0)
	limit := 100
	var err error
	if raw := r.URL.Query().Get("after"); raw != "" {
		after, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, 0, contracts.Fail(400, "INVALID_REQUEST", "Некорректный курсор")
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, contracts.Fail(400, "INVALID_REQUEST", "Некорректный лимит")
		}
	}
	if after < 0 || limit < 1 || limit > 200 {
		return 0, 0, contracts.Fail(400, "INVALID_REQUEST", "Некорректные параметры страницы")
	}
	return after, limit, nil
}
func (a *App) proxyRecords(w http.ResponseWriter, r *http.Request, role, table string) {
	if !a.checkRun(w, r) {
		return
	}
	if _, _, err := cursor(r); err != nil {
		httpapi.Error(w, err)
		return
	}
	var p storage.Page
	err := httpapi.Call(r.Context(), a.Client, "GET", a.Config.URLs[role]+"/internal/records/"+table+"?"+r.URL.RawQuery, nil, &p)
	if err != nil {
		httpapi.Error(w, err)
		return
	}
	httpapi.Write(w, 200, p)
}

func (a *App) readTable(ctx context.Context, role, table, run, order string) ([]map[string]any, int64, error) {
	items := make([]map[string]any, 0)
	after := int64(0)
	for {
		var p storage.Page
		path := a.Config.URLs[role] + "/internal/records/" + table + "?run_id=" + url.QueryEscape(run) + "&order_id=" + url.QueryEscape(order) + "&limit=200&after=" + strconv.FormatInt(after, 10)
		if err := httpapi.Call(ctx, a.Client, "GET", path, nil, &p); err != nil {
			return nil, 0, err
		}
		items = append(items, p.Items...)
		if !p.HasMore {
			return items, p.SampledAtMS, nil
		}
		if p.NextCursor <= after {
			return nil, 0, fmt.Errorf("non-advancing records cursor from %s", role)
		}
		after = p.NextCursor
	}
}

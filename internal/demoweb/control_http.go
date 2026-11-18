package demoweb

import (
	"net/http"
	"sync"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
)

func (a *App) registerControl(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/presenter/prepare", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Scenario string `json:"scenario"`
		}
		if err := httpapi.Read(w, r, &body); err != nil {
			httpapi.Error(w, err)
			return
		}
		a.beginHTTP(w, r, ControlCommand{Kind: "prepare", Scenario: body.Scenario, InitialSeats: 1})
	})
	mux.HandleFunc("POST /api/presenter/reset", func(w http.ResponseWriter, r *http.Request) {
		var body struct{}
		if err := httpapi.Read(w, r, &body); err != nil {
			httpapi.Error(w, err)
			return
		}
		a.beginHTTP(w, r, ControlCommand{Kind: "reset", Scenario: "happy_path", InitialSeats: 1})
	})
	mux.HandleFunc("POST /api/presenter/actions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RunID      string `json:"run_id"`
			Generation int64  `json:"generation"`
			Action     string `json:"action"`
		}
		if err := httpapi.Read(w, r, &body); err != nil {
			httpapi.Error(w, err)
			return
		}
		a.beginHTTP(w, r, ControlCommand{Kind: body.Action, RunID: body.RunID, Generation: body.Generation})
	})
	mux.HandleFunc("GET /api/operations/{id}", func(w http.ResponseWriter, r *http.Request) {
		op, err := a.Operation(r.Context(), r.PathValue("id"))
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, op)
	})
	mux.HandleFunc("POST /api/operations/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		var body struct{}
		if err := httpapi.Read(w, r, &body); err != nil {
			httpapi.Error(w, err)
			return
		}
		if r.Header.Get("Idempotency-Key") == "" {
			httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Требуется Idempotency-Key"))
			return
		}
		if err := a.RetryOperation(r.Context(), r.PathValue("id")); err != nil {
			httpapi.Error(w, err)
			return
		}
		op, err := a.Operation(r.Context(), r.PathValue("id"))
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 202, op)
	})
	mux.HandleFunc("GET /api/presenter/state", a.presenterState)
}
func (a *App) beginHTTP(w http.ResponseWriter, r *http.Request, c ControlCommand) {
	op, err := a.BeginOperation(r.Context(), r.Header.Get("Idempotency-Key"), c)
	if err != nil {
		httpapi.Error(w, err)
		return
	}
	httpapi.Write(w, 202, op)
}
func (a *App) presenterState(w http.ResponseWriter, r *http.Request) {
	session, err := a.Session(r.Context())
	if err != nil {
		httpapi.Error(w, err)
		return
	}
	components := make(map[string]any, 3)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, role := range contracts.Services {
		wg.Go(func() {
			var state control.ServiceState
			err := httpapi.Call(r.Context(), a.Client, "GET", a.Config.URLs[role]+"/internal/state", nil, &state)
			value := map[string]any{"ok": err == nil, "data": state, "error": nil}
			if err != nil {
				value["data"] = nil
				value["error"] = err.Error()
			}
			mu.Lock()
			components[role] = value
			mu.Unlock()
		})
	}
	wg.Wait()
	var operation *Operation
	if session.ActiveOperationID != nil {
		op, err := a.Operation(r.Context(), *session.ActiveOperationID)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		operation = &op
	}
	httpapi.Write(w, 200, map[string]any{"session": session, "components": components, "operation": operation})
}

package service

import (
	"net/http"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/workers"
)

func registerWorkers(mux *http.ServeMux, c *control.ServiceControl, s *workers.Supervisor) {
	mux.HandleFunc("PUT /internal/workers/{worker}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RunID      string `json:"run_id"`
			Generation int64  `json:"generation"`
			Desired    string `json:"desired_state"`
		}
		if err := httpapi.Read(w, r, &body); err != nil {
			httpapi.Error(w, err)
			return
		}
		if body.Desired != "RUNNING" {
			httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Допустимо только возобновление"))
			return
		}
		if err := c.Resume(r.Context(), body.RunID, body.Generation, r.PathValue("worker")); err != nil {
			httpapi.Error(w, err)
			return
		}
		states, err := s.Snapshot(r.Context())
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, states[r.PathValue("worker")])
	})
}

package service

import (
	"database/sql"
	"net/http"
	"strconv"

	"github.com/defuntitled/event_driven_demo/internal/config"
	"github.com/defuntitled/event_driven_demo/internal/contracts"
	"github.com/defuntitled/event_driven_demo/internal/control"
	"github.com/defuntitled/event_driven_demo/internal/httpapi"
	"github.com/defuntitled/event_driven_demo/internal/orders"
	"github.com/defuntitled/event_driven_demo/internal/storage"
	"github.com/defuntitled/event_driven_demo/internal/workers"
)

func Register(mux *http.ServeMux, c config.Config, db *sql.DB, supervisor *workers.Supervisor) {
	controller := &control.ServiceControl{DB: db, Role: c.Service, Workers: supervisor}
	registerWorkers(mux, controller, supervisor)
	mux.HandleFunc("GET /internal/records/{table}", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		var err error
		if raw := r.URL.Query().Get("limit"); raw != "" {
			limit, err = strconv.Atoi(raw)
			if err != nil {
				httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Некорректный лимит"))
				return
			}
		}
		after := int64(0)
		if raw := r.URL.Query().Get("after"); raw != "" {
			after, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				httpapi.Error(w, contracts.Fail(400, "INVALID_REQUEST", "Некорректный курсор"))
				return
			}
		}
		page, err := storage.Records(r.Context(), db, c.Service, r.PathValue("table"), r.URL.Query().Get("run_id"), r.URL.Query().Get("order_id"), after, limit)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, page)
	})
	mux.HandleFunc("PUT /internal/run-control", func(w http.ResponseWriter, r *http.Request) {
		var v contracts.Run
		if err := httpapi.Read(w, r, &v); err != nil {
			httpapi.Error(w, err)
			return
		}
		if err := controller.Apply(r.Context(), v); err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, v)
	})
	mux.HandleFunc("GET /internal/state", func(w http.ResponseWriter, r *http.Request) {
		state, err := control.ReadState(r.Context(), db, supervisor)
		if err != nil {
			httpapi.Error(w, err)
			return
		}
		httpapi.Write(w, 200, state)
	})

	if c.Service == "orders" {
		mux.HandleFunc("POST /internal/orders", func(w http.ResponseWriter, r *http.Request) {
			var v orders.CreateCommand
			if err := httpapi.Read(w, r, &v); err != nil {
				httpapi.Error(w, err)
				return
			}
			o, err := orders.CreateOrder(r.Context(), db, v)
			if err != nil {
				httpapi.Error(w, err)
				return
			}
			httpapi.Write(w, 202, o)
		})
		mux.HandleFunc("GET /internal/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
			o, err := orders.GetOrder(r.Context(), db, r.URL.Query().Get("run_id"), r.PathValue("id"))
			if err != nil {
				httpapi.Error(w, err)
				return
			}
			httpapi.Write(w, 200, o)
		})
	}

}

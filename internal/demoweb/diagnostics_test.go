package demoweb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/config"
)

func TestDiagnosticsSourceError(t *testing.T) {
	t.Parallel()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"amount_cents":50000}],"sampled_at_ms":123}`))
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":{"code":"UNAVAILABLE","message":"source unavailable"}}`))
	}))
	defer bad.Close()
	app := New(nil, config.Config{InternalTimeout: time.Second, URLs: map[string]string{"orders": good.URL, "payments": bad.URL, "reservations": good.URL}})
	components := app.Databases(t.Context(), "run1", "order1")
	if components["payments"].OK || components["payments"].Data != nil || components["payments"].Error == nil {
		t.Fatalf("error replaced with data: %+v", components["payments"])
	}
	if !components["orders"].OK || components["orders"].SampledAtMS != 123 {
		t.Fatal(components["orders"])
	}
}

func TestDiagnosticsReadsAllPages(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "200" {
			_, _ = w.Write([]byte(`{"items":[{"row":201}],"has_more":false,"next_cursor":201}`))
			return
		}
		rows := make([]map[string]int, 200)
		for i := range rows {
			rows[i] = map[string]int{"row": i + 1}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": rows, "has_more": true, "next_cursor": 200})
	}))
	defer server.Close()
	a := New(nil, config.Config{InternalTimeout: time.Second, URLs: map[string]string{"orders": server.URL, "payments": server.URL, "reservations": server.URL}})
	result := a.Databases(t.Context(), "run", "order")
	if len(result["payments"].Data["payment_operations"]) != 201 {
		t.Fatal("diagnostics truncated at page boundary")
	}
}

package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestForwardHeadersAndOrigin(t *testing.T) {
	t.Parallel()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-o3-trace-id") != "trace" || r.Header.Get("X-O3-MeshVersion") != "mesh" || r.Header.Get("x-o3-s2s") != "" {
			t.Errorf("incorrect forwarded headers: %v", r.Header)
		}
		w.WriteHeader(200)
	}))
	defer target.Close()
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Call(r.Context(), &http.Client{Timeout: time.Second}, "GET", target.URL, nil, nil); err != nil {
			t.Error(err)
		}
	}), true)
	r := httptest.NewRequest("GET", "http://localhost:8080/api/session", nil)
	r.Header.Set("x-o3-trace-id", "trace")
	r.Header.Set("X-O3-MeshVersion", "mesh")
	r.Header.Set("x-o3-s2s", "do-not-forward")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, origin := range []string{"http://evil.example", "null"} {
		r.Header.Set("Origin", origin)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("cross origin accepted")
		}
	}
	r.Header.Del("Origin")
	r.Host = "evil.example"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("non-loopback host accepted")
	}
}

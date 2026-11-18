package httpapi

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

type headersKey struct{}

func ForwardHeaders(ctx context.Context) http.Header {
	if headers, ok := ctx.Value(headersKey{}).(http.Header); ok {
		return headers.Clone()
	}
	return make(http.Header)
}

func WithHeaders(ctx context.Context, incoming http.Header) context.Context {
	forward := make(http.Header)
	for name, values := range incoming {
		key := strings.ToLower(name)
		if strings.HasPrefix(key, "x-o3-") && key != "x-o3-app-name" && key != "x-o3-app-version" && key != "x-o3-s2s" {
			forward[name] = append([]string(nil), values...)
		}
	}
	return context.WithValue(ctx, headersKey{}, forward)
}

func Middleware(next http.Handler, loopback bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if loopback && !localRequest(r) {
			Error(w, contracts.Fail(403, "FORBIDDEN_ORIGIN", "Демо доступно только с локального адреса"))
			return
		}
		next.ServeHTTP(w, r.WithContext(WithHeaders(r.Context(), r.Header)))
	})
}

func localRequest(r *http.Request) bool {
	host := r.Host
	if value, _, err := net.SplitHostPort(host); err == nil {
		host = value
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Scheme != "http" || parsed.Host != r.Host {
			return false
		}
	}
	return true
}

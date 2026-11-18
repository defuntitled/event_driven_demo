package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/defuntitled/event_driven_demo/internal/contracts"
)

func Write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write response", "error", err)
	}
}
func Error(w http.ResponseWriter, err error) {
	f, ok := errors.AsType[*contracts.Failure](err)
	if !ok {
		slog.Error("request failed", "error", err)
		f = &contracts.Failure{Status: 500, Code: "INTERNAL_ERROR", Message: "Внутренняя ошибка сервиса", Retryable: true}
	}
	Write(w, f.Status, map[string]any{"error": f})
}
func Read(w http.ResponseWriter, r *http.Request, value any) error {
	if r.Header.Get("Content-Type") != "application/json" {
		return contracts.Fail(400, "INVALID_REQUEST", "Требуется Content-Type: application/json")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		return contracts.Fail(400, "INVALID_REQUEST", "Тело запроса слишком большое")
	}
	if err = contracts.DecodeCommand(body, value); err != nil {
		return contracts.Fail(400, "INVALID_REQUEST", "Некорректный JSON: "+err.Error())
	}
	return nil
}

func Call(ctx context.Context, client *http.Client, method, url string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header = ForwardHeaders(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-o3-app-name", "demo-web")
	resp, err := client.Do(req)
	if err != nil {
		return contracts.Fail(503, "DEPENDENCY_UNAVAILABLE", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var v struct {
			Error contracts.Failure `json:"error"`
		}
		if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&v); err != nil {
			return contracts.Fail(503, "DEPENDENCY_UNAVAILABLE", "Некорректный ответ сервиса")
		}
		v.Error.Status = resp.StatusCode
		return &v.Error
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
}

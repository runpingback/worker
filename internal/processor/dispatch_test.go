package processor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSignPayload(t *testing.T) {
	body := `{"function":"test"}`
	timestamp := "1700000000"
	secret := "mysecret"

	sig := signPayload(timestamp, body, secret)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + body))
	expected := hex.EncodeToString(mac.Sum(nil))

	if sig != expected {
		t.Errorf("signPayload = %q, want %q", sig, expected)
	}
}

func TestDispatch_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing Content-Type header")
		}
		if r.Header.Get("X-Pingback-Signature") == "" {
			t.Error("missing signature header")
		}
		if r.Header.Get("X-Pingback-Timestamp") == "" {
			t.Error("missing timestamp header")
		}

		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		json.Unmarshal(body, &payload)
		if payload["function"] != "send-emails" {
			t.Errorf("expected function send-emails, got %v", payload["function"])
		}

		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{
			"logs":  []map[string]any{{"timestamp": 123, "level": "info", "message": "done"}},
			"tasks": []map[string]any{{"name": "child", "payload": map[string]any{"id": "1"}}},
		})
	}))
	defer server.Close()

	d := &Dispatcher{Client: server.Client()}
	result, err := d.Dispatch(context.Background(), DispatchRequest{
		EndpointURL:    server.URL,
		CronSecret:     "secret123",
		FunctionName:   "send-emails",
		ExecutionID:    "exec-1",
		Attempt:        1,
		ScheduledAt:    "2026-04-25T12:00:00.000Z",
		TimeoutSeconds: 30,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.HttpStatus != 200 {
		t.Errorf("expected status 200, got %d", result.HttpStatus)
	}
	if len(result.Logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(result.Logs))
	}
	if len(result.Tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(result.Tasks))
	}
}

func TestDispatch_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"error":"internal"}`))
	}))
	defer server.Close()

	d := &Dispatcher{Client: server.Client()}
	result, err := d.Dispatch(context.Background(), DispatchRequest{
		EndpointURL:    server.URL,
		CronSecret:     "secret123",
		FunctionName:   "test",
		ExecutionID:    "exec-1",
		Attempt:        1,
		ScheduledAt:    "2026-04-25T12:00:00.000Z",
		TimeoutSeconds: 30,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Success {
		t.Error("expected failure")
	}
	if result.HttpStatus != 500 {
		t.Errorf("expected status 500, got %d", result.HttpStatus)
	}
}

func TestDispatch_Timeout(t *testing.T) {
	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-serverCtx.Done():
		}
	}))
	defer func() {
		serverCancel()
		server.Close()
	}()

	d := &Dispatcher{Client: server.Client()}
	_, err := d.Dispatch(context.Background(), DispatchRequest{
		EndpointURL:    server.URL,
		CronSecret:     "secret123",
		FunctionName:   "test",
		ExecutionID:    "exec-1",
		Attempt:        1,
		ScheduledAt:    "2026-04-25T12:00:00.000Z",
		TimeoutSeconds: 1,
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

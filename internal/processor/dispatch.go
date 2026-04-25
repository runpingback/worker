package processor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/runpingback/worker/internal/db"
	"github.com/runpingback/worker/internal/limits"
)

type Dispatcher struct {
	Client *http.Client
}

type DispatchRequest struct {
	EndpointURL    string
	CronSecret     string
	FunctionName   string
	ExecutionID    string
	Attempt        int
	ScheduledAt    string
	TimeoutSeconds int
	Payload        json.RawMessage
}

type DispatchResult struct {
	Success      bool
	HttpStatus   int
	ResponseBody string
	Logs         []db.LogEntry
	Tasks        []limits.Task
	ErrorMessage string
}

func signPayload(timestamp, body, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + body))
	return hex.EncodeToString(mac.Sum(nil))
}

func (d *Dispatcher) Dispatch(ctx context.Context, req DispatchRequest) (*DispatchResult, error) {
	bodyMap := map[string]any{
		"function":    req.FunctionName,
		"executionId": req.ExecutionID,
		"attempt":     req.Attempt,
		"scheduledAt": req.ScheduledAt,
	}
	if req.Payload != nil {
		bodyMap["payload"] = json.RawMessage(req.Payload)
	}

	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}
	body := string(bodyBytes)

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	signature := signPayload(timestamp, body, req.CronSecret)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.EndpointURL, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Pingback-Signature", signature)
	httpReq.Header.Set("X-Pingback-Timestamp", timestamp)

	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http dispatch: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	respText := string(respBody)

	result := &DispatchResult{
		Success:      resp.StatusCode >= 200 && resp.StatusCode < 300,
		HttpStatus:   resp.StatusCode,
		ResponseBody: respText,
	}

	var parsed struct {
		Logs  []db.LogEntry `json:"logs"`
		Tasks []limits.Task `json:"tasks"`
	}
	if json.Unmarshal(respBody, &parsed) == nil {
		result.Logs = parsed.Logs
		result.Tasks = parsed.Tasks
	}

	if !result.Success {
		result.ErrorMessage = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}

	return result, nil
}

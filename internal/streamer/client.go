package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

const (
	max429Retries  = 3 // retries after the first 429; total = 4 attempts
	max5xxAttempts = 5 // total attempts including first; backoff 1s,2s,4s,8s
)

// MQClient is an HTTP client for the custom message queue service.
type MQClient struct {
	baseURL    string
	httpClient *http.Client
	sleepFn    func(context.Context, time.Duration) error // injectable for testing
}

// NewMQClient creates a new MQClient targeting baseURL.
// Each outbound request carries a 10 s hard timeout.
func NewMQClient(baseURL string) *MQClient {
	return &MQClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		sleepFn:    ctxSleep,
	}
}

// ctxSleep is the default sleep implementation that respects context cancellation.
func ctxSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishBatch serialises batch as JSON and POSTs it to {baseURL}/publish.
//
// Retry policy:
//   - HTTP 429: retry up to 3 times, honouring the Retry-After response header.
//   - HTTP 5xx: exponential backoff (1s, 2s, 4s, 8s), up to 5 total attempts.
//   - Any other non-2xx: return error immediately.
func (c *MQClient) PublishBatch(ctx context.Context, batch model.PublishBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("streamer: marshal batch: %w", err)
	}

	var (
		retries429 int
		attempt5xx int
	)

	for {
		resp, err := c.do(ctx, data)
		if err != nil {
			return fmt.Errorf("streamer: %w", err)
		}

		code := resp.StatusCode

		// Success
		if code >= 200 && code < 300 {
			return nil
		}

		// 429 – Rate limited: honour Retry-After header
		if code == http.StatusTooManyRequests && retries429 < max429Retries {
			retries429++
			delay := parseRetryAfter(resp.Header.Get("Retry-After"))
			if err := c.sleepFn(ctx, delay); err != nil {
				return fmt.Errorf("streamer: %w", err)
			}
			continue
		}

		// 5xx – Server error: exponential backoff
		if code >= 500 && attempt5xx < max5xxAttempts-1 {
			attempt5xx++
			backoff := time.Duration(1<<uint(attempt5xx-1)) * time.Second
			if err := c.sleepFn(ctx, backoff); err != nil {
				return fmt.Errorf("streamer: %w", err)
			}
			continue
		}

		return fmt.Errorf("streamer: server returned %d", code)
	}
}

// do executes a single POST /publish request and returns the response.
// The response body is fully drained and closed before returning.
func (c *MQClient) do(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/publish", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp, nil
}

// parseRetryAfter parses the Retry-After header as whole seconds.
// Falls back to 1 second on parse failure.
func parseRetryAfter(h string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(h)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Second
}

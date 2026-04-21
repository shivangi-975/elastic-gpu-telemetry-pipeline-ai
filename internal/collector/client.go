package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

const (
	consumeTimeout = 30 * time.Second
	ackTimeout     = 10 * time.Second
	maxRetries     = 3
)

// Client is a typed HTTP client for the MQ server API.
// All methods retry on HTTP 5xx responses using exponential backoff.
type Client struct {
	baseURL    string
	httpClient *http.Client
	// RetryWaits controls the pause before each successive retry attempt.
	// Default is [1s, 2s, 4s]. Override to zero durations in tests.
	RetryWaits []time.Duration
}

// NewClient returns a Client that targets baseURL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
		RetryWaits: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second},
	}
}

// Consume fetches up to max messages for the given consumer group and consumer
// ID. It retries up to 3 times on HTTP 5xx with exponential backoff.
func (c *Client) Consume(ctx context.Context, group, consumerID string, max int) (model.ConsumeResponse, error) {
	url := fmt.Sprintf("%s/consume?group=%s&consumer_id=%s&max=%d",
		c.baseURL, group, consumerID, max)

	var resp model.ConsumeResponse
	err := c.doWithRetry(ctx, consumeTimeout, func(reqCtx context.Context) (*http.Response, error) {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		return c.httpClient.Do(req)
	}, &resp)
	if err != nil {
		return model.ConsumeResponse{}, fmt.Errorf("collector: consume: %w", err)
	}
	return resp, nil
}

// Ack acknowledges a processed batch offset with the MQ server.
// It retries up to 3 times on HTTP 5xx with exponential backoff.
func (c *Client) Ack(ctx context.Context, req model.AckRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("collector: ack: marshal: %w", err)
	}

	err = c.doWithRetry(ctx, ackTimeout, func(reqCtx context.Context) (*http.Response, error) {
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
			c.baseURL+"/ack", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		return c.httpClient.Do(httpReq)
	}, nil)
	if err != nil {
		return fmt.Errorf("collector: ack: %w", err)
	}
	return nil
}

// doWithRetry executes fn, retrying on HTTP 5xx up to maxRetries times.
// Each attempt uses a child context with the given per-request timeout.
// If out is non-nil the response body is JSON-decoded into it on success.
func (c *Client) doWithRetry(
	ctx context.Context,
	timeout time.Duration,
	fn func(reqCtx context.Context) (*http.Response, error),
	out any,
) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			wait := c.retryWait(attempt - 1)
			slog.Info("retrying MQ request",
				"attempt", attempt,
				"wait", wait.String())
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(wait):
			}
		}

		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := fn(reqCtx)
		cancel()

		if err != nil {
			lastErr = err
			slog.Warn("MQ request error", "attempt", attempt+1, "error", err)
			continue
		}

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("http status %d", resp.StatusCode)
			slog.Warn("MQ returned 5xx", "attempt", attempt+1, "status", resp.StatusCode)
			continue
		}

		// Success path — decode body and close.
		var decErr error
		if out != nil {
			decErr = json.NewDecoder(resp.Body).Decode(out)
		}
		resp.Body.Close()
		if decErr != nil {
			return fmt.Errorf("decode response: %w", decErr)
		}
		return nil
	}

	return lastErr
}

// retryWait returns the configured wait duration for the i-th retry.
func (c *Client) retryWait(i int) time.Duration {
	if i < len(c.RetryWaits) {
		return c.RetryWaits[i]
	}
	return c.RetryWaits[len(c.RetryWaits)-1]
}

package insight

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultHTTPExporterTimeout is the default request timeout for
// HTTPExporter when no custom client or timeout is specified.
const DefaultHTTPExporterTimeout = 10 * time.Second

// HTTPDoer is the interface for sending HTTP requests. *http.Client
// satisfies this interface.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPExporterConfig configures an HTTPExporter.
type HTTPExporterConfig struct {
	// URL is the endpoint to POST records to. Required.
	URL string

	// Client is the HTTP client used for requests. If nil, a default
	// client with Timeout is used.
	Client HTTPDoer

	// Headers are added to every request (e.g. Authorization).
	Headers map[string]string

	// Timeout bounds a single HTTP request. Defaults to
	// DefaultHTTPExporterTimeout when zero and Client is nil. Ignored
	// when Client is provided (the caller's client owns its timeout).
	Timeout time.Duration

	// MaxRetries is the maximum number of retries on 5xx responses.
	// Defaults to 1 when zero.
	MaxRetries int

	// OperationsFormat controls how operations are rendered in the JSON
	// body. Defaults to OperationsFormatArray when empty.
	OperationsFormat OperationsFormat
}

// HTTPExporter POSTs JSON-serialized records to a configurable URL. It
// implements Exporter and requires no AWS dependencies — only the
// standard library net/http.
type HTTPExporter struct {
	url              string
	client           HTTPDoer
	headers          map[string]string
	maxRetries       int
	operationsFormat OperationsFormat
}

// NewHTTPExporter returns an HTTPExporter configured by cfg. Panics if
// cfg.URL is empty.
func NewHTTPExporter(cfg HTTPExporterConfig) *HTTPExporter {
	if cfg.URL == "" {
		panic("insight.NewHTTPExporter: URL is required")
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = DefaultHTTPExporterTimeout
		}
		client = &http.Client{Timeout: timeout}
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	return &HTTPExporter{
		url:              cfg.URL,
		client:           client,
		headers:          cfg.Headers,
		maxRetries:       maxRetries,
		operationsFormat: cfg.OperationsFormat,
	}
}

// Export sends record as JSON to the configured URL via HTTP POST. On
// 5xx responses it retries up to MaxRetries times with a simple
// exponential backoff (100ms, 200ms, ...).
func (e *HTTPExporter) Export(ctx context.Context, record Record) error {
	body, err := RenderRecord(record, e.operationsFormat)
	if err != nil {
		return fmt.Errorf("insight.HTTPExporter: rendering record: %w", err)
	}

	var lastErr error
	attempts := 1 + e.maxRetries
	for i := range attempts {
		if err := e.doRequest(ctx, body); err != nil {
			lastErr = err
			// Only retry on server errors (5xx).
			if !isServerError(err) {
				return err
			}
			if i < attempts-1 {
				backoff := time.Duration(100*(i+1)) * time.Millisecond
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}

// Close is a no-op — HTTPExporter holds no persistent connections that
// require cleanup.
func (e *HTTPExporter) Close() error {
	return nil
}

// doRequest performs a single HTTP POST and returns an error on non-2xx
// status.
func (e *HTTPExporter) doRequest(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("insight.HTTPExporter: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("insight.HTTPExporter: POST %s: %w", e.url, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return &httpExporterError{url: e.url, statusCode: resp.StatusCode}
	}
	return nil
}

// httpExporterError represents a non-success HTTP response.
type httpExporterError struct {
	url        string
	statusCode int
}

func (e *httpExporterError) Error() string {
	return fmt.Sprintf("insight.HTTPExporter: POST %s returned status %d", e.url, e.statusCode)
}

// isServerError reports whether err is a 5xx httpExporterError.
func isServerError(err error) bool {
	he, ok := err.(*httpExporterError)
	return ok && he.statusCode >= 500
}

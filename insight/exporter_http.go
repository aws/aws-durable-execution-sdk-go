package insight

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

// DefaultHttpExporterTimeout is HttpExporter's own default request
// timeout, matching the JS SDK's own documented default (10000ms).
const DefaultHttpExporterTimeout = 10 * time.Second

// HttpExporter is a generic HTTP/webhook exporter: POSTs (or PUTs) the
// full rendered record as JSON to any URL, matching the JS SDK's own
// HttpExporter - the right choice for custom backends, internal
// microservices, SaaS integrations without a dedicated exporter, or
// prototyping, distinct from OTelExporter's own OTLP-specific envelope
// (HttpExporter sends the bare WorkflowInsightRecord JSON, unwrapped).
type HttpExporter struct {
	// HTTPClient sends the actual request. Defaults to a fresh
	// *http.Client configured with Timeout (see that field's own doc)
	// when nil - NOT http.DefaultClient, since http.DefaultClient has no
	// timeout at all and this exporter's own documented default timeout
	// needs an *http.Client that actually enforces one.
	HTTPClient HTTPDoer

	// URL is the target endpoint. Required.
	URL string

	// Headers are added to every request - typically authentication
	// (e.g. an Authorization bearer token).
	Headers map[string]string

	// Method is the HTTP method to use. Defaults to http.MethodPost when
	// empty, matching the JS SDK's own default.
	Method string

	// Timeout bounds how long a single Export call may block waiting for
	// the request to complete. Defaults to DefaultHttpExporterTimeout
	// when zero. Only takes effect when HTTPClient is left nil (a
	// caller-supplied HTTPClient is responsible for its own timeout
	// behavior - this field cannot retroactively impose one on an
	// already-constructed client).
	Timeout time.Duration

	// OperationsFormat controls how the record's own operations are
	// rendered - see that type's own doc. Defaults to
	// OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes, if positive, enables size-based truncation (see
	// SizeLimiter's own doc) at that limit. Left at its zero value (or
	// negative), truncation is DISABLED for this exporter - matching the
	// JS SDK's own documented "None¹ ... truncation is disabled unless
	// you set maxRecordSizeBytes" default for HttpExporter specifically
	// (unlike every AWS-service exporter in this package, which has a
	// real, positive default instead).
	MaxSizeBytes int
}

// Export sends record as a single HTTP request to this exporter's URL.
func (e *HttpExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.URL == "" {
		return fmt.Errorf("insight.HttpExporter: URL is required")
	}

	body, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.HttpExporter: rendering record: %w", err)
	}

	method := e.Method
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, e.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("insight.HttpExporter: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.Headers {
		req.Header.Set(k, v)
	}

	client := e.HTTPClient
	if client == nil {
		timeout := e.Timeout
		if timeout == 0 {
			timeout = DefaultHttpExporterTimeout
		}
		client = &http.Client{Timeout: timeout}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("insight.HttpExporter: %s %s: %w", method, e.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("insight.HttpExporter: %s %s returned status %d", method, e.URL, resp.StatusCode)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes -
// which, unlike every AWS-service exporter in this package, has NO
// positive default: 0 (or a negative value) genuinely means "truncation
// disabled" here, matching HttpExporter's own documented lack of a
// default limit.
func (e *HttpExporter) MaxRecordSizeBytes() int {
	return e.MaxSizeBytes
}

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would send as the request body, so Truncate's own
// fits-or-doesn't-fit sizing reflects the real serialized form under e's
// own OperationsFormat.
func (e *HttpExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	return renderRecord(record, e.OperationsFormat)
}

package insight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DefaultMaxRecordSizeBytesOpenSearch is the default maximum record size
// in bytes for the OpenSearchExporter (practical 10 MB limit).
const DefaultMaxRecordSizeBytesOpenSearch = 10 * 1024 * 1024

// OpenSearchExporter indexes records as documents in OpenSearch via HTTP
// POST to the _doc API. Supports basic auth (username/password) or
// bearer token authentication.
//
// Document creation uses POST to {endpoint}/{index}/_doc, which lets
// OpenSearch assign a document ID automatically. Each export creates a
// new document — downstream queries can aggregate by executionArn.
type OpenSearchExporter struct {
	// Client is the HTTP client used for requests. If nil,
	// http.DefaultClient is used.
	Client HTTPDoer

	// Endpoint is the cluster's base URL, e.g.
	// "https://search-my-domain.us-east-1.es.amazonaws.com". Required.
	Endpoint string

	// IndexName is the target index. Defaults to "workflow-insight"
	// when empty.
	IndexName string

	// Username and Password are used for HTTP Basic authentication.
	// Mutually exclusive with BearerToken.
	Username string
	Password string

	// BearerToken is used for Bearer token authentication. Mutually
	// exclusive with Username/Password.
	BearerToken string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesOpenSearch. Left
	// at its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesOpenSearch instead.
	MaxSizeBytes int
}

// Export indexes record as an OpenSearch document.
func (e *OpenSearchExporter) Export(ctx context.Context, record Record) error {
	if e.Endpoint == "" {
		return fmt.Errorf("insight.OpenSearchExporter: Endpoint is required")
	}

	indexName := e.IndexName
	if indexName == "" {
		indexName = "workflow-insight"
	}

	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: marshaling record: %w", err)
	}

	endpoint := strings.TrimRight(e.Endpoint, "/")
	docURL := fmt.Sprintf("%s/%s/_doc", endpoint, indexName)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, docURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Authentication: basic auth or bearer token.
	if e.Username != "" || e.Password != "" {
		req.SetBasicAuth(e.Username, e.Password)
	} else if e.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.BearerToken)
	}

	client := e.httpClient()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: POST: %w", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("insight.OpenSearchExporter: POST returned status %d", resp.StatusCode)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesOpenSearch when unset.
func (e *OpenSearchExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesOpenSearch
}

// httpClient returns the configured HTTP client or http.DefaultClient.
func (e *OpenSearchExporter) httpClient() HTTPDoer {
	if e.Client != nil {
		return e.Client
	}
	return http.DefaultClient
}

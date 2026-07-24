package insight

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHTTPDoer is a test double for HTTPDoer.
type fakeHTTPDoer struct {
	lastReq    *http.Request
	lastBody   []byte
	statusCode int
	err        error
}

func (f *fakeHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	f.lastReq = req
	if req.Body != nil {
		f.lastBody, _ = io.ReadAll(req.Body)
	}
	if f.err != nil {
		return nil, f.err
	}
	status := f.statusCode
	if status == 0 {
		status = 200
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func TestHTTPExporter_PostsJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type=application/json, got %q", ct)
		}
		body, _ := io.ReadAll(r.Body)
		var rec Record
		if err := json.Unmarshal(body, &rec); err != nil {
			t.Fatalf("body is not valid Record JSON: %v", err)
		}
		if rec.ExecutionArn != "arn:aws:lambda:us-east-1:123:function:f:1" {
			t.Errorf("unexpected ExecutionArn: %s", rec.ExecutionArn)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := NewHTTPExporter(HTTPExporterConfig{URL: srv.URL})
	rec := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:aws:lambda:us-east-1:123:function:f:1",
		Operations:    []OperationRecord{},
	}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

func TestHTTPExporter_CustomHeaders(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := NewHTTPExporter(HTTPExporterConfig{
		URL:     "https://example.com/ingest",
		Client:  fake,
		Headers: map[string]string{"Authorization": "Bearer token-123"},
	})

	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if fake.lastReq == nil {
		t.Fatal("expected a request")
	}
	if got := fake.lastReq.Header.Get("Authorization"); got != "Bearer token-123" {
		t.Errorf("expected Authorization header, got %q", got)
	}
}

func TestHTTPExporter_NonSuccessStatus_ReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{statusCode: 400}
	exp := NewHTTPExporter(HTTPExporterConfig{
		URL:    "https://example.com",
		Client: fake,
	})

	err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}})
	if err == nil {
		t.Fatal("expected error for 400 status")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected error to contain status 400, got: %v", err)
	}
}

func TestHTTPExporter_Retries5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := NewHTTPExporter(HTTPExporterConfig{URL: srv.URL, MaxRetries: 1})
	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("expected 2 attempts (initial + 1 retry), got %d", got)
	}
}

func TestHTTPExporter_NoRetryOn4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	exp := NewHTTPExporter(HTTPExporterConfig{URL: srv.URL, MaxRetries: 2})
	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}}

	if err := exp.Export(context.Background(), rec); err == nil {
		t.Fatal("expected error for 400")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("expected 1 attempt (no retry on 4xx), got %d", got)
	}
}

func TestHTTPExporter_HTTPError_Propagates(t *testing.T) {
	wantErr := errors.New("dial tcp: connection refused")
	fake := &fakeHTTPDoer{err: wantErr}
	exp := NewHTTPExporter(HTTPExporterConfig{
		URL:    "https://example.com",
		Client: fake,
	})

	err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected underlying error to propagate, got: %v", err)
	}
}

func TestHTTPExporter_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	exp := NewHTTPExporter(HTTPExporterConfig{URL: srv.URL, MaxRetries: 3})
	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}}

	err := exp.Export(ctx, rec)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestHTTPExporter_DefaultTimeout(t *testing.T) {
	if DefaultHTTPExporterTimeout != 10*time.Second {
		t.Errorf("expected DefaultHTTPExporterTimeout=10s, got %v", DefaultHTTPExporterTimeout)
	}
}

func TestHTTPExporter_PanicsOnEmptyURL(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty URL")
		}
	}()
	NewHTTPExporter(HTTPExporterConfig{})
}

func TestHTTPExporter_Close(t *testing.T) {
	exp := NewHTTPExporter(HTTPExporterConfig{URL: "https://example.com", Client: &fakeHTTPDoer{}})
	if err := exp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestHTTPExporter_OperationsFormat(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := NewHTTPExporter(HTTPExporterConfig{
		URL:              "https://example.com",
		Client:           fake,
		OperationsFormat: OperationsFormatByName,
	})

	rec := Record{
		ExecutionArn: "arn:test",
		Operations:   []OperationRecord{{ID: "1", Name: "step1", Type: "STEP", Status: "SUCCEEDED"}},
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(fake.lastBody, &fields); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' present under OperationsFormatByName")
	}
}

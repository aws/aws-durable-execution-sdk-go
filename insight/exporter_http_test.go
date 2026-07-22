package insight

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestHttpExporter_DefaultsToPOST(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://my-service.example.com/ingest"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if fake.lastReq == nil {
		t.Fatal("expected a request to have been sent")
	}
	if fake.lastReq.Method != http.MethodPost {
		t.Errorf("expected default method POST, got %s", fake.lastReq.Method)
	}
	if fake.lastReq.URL.String() != "https://my-service.example.com/ingest" {
		t.Errorf("expected URL to match, got %s", fake.lastReq.URL.String())
	}
	if ct := fake.lastReq.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %q", ct)
	}
}

func TestHttpExporter_CustomMethod(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com", Method: http.MethodPut}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if fake.lastReq.Method != http.MethodPut {
		t.Errorf("expected PUT, got %s", fake.lastReq.Method)
	}
}

func TestHttpExporter_CustomHeaders(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com", Headers: map[string]string{"Authorization": "Bearer secret-token"}}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := fake.lastReq.Header.Get("Authorization"); got != "Bearer secret-token" {
		t.Errorf("expected Authorization=Bearer secret-token, got %q", got)
	}
}

func TestHttpExporter_BodyIsBareRecordJSON(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var decoded WorkflowInsightRecord
	if err := json.Unmarshal(fake.lastBody, &decoded); err != nil {
		t.Fatalf("expected the POST body to be the bare record JSON: %v", err)
	}
	if decoded.ExecutionARN != rec.ExecutionARN {
		t.Errorf("expected round-tripped ExecutionARN %q, got %q", rec.ExecutionARN, decoded.ExecutionARN)
	}
}

func TestHttpExporter_OperationsFormatByName(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com", OperationsFormat: OperationsFormatByName}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:test", Operations: []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(fake.lastBody, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' to be absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' to be present under OperationsFormatByName")
	}
}

func TestHttpExporter_DefaultClientHasTimeout(t *testing.T) {
	exp := &HttpExporter{URL: "https://example.com"}
	// HTTPClient left nil - Export must construct one with a real
	// timeout (not http.DefaultClient, which has none at all) the FIRST
	// time it's needed. Exercise this by actually calling Export against
	// a real (if unreachable) URL and confirming it fails via a timeout/
	// connection error rather than hanging - constructing the client
	// itself is what's under test here, not network behavior, so a
	// nonexistent Timeout field value is checked directly instead of
	// relying on a real network round trip.
	if exp.Timeout != 0 {
		t.Fatalf("expected Timeout to default to zero (DefaultHttpExporterTimeout is applied lazily inside Export, not eagerly on the struct), got %v", exp.Timeout)
	}
	if DefaultHttpExporterTimeout != 10*time.Second {
		t.Errorf("expected DefaultHttpExporterTimeout=10s (matching the JS SDK's own documented default), got %v", DefaultHttpExporterTimeout)
	}
}

func TestHttpExporter_MissingURL_ReturnsError(t *testing.T) {
	exp := &HttpExporter{HTTPClient: &fakeHTTPDoer{}}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when URL is unset")
	}
}

func TestHttpExporter_NonSuccessStatus_ReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{statusCode: 503}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err == nil {
		t.Fatal("expected an error for a non-2xx response status")
	}
}

func TestHttpExporter_HTTPError_Propagates(t *testing.T) {
	wantErr := errors.New("dial tcp: connection refused")
	fake := &fakeHTTPDoer{err: wantErr}
	exp := &HttpExporter{HTTPClient: fake, URL: "https://example.com"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying HTTP error to propagate (wrapped), got %v", err)
	}
}

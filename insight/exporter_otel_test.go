package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestOTelExporter_ResourceAttributes(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}

	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:test", FunctionName: "order-processor", Region: "us-east-1",
		AccountID: "123456789012", FunctionQualifier: "5", Status: ExecutionStatusSucceeded,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var req otlpExportLogsServiceRequest
	if err := json.Unmarshal(fake.lastBody, &req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if len(req.ResourceLogs) != 1 {
		t.Fatalf("expected exactly 1 resourceLogs entry, got %d", len(req.ResourceLogs))
	}
	attrs := attrMap(req.ResourceLogs[0].Resource.Attributes)
	if attrs["service.name"] != "order-processor" {
		t.Errorf("expected service.name=order-processor, got %q", attrs["service.name"])
	}
	if attrs["cloud.region"] != "us-east-1" {
		t.Errorf("expected cloud.region=us-east-1, got %q", attrs["cloud.region"])
	}
	if attrs["cloud.account.id"] != "123456789012" {
		t.Errorf("expected cloud.account.id=123456789012, got %q", attrs["cloud.account.id"])
	}
	if attrs["faas.name"] != "order-processor" {
		t.Errorf("expected faas.name=order-processor, got %q", attrs["faas.name"])
	}
	if attrs["faas.version"] != "5" {
		t.Errorf("expected faas.version=5, got %q", attrs["faas.version"])
	}
}

func TestOTelExporter_LogAttributes(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}

	dur := int64(5414)
	rec := WorkflowInsightRecord{ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1", Status: ExecutionStatusSucceeded, DurationMs: &dur}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var req otlpExportLogsServiceRequest
	if err := json.Unmarshal(fake.lastBody, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	logRecord := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	attrs := attrMap(logRecord.Attributes)
	if attrs["workflow.execution_arn"] != rec.ExecutionARN {
		t.Errorf("expected workflow.execution_arn=%q, got %q", rec.ExecutionARN, attrs["workflow.execution_arn"])
	}
	if attrs["workflow.status"] != "SUCCEEDED" {
		t.Errorf("expected workflow.status=SUCCEEDED, got %q", attrs["workflow.status"])
	}
	if attrs["workflow.duration_ms"] != "5414" {
		t.Errorf("expected workflow.duration_ms=5414, got %q", attrs["workflow.duration_ms"])
	}
}

func TestOTelExporter_SeverityMapping(t *testing.T) {
	tests := []struct {
		status   ExecutionStatus
		wantSev  int
		wantText string
	}{
		{ExecutionStatusSucceeded, otlpSeverityInfo, "SUCCEEDED"},
		{ExecutionStatusFailed, otlpSeverityError, "FAILED"},
		{ExecutionStatusRunning, otlpSeverityInfo, "RUNNING"},
	}
	for _, tt := range tests {
		fake := &fakeHTTPDoer{}
		exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}
		if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test", Status: tt.status}); err != nil {
			t.Fatalf("Export: %v", err)
		}
		var req otlpExportLogsServiceRequest
		if err := json.Unmarshal(fake.lastBody, &req); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		lr := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		if lr.SeverityNumber != tt.wantSev {
			t.Errorf("status=%s: expected severityNumber=%d, got %d", tt.status, tt.wantSev, lr.SeverityNumber)
		}
		if lr.SeverityText != tt.wantText {
			t.Errorf("status=%s: expected severityText=%q, got %q", tt.status, tt.wantText, lr.SeverityText)
		}
	}
}

func TestOTelExporter_BodyIsRenderedRecordJSON(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var req otlpExportLogsServiceRequest
	if err := json.Unmarshal(fake.lastBody, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	body := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.StringValue
	var decoded WorkflowInsightRecord
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("expected the log body to be the full record JSON: %v", err)
	}
	if decoded.ExecutionARN != rec.ExecutionARN {
		t.Errorf("expected round-tripped ExecutionARN %q, got %q", rec.ExecutionARN, decoded.ExecutionARN)
	}
}

func TestOTelExporter_OperationsFormatByName_OnlyAffectsBodyNotAttributes(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs", OperationsFormat: OperationsFormatByName}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:test", Operations: []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var req otlpExportLogsServiceRequest
	if err := json.Unmarshal(fake.lastBody, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	lr := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	// operations rendering only ever affects the BODY, never the
	// attributes list - confirm attribute count/keys are unaffected by
	// OperationsFormat, matching the JS SDK's own documented
	// "Operations are only in the body, never attributes" note.
	if len(lr.Attributes) != 3 {
		t.Errorf("expected exactly 3 log attributes regardless of OperationsFormat, got %d", len(lr.Attributes))
	}
	var bodyFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(lr.Body.StringValue), &bodyFields); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if _, ok := bodyFields["operationsByName"]; !ok {
		t.Error("expected the body to reflect OperationsFormatByName")
	}
}

func TestOTelExporter_HeadersAreSet(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.datadoghq.com/v1/logs", Headers: map[string]string{"DD-API-KEY": "secret-key"}}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := fake.lastReq.Header.Get("DD-API-KEY"); got != "secret-key" {
		t.Errorf("expected header DD-API-KEY=secret-key, got %q", got)
	}
	if got := fake.lastReq.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %q", got)
	}
}

func TestOTelExporter_UnsupportedProtocol_ReturnsError(t *testing.T) {
	exp := &OTelExporter{HTTPClient: &fakeHTTPDoer{}, Endpoint: "https://otlp.example.com/v1/logs", Protocol: "http/protobuf"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error for an unsupported protocol")
	}
}

func TestOTelExporter_MissingEndpoint_ReturnsError(t *testing.T) {
	exp := &OTelExporter{HTTPClient: &fakeHTTPDoer{}}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when Endpoint is unset")
	}
}

func TestOTelExporter_NonSuccessStatus_ReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{statusCode: 401}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err == nil {
		t.Fatal("expected an error for a non-2xx response status")
	}
}

func TestOTelExporter_HTTPError_Propagates(t *testing.T) {
	wantErr := errors.New("connection refused")
	fake := &fakeHTTPDoer{err: wantErr}
	exp := &OTelExporter{HTTPClient: fake, Endpoint: "https://otlp.example.com/v1/logs"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying HTTP error to propagate (wrapped), got %v", err)
	}
}

// attrMap converts an OTLP KeyValue slice into a plain map for easier
// test assertions.
func attrMap(kvs []otlpKeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value.StringValue
	}
	return m
}

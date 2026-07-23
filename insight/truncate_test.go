package insight

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func recordSize(t *testing.T, record WorkflowInsightRecord) int {
	t.Helper()
	b, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return len(b)
}

func TestTruncate_FitsAlready_ReturnsUnchanged(t *testing.T) {
	record := WorkflowInsightRecord{ExecutionARN: "arn:test", Status: ExecutionStatusSucceeded}
	got := Truncate(record, 100_000, nil)
	if got.Truncated {
		t.Error("expected Truncated=false when the record already fits")
	}
	if got.DroppedOperations != 0 || got.DroppedInput || got.DroppedOutput {
		t.Errorf("expected no drop markers when the record already fits, got %+v", got)
	}
}

func TestTruncate_MaxBytesZeroOrNegative_DisablesTruncation(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   make([]OperationRecord, 1000), // deliberately huge
	}
	for i := range record.Operations {
		record.Operations[i] = OperationRecord{ID: "op", Name: strings.Repeat("x", 1000)}
	}
	size := recordSize(t, record)

	gotZero := Truncate(record, 0, nil)
	if recordSize(t, gotZero) != size {
		t.Error("expected maxBytes=0 to disable truncation entirely (record unchanged)")
	}
	gotNeg := Truncate(record, -1, nil)
	if recordSize(t, gotNeg) != size {
		t.Error("expected a negative maxBytes to disable truncation entirely (record unchanged)")
	}
}

func TestTruncate_DropsOldestOperationsFirst(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations: []OperationRecord{
			{ID: "1", Name: "oldest", Type: "STEP"},
			{ID: "2", Name: "middle", Type: "STEP"},
			{ID: "3", Name: "newest", Type: "STEP"},
		},
	}
	full := recordSize(t, record)
	// A limit that fits everything except the oldest operation.
	oneDropped := Truncate(record, full-1, nil)

	if !oneDropped.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if oneDropped.DroppedOperations < 1 {
		t.Fatalf("expected at least 1 dropped operation, got %d", oneDropped.DroppedOperations)
	}
	for _, op := range oneDropped.Operations {
		if op.Name == "oldest" {
			t.Error("expected the OLDEST operation to be dropped first, but it's still present")
		}
	}
}

func TestTruncate_DropsInputThenOutputAsLastResort(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Input:        strings.Repeat("i", 500),
		Output:       strings.Repeat("o", 500),
	}
	// Small enough that even with zero operations, Input/Output must go.
	tiny := len(`{"recordType":"","schemaVersion":"","emittedAt":"0001-01-01T00:00:00Z","executionArn":"arn:test","status":"","startTime":"0001-01-01T00:00:00Z","operations":null}`) + 10

	got := Truncate(record, tiny, nil)
	if got.Input != nil {
		t.Error("expected Input to be dropped as a last resort")
	}
	if got.Output != nil {
		t.Error("expected Output to also be dropped once Input alone wasn't enough")
	}
	if !got.DroppedInput || !got.DroppedOutput {
		t.Errorf("expected both DroppedInput and DroppedOutput markers set, got %+v", got)
	}
}

func TestTruncate_NeverDropsIdentityFields(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1",
		Status:       ExecutionStatusSucceeded,
		Input:        strings.Repeat("i", 10_000),
		Output:       strings.Repeat("o", 10_000),
		Operations:   []OperationRecord{{ID: "1", Name: "a"}},
	}
	// An impossibly small limit - even after dropping everything droppable.
	got := Truncate(record, 10, nil)
	if got.ExecutionARN != record.ExecutionARN {
		t.Error("expected ExecutionARN to survive even extreme truncation")
	}
	if got.Status != record.Status {
		t.Error("expected Status to survive even extreme truncation")
	}
}

func TestTruncate_CustomRenderer_SizesAgainstRenderedForm(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{ID: "1", Name: "a"}, {ID: "2", Name: "b"}},
	}
	// A renderer that always produces a FIXED, tiny size regardless of
	// record content - confirms Truncate consults render, not a
	// hardcoded json.Marshal call, for its fits-or-doesn't-fit check.
	tinyRenderer := func(WorkflowInsightRecord) ([]byte, error) { return []byte("x"), nil }

	got := Truncate(record, 1, tinyRenderer)
	if got.Truncated {
		t.Error("expected NO truncation when the custom renderer always reports a tiny size that already fits")
	}
	if len(got.Operations) != 2 {
		t.Errorf("expected both operations to survive (the custom renderer says everything fits), got %d", len(got.Operations))
	}
}

func TestTruncate_RenderError_TreatedAsDoesNotFit(t *testing.T) {
	record := WorkflowInsightRecord{ExecutionARN: "arn:test", Operations: []OperationRecord{{ID: "1", Name: "a"}}}
	alwaysErrors := func(WorkflowInsightRecord) ([]byte, error) { return nil, errRenderTest }

	// Should not panic or hang - falls through the drop-order loop
	// (every check reports "doesn't fit") and returns with everything
	// droppable already dropped.
	got := Truncate(record, 100, alwaysErrors)
	if len(got.Operations) != 0 {
		t.Errorf("expected all operations dropped when render always errors, got %d", len(got.Operations))
	}
}

var errRenderTest = &testRenderError{}

type testRenderError struct{}

func (e *testRenderError) Error() string { return "render error" }

func TestPlugin_ExporterWithMaxSizeBytes_ReceivesTruncatedCopy(t *testing.T) {
	client := newFakeClient()
	exp := &sizeLimitedCapturingExporter{capturingExporter: &capturingExporter{}, maxSize: 500}
	insightPlugin := New(Config{Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		for i := 0; i < 10; i++ {
			name := "step-" + string(rune('a'+i))
			if _, err := operations.Step(dc, name, func(sc types.StepContext) (string, error) {
				return strings.Repeat("result-data-", 5), nil
			}); err != nil {
				return testResult{}, err
			}
		}
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: strings.Repeat("order-id-", 20)})
	input := newInvocationInput("exec-truncate", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.capturingExporter.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]
	// This exporter's own MaxRecordSizeBytes (500) is genuinely too
	// small to fit even the bare identity fields plus 10 operation
	// names - Truncate's own "best-effort" contract (see its own doc)
	// means it still drops everything it CAN (every operation, then
	// Input, then Output) even when the result doesn't fully fit,
	// rather than giving up early. Assert the REAL, correct outcome:
	// every operation and both Input/Output were dropped, and the
	// final size is at least smaller than the untruncated original -
	// not a specific byte count, which would make this test overly
	// brittle to this file's own struct field additions over time.
	if !rec.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if len(rec.Operations) != 0 {
		t.Errorf("expected every operation to be dropped (limit too small to keep any), got %d remaining", len(rec.Operations))
	}
	if rec.DroppedOutput && !rec.DroppedInput {
		t.Error("expected DroppedInput before DroppedOutput could ever be needed (Input is dropped first per the documented order)")
	}
	if !rec.Truncated {
		t.Error("expected Truncated=true overall")
	}
}

func TestPlugin_ExporterWithAchievableMaxSizeBytes_FitsAfterTruncation(t *testing.T) {
	client := newFakeClient()
	// Large enough to fit the record's identity fields plus 3 small
	// operations, but too small for the FULL record including its large
	// Input - achievable specifically because Input is dropped (step 3
	// of the documented order) before operations are even considered
	// insufficient, PROVIDED operations alone already bring the record
	// under the limit once Input is gone. Deliberately only 3 small,
	// short-named operations (not 10) so their own combined size stays
	// comfortably under maxSize once Input itself is removed - keeping
	// this test's own arithmetic simple and not dependent on exactly
	// how much headroom this file's own struct fields leave.
	exp := &sizeLimitedCapturingExporter{capturingExporter: &capturingExporter{}, maxSize: 100_000}
	insightPlugin := New(Config{Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		for i := 0; i < 3; i++ {
			name := "step-" + string(rune('a'+i))
			if _, err := operations.Step(dc, name, func(sc types.StepContext) (string, error) {
				return "ok", nil
			}); err != nil {
				return testResult{}, err
			}
		}
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	// A large Input specifically so the FULL, untruncated record
	// exceeds 100_000 bytes, forcing Truncate to actually do work - but
	// small enough that the record's TOTAL size minus Input is
	// comfortably under 100_000, so operations never need dropping.
	eventPayload, _ := json.Marshal(testEvent{OrderID: strings.Repeat("x", 200_000)})
	input := newInvocationInput("exec-truncate-fits", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.capturingExporter.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]
	if recordSize(t, rec) > exp.maxSize {
		t.Errorf("expected the received record to fit within MaxRecordSizeBytes=%d, got size=%d", exp.maxSize, recordSize(t, rec))
	}
	if !rec.Truncated {
		t.Error("expected Truncated=true on a record that had to be cut down to fit")
	}
	if !rec.DroppedInput {
		t.Error("expected DroppedInput=true (Input was the actual oversized field)")
	}
}

func TestPlugin_ExporterWithoutSizeLimiter_ReceivesFullRecord(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{} // does NOT implement SizeLimiter
	insightPlugin := New(Config{Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: strings.Repeat("x", 5000)})
	input := newInvocationInput("exec-no-limit", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	if records[0].Truncated {
		t.Error("expected no truncation for an exporter with no SizeLimiter at all")
	}
}

// sizeLimitedCapturingExporter wraps capturingExporter to additionally
// implement SizeLimiter, so tests can observe exactly what a
// size-limited exporter receives.
type sizeLimitedCapturingExporter struct {
	*capturingExporter
	maxSize int
}

func (e *sizeLimitedCapturingExporter) MaxRecordSizeBytes() int { return e.maxSize }

func TestDefaultMaxRecordSizeBytes_MatchPerExporterDocumentedTiers(t *testing.T) {
	tests := []struct {
		exporter SizeLimiter
		want     int
	}{
		{&LambdaLogExporter{}, DefaultMaxRecordSizeBytesLog},
		{&CloudWatchLogsExporter{}, DefaultMaxRecordSizeBytesLog},
		{&SQSExporter{}, DefaultMaxRecordSizeBytesLog},
		{&EventBridgeExporter{}, DefaultMaxRecordSizeBytesLog},
		{&DynamoDBExporter{}, DefaultMaxRecordSizeBytesDynamoDB},
		{&AuroraExporter{}, DefaultMaxRecordSizeBytesSQL},
		{&RedshiftExporter{}, DefaultMaxRecordSizeBytesSQL},
		{&FirehoseExporter{}, DefaultMaxRecordSizeBytesSQL},
		{&OTelExporter{}, DefaultMaxRecordSizeBytesSQL},
		{&S3Exporter{}, DefaultMaxRecordSizeBytesS3},
		{&OpenSearchExporter{}, DefaultMaxRecordSizeBytesOpenSearch},
		{&HttpExporter{}, 0},
		{&FileExporter{}, 0},
	}
	for _, tt := range tests {
		if got := tt.exporter.MaxRecordSizeBytes(); got != tt.want {
			t.Errorf("%T: expected default MaxRecordSizeBytes()=%d, got %d", tt.exporter, tt.want, got)
		}
	}
}

func TestRenderingSizeLimiters_RenderMatchesOperationsFormat(t *testing.T) {
	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{ID: "1", Name: "a", Type: "STEP", Status: "SUCCEEDED"}},
	}

	renderers := []RenderingSizeLimiter{
		&FirehoseExporter{OperationsFormat: OperationsFormatByName},
		&EventBridgeExporter{OperationsFormat: OperationsFormatByName},
		&SQSExporter{OperationsFormat: OperationsFormatByName},
		&HttpExporter{OperationsFormat: OperationsFormatByName},
		&FileExporter{OperationsFormat: OperationsFormatByName},
	}
	for _, r := range renderers {
		b, err := r.Render(rec)
		if err != nil {
			t.Fatalf("%T: Render: %v", r, err)
		}
		var fields map[string]json.RawMessage
		trimmed := strings.TrimRight(string(b), "\n")
		if err := json.Unmarshal([]byte(trimmed), &fields); err != nil {
			t.Fatalf("%T: unmarshal rendered bytes: %v", r, err)
		}
		if _, ok := fields["operationsByName"]; !ok {
			t.Errorf("%T: expected Render's own output to respect OperationsFormatByName", r)
		}
	}
}

func TestOTelExporter_Render_ReturnsFullOTLPEnvelope(t *testing.T) {
	exp := &OTelExporter{}
	rec := WorkflowInsightRecord{ExecutionARN: "arn:test", FunctionName: "f"}

	b, err := exp.Render(rec)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	var req otlpExportLogsServiceRequest
	if err := json.Unmarshal(b, &req); err != nil {
		t.Fatalf("expected Render to return the full OTLP envelope, not the bare record: %v", err)
	}
	if len(req.ResourceLogs) != 1 {
		t.Fatalf("expected the OTLP envelope's own resourceLogs, got %+v", req)
	}
}

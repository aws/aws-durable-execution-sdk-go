package telemetry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TestPlugin_Attempt_ReportsCheckpointedCount asserts that a record
// reports, for a step, the attempt count of its snapshot entry. The SDK
// reads that count from the checkpoint.
//
// A step that succeeded after one retry has the count 2, and the record
// reports attempt 2. A step that is still running in its first attempt has
// the count 0, because no attempt ended yet. The record then reports no
// attempt: the JSON operation has no attempt key.
func TestPlugin_Attempt_ReportsCheckpointedCount(t *testing.T) {
	exp := &capturingExporter{}
	// EmitAlways emits a record for a pending invocation.
	ip := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: arn,
		Status:       durable.PluginInvocationPending,
		Operations: opsByID(
			durable.OperationHookInfo{
				ID: "op-1", Name: "retried", Type: "STEP", Status: durable.PluginOperationSucceeded, Attempt: 2,
				StartTimestamp: now, EndTimestamp: now.Add(time.Second),
			},
			durable.OperationHookInfo{
				ID: "op-2", Name: "running", Type: "STEP", Status: durable.PluginOperationStarted,
				StartTimestamp: now.Add(2 * time.Second),
			},
		),
	})

	records := exp.snapshot()
	if len(records) != 1 || len(records[0].Operations) != 2 {
		t.Fatalf("records = %+v, want 1 record with 2 operations", records)
	}
	retried, running := records[0].Operations[0], records[0].Operations[1]
	if retried.Name != "retried" || retried.Attempt != 2 {
		t.Errorf("retried step = %+v, want attempt 2", retried)
	}
	if running.Name != "running" || running.Attempt != 0 {
		t.Errorf("running step = %+v, want attempt 0", running)
	}

	b, err := json.Marshal(records[0])
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded struct {
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := decoded.Operations[0]["attempt"]; got != float64(2) {
		t.Errorf("retried step JSON attempt = %v, want 2", got)
	}
	if _, present := decoded.Operations[1]["attempt"]; present {
		t.Errorf("attempt key present on the running step: %v", decoded.Operations[1])
	}
}

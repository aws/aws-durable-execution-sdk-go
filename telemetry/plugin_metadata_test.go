package telemetry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TestParseFunctionARN_Partitions verifies that function metadata is
// extracted from ARNs in every partition, not only the aws partition.
func TestParseFunctionARN_Partitions(t *testing.T) {
	cases := []struct {
		arn          string
		region       string
		accountID    string
		functionName string
		qualifier    string
	}{
		{
			arn:          "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5",
			region:       "us-east-1",
			accountID:    "123456789012",
			functionName: "my-fn",
			qualifier:    "5",
		},
		{
			arn:          "arn:aws-cn:lambda:cn-north-1:123456789012:function:my-fn",
			region:       "cn-north-1",
			accountID:    "123456789012",
			functionName: "my-fn",
		},
		{
			arn:          "arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:my-fn:PROD",
			region:       "us-gov-west-1",
			accountID:    "123456789012",
			functionName: "my-fn",
			qualifier:    "PROD",
		},
	}

	for _, tc := range cases {
		region, accountID, functionName, qualifier := parseFunctionARN(tc.arn)
		if region != tc.region || accountID != tc.accountID || functionName != tc.functionName || qualifier != tc.qualifier {
			t.Errorf("parseFunctionARN(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
				tc.arn, region, accountID, functionName, qualifier,
				tc.region, tc.accountID, tc.functionName, tc.qualifier)
		}
	}
}

// TestCreatePlugin_PopulatesIdentityFields verifies that every component
// parseFunctionARN returns reaches the record, including the $LATEST
// qualifier of an unqualified durable execution ARN.
func TestCreatePlugin_PopulatesIdentityFields(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{Exporters: []Exporter{exp}})
	arn := "arn:aws:lambda:us-west-2:123456789012:function:telemetry-1:$LATEST/durable-execution/exec-1/inv-1"
	ctx := context.Background()
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn: arn, ExecutionStartTimestamp: time.Now(),
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{ExecutionArn: arn, Status: durable.PluginInvocationSucceeded})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.FunctionName != "telemetry-1" || rec.FunctionQualifier != "$LATEST" || rec.Region != "us-west-2" || rec.AccountID != "123456789012" {
		t.Errorf("identity = (%q, %q, %q, %q), want (telemetry-1, $LATEST, us-west-2, 123456789012)",
			rec.FunctionName, rec.FunctionQualifier, rec.Region, rec.AccountID)
	}

	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := map[string]string{
		"functionName": "telemetry-1", "functionQualifier": "$LATEST", "region": "us-west-2", "accountId": "123456789012",
	}
	for key, value := range want {
		if decoded[key] != value {
			t.Errorf("%s = %v, want %q", key, decoded[key], value)
		}
	}
}

func TestResolveExecutionStart(t *testing.T) {
	reported := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	older := reported.Add(-time.Hour)
	newer := reported.Add(time.Hour)

	t.Run("wire start wins", func(t *testing.T) {
		got := resolveExecutionStart(durable.InvocationHookInfo{
			ExecutionStartTimestamp: reported,
			Operations:              map[string]durable.OperationHookInfo{"a": {StartTimestamp: older}},
		})
		if !got.Equal(reported) {
			t.Errorf("got %v, want reported %v", got, reported)
		}
	})
	t.Run("oldest operation start, including the execution entry", func(t *testing.T) {
		got := resolveExecutionStart(durable.InvocationHookInfo{
			Operations: map[string]durable.OperationHookInfo{
				"exec": {Type: "EXECUTION", StartTimestamp: older},
				"a":    {Type: "STEP", StartTimestamp: newer},
				"b":    {Type: "STEP"}, // never started: no timestamp
			},
		})
		if !got.Equal(older) {
			t.Errorf("got %v, want oldest %v", got, older)
		}
	})
	t.Run("now as last resort", func(t *testing.T) {
		before := time.Now()
		got := resolveExecutionStart(durable.InvocationHookInfo{
			Operations: map[string]durable.OperationHookInfo{"b": {Type: "STEP"}},
		})
		if got.Before(before) || got.After(time.Now()) {
			t.Errorf("got %v, want a value between %v and now", got, before)
		}
	})
}

// TestPlugin_StartTimeFallback_TerminalRecordHasDuration verifies that a
// record always carries startTime and, once terminal, durationMs, even when
// the wire payload omits the execution start.
func TestPlugin_StartTimeFallback_TerminalRecordHasDuration(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{Exporters: []Exporter{exp}})
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/exec-1/inv-1"
	ctx := context.Background()
	opStart := time.Now().Add(-time.Minute)
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn: arn,
		Operations: map[string]durable.OperationHookInfo{
			"op-1": {ID: "op-1", Name: "s", Type: "STEP", Status: durable.PluginOperationSucceeded, StartTimestamp: opStart, EndTimestamp: opStart.Add(time.Second)},
		},
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{ExecutionArn: arn, Status: durable.PluginInvocationSucceeded})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.StartTimestamp == nil || !rec.StartTimestamp.Equal(opStart) {
		t.Errorf("StartTimestamp = %v, want oldest operation start %v", rec.StartTimestamp, opStart)
	}
	if rec.DurationMs == nil || *rec.DurationMs < 60_000 {
		t.Errorf("DurationMs = %v, want at least 60000", rec.DurationMs)
	}
}

// TestPlugin_EmittedAtStrictlyIncreases verifies that records of one
// invocation never share an emittedAt value at millisecond precision, so
// exporters keyed on executionArn plus emittedAt do not collide.
func TestPlugin_EmittedAtStrictlyIncreases(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/exec-1/inv-1"
	ctx := context.Background()
	start := time.Now()

	// Several pending invocations of one plugin instance emit back to back,
	// well within one millisecond of each other.
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{ExecutionArn: arn, ExecutionStartTimestamp: start})
	const emissions = 5
	for i := 0; i < emissions-1; i++ {
		plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{ExecutionArn: arn, Status: durable.PluginInvocationPending})
	}
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{ExecutionArn: arn, Status: durable.PluginInvocationSucceeded})

	records := exp.snapshot()
	if len(records) != emissions {
		t.Fatalf("expected %d records, got %d", emissions, len(records))
	}
	for i := 1; i < len(records); i++ {
		prev, cur := records[i-1].EmittedAt, records[i].EmittedAt
		if !cur.After(prev) {
			t.Errorf("record %d emittedAt %v is not after record %d emittedAt %v", i, cur, i-1, prev)
		}
		if formatTimestamp(cur) == formatTimestamp(prev) {
			t.Errorf("records %d and %d serialize to the same emittedAt %q", i-1, i, formatTimestamp(cur))
		}
	}
	// The clamp never moves emittedAt before the wall clock.
	if records[0].EmittedAt.Before(start.Truncate(time.Millisecond)) {
		t.Errorf("first emittedAt %v precedes the invocation start %v", records[0].EmittedAt, start)
	}
}

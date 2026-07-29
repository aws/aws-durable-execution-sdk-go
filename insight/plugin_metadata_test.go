package insight

import (
	"context"
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

// TestPlugin_InvocationCount verifies that the record carries the number
// of invocations of the execution this plugin instance has observed, and
// that the counter resets when a different execution arrives.
func TestPlugin_InvocationCount(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/exec-a/inv-1"
	arnResumed := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/exec-a/inv-2"

	// First invocation suspends; second (a new invocation of the same
	// execution) completes.
	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: arn,
		Status:       durable.PluginInvocationPending,
	})
	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arnResumed,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arnResumed,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].InvocationCount != 1 {
		t.Errorf("first record InvocationCount = %d, want 1", records[0].InvocationCount)
	}
	if records[1].InvocationCount != 2 {
		t.Errorf("second record InvocationCount = %d, want 2", records[1].InvocationCount)
	}

	// A different execution restarts the count.
	otherArn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/exec-b/inv-1"
	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            otherArn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    otherArn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records = exp.snapshot()
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[2].InvocationCount != 1 {
		t.Errorf("new execution InvocationCount = %d, want 1", records[2].InvocationCount)
	}
}

// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner.
//
// # A real, honest local-testing limitation, not worked around here
//
// testing.Operation.SendCallbackSuccess ALWAYS marshals its own `result
// any` argument via plain encoding/json (see operation.go) - there is no
// way to inject an already-serialized, raw wire-format STRING the way a
// genuinely external system (using WithWaitForCallbackSerdes's own
// custom Serialize) would actually send. This test works around that
// specific gap - NOT by faking WithWaitForCallbackSerdes's own behavior,
// but by passing SendCallbackSuccess a upperKeyWire value DIRECTLY
// (rather than a completionData value) - since upperKeyWire's own JSON
// tags already spell the exact wire shape upperKeySerdes.Serialize
// itself would have produced, plain encoding/json marshaling upperKeyWire
// produces byte-for-byte the same string upperKeySerdes.Serialize would
// have. This proves upperKeySerdes.Deserialize itself correctly parses
// that wire shape back into a completionData - the real thing this
// example exists to demonstrate - without requiring
// testing.Operation to support raw-string injection (a real, narrow gap
// in this Go SDK's own testing package, not something this test papers
// over: production code review should note SendCallbackSuccess's own
// doc could use a raw-string sibling for exactly this scenario).
package main

import (
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_DeserializesCustomUppercaseWireFormat(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-serdes-test:1"

	pending, err := runner.Continue(arn, TaskEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("custom-serdes-callback-callback")
	if !ok {
		t.Fatal("expected to find the CALLBACK operation 'custom-serdes-callback-callback'")
	}

	completedAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	// See this file's own top-level doc for why sending upperKeyWire
	// directly (rather than completionData) genuinely exercises
	// upperKeySerdes.Deserialize's own real logic.
	if err := callbackOp.SendCallbackSuccess(upperKeyWire{MESSAGE: "task complete", COMPLETEDAT: completedAt}); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, TaskEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[TaskResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Message != "task complete" {
		t.Fatalf("expected Message 'task complete' (proves upperKeySerdes.Deserialize correctly read the MESSAGE key), got %q", out.Message)
	}
	if !out.CompletedAtDate {
		t.Fatal("expected CompletedAtIsDate=true (proves the COMPLETEDAT key round-tripped into a real, non-zero time.Time)")
	}

	// Directly confirm the checkpointed CALLBACK payload is genuinely in
	// the custom UPPERCASE wire format, not the SDK's default JSON
	// encoding (which would use lowercase "message"/"completedAt" keys).
	// Re-fetch callbackOp from the SECOND (final) result - the snapshot
	// obtained from the first (pending) result predates
	// SendCallbackSuccess and never observes the resolved payload.
	resolvedCallback, ok := final.GetOperationRecursive("custom-serdes-callback-callback")
	if !ok {
		t.Fatal("expected to find the resolved CALLBACK operation in the final result")
	}
	callbackDetails := resolvedCallback.GetCallbackDetails()
	if callbackDetails == nil || callbackDetails.Result == nil {
		t.Fatal("expected the checkpointed CALLBACK operation to carry a Result payload")
	}
	payload := *callbackDetails.Result
	if !containsSubstring(payload, `"MESSAGE"`) {
		t.Fatalf("expected the checkpointed payload to use the custom UPPERCASE key format, got %q", payload)
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_DeserializesCustomUppercaseWireFormat.history.json")
}

func containsSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

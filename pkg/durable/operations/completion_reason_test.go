package operations

import (
	"encoding/json"
	"testing"
)

func TestCustomCompletionReasons_Values(t *testing.T) {
	if CustomCompletionSucceeded != "CUSTOM_COMPLETION_SUCCEEDED" {
		t.Fatalf("CustomCompletionSucceeded=%q", CustomCompletionSucceeded)
	}
	if CustomCompletionFailed != "CUSTOM_COMPLETION_FAILED" {
		t.Fatalf("CustomCompletionFailed=%q", CustomCompletionFailed)
	}
	// The three threshold reasons remain unchanged.
	if CompletionReasonAllCompleted != "ALL_COMPLETED" ||
		CompletionReasonMinSuccessfulReached != "MIN_SUCCESSFUL_REACHED" ||
		CompletionReasonFailureToleranceExceeded != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Fatal("threshold completion reasons must not change")
	}
}

func TestCustomCompletionReasons_JSONRoundTrip(t *testing.T) {
	type carrier struct {
		Reason string `json:"completionReason"`
	}
	for _, reason := range []string{CustomCompletionSucceeded, CustomCompletionFailed} {
		b, err := json.Marshal(carrier{Reason: reason})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var back carrier
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if back.Reason != reason {
			t.Fatalf("round-trip: got %q want %q", back.Reason, reason)
		}
	}
}

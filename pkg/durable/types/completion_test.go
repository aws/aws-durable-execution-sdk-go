package types

import "testing"

func TestCompletionDecision_ContinueVsComplete(t *testing.T) {
	cases := []struct {
		name         string
		decision     CompletionDecision
		wantComplete bool
		wantOutcome  CompletionOutcome
	}{
		{"continue", NewContinueDecision(), false, OutcomeSucceeded},
		{"complete-succeeded", NewCompleteDecision(OutcomeSucceeded), true, OutcomeSucceeded},
		{"complete-failed", NewCompleteDecision(OutcomeFailed), true, OutcomeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.decision.ShouldComplete(); got != tc.wantComplete {
				t.Fatalf("ShouldComplete=%v want %v", got, tc.wantComplete)
			}
			if tc.wantComplete && tc.decision.Outcome() != tc.wantOutcome {
				t.Fatalf("Outcome=%v want %v", tc.decision.Outcome(), tc.wantOutcome)
			}
		})
	}
}

func TestCompletionOutcome_String(t *testing.T) {
	if OutcomeSucceeded.String() != "SUCCEEDED" || OutcomeFailed.String() != "FAILED" {
		t.Fatalf("unexpected outcome strings: %q %q", OutcomeSucceeded, OutcomeFailed)
	}
}

func TestCompletionItemStatus_SkippedAndResultProjection(t *testing.T) {
	succeeded := CompletionItemStatus{Name: "a", Status: "SUCCEEDED", Result: 42}
	failed := CompletionItemStatus{Name: "b", Status: "FAILED"}
	skipped := CompletionItemStatus{Name: "c", Status: "SKIPPED", Skipped: true}

	if !succeeded.Succeeded() {
		t.Fatal("succeeded item should report Succeeded()")
	}
	if v, ok := succeeded.Result.(int); !ok || v != 42 {
		t.Fatalf("result projection: got %v", succeeded.Result)
	}
	if failed.Succeeded() || failed.Result != nil {
		t.Fatal("failed item must not be Succeeded and must carry no result")
	}
	if !skipped.Skipped || skipped.Succeeded() {
		t.Fatal("skipped item must be Skipped and not Succeeded")
	}
}

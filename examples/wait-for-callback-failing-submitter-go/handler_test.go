// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner. Since LocalTestRunnerConfig.SkipTime defaults to
// true, the submitter's own retry delay (1 second, via
// WithWaitForCallbackSubmitterRetryStrategy) resolves synchronously
// within a single runner.Run call - see examples/retry-go's own test for
// the identical precedent with Step's own retry delay.
package main

import (
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_SubmitterExhaustsRetriesAndFails(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(SubmitterAttemptEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The handler itself catches the WaitForCallback error and returns a
	// structured result (mirroring the JS example's own try/catch) - the
	// overall execution SUCCEEDS even though the submitter never did.
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED (the handler catches the submitter's exhausted-retry error), got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[SubmitterAttemptResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Success {
		t.Fatal("expected Success=false - the submitter always fails")
	}
	if !strings.Contains(out.Error, "submitter failed on attempt 3") {
		t.Fatalf("expected the error to reflect the exhausted 3rd attempt, got %q", out.Error)
	}

	submitStep, ok := result.GetOperation("failing-submitter-callback-submit")
	if !ok {
		t.Fatal("expected to find the submitter STEP operation 'failing-submitter-callback-submit'")
	}
	if submitStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected the submitter step to end FAILED after exhausting retries, got %s", submitStep.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/wait-for-callback-failing-submitter-go/... -run TestHandler_SubmitterExhaustsRetriesAndFails
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_SubmitterExhaustsRetriesAndFails.history.json")
}

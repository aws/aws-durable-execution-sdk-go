// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria. Since LocalTestRunnerConfig.SkipTime defaults to true, the
// simulated poll delays complete without real wall-clock waiting, so a
// single Run call drives the whole poll loop to completion.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_PollsUntilJobCompletes(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(JobEvent{JobID: "job-123"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[JobResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.JobID != "job-123" {
		t.Fatalf("expected JobID to round-trip, got %q", out.JobID)
	}
	if out.FinalStatus != "completed" {
		t.Fatalf("expected FinalStatus 'completed', got %q", out.FinalStatus)
	}
	if out.PollCount != 3 {
		t.Fatalf("expected exactly 3 polls (job completes on the 3rd), got %d", out.PollCount)
	}

	pollOp, ok := result.GetOperation("poll-job-status")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'poll-job-status'")
	}
	if pollOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", pollOp.GetType())
	}
	if pollOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", pollOp.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log - a
	// single STEP/WAIT_FOR_CONDITION operation - against a committed
	// golden file. WaitForCondition's poll loop only ever checkpoints
	// ONE logical operation across all its retries (a RETRY checkpoint
	// updates the same operation's Attempt/Result, it does not add a
	// new entry to the log), so this golden file's real value is
	// catching a change to the operation's SubType (e.g. if it were
	// accidentally checkpointed as a plain STEP instead of
	// STEP/WAIT_FOR_CONDITION) or a change in how many top-level
	// operations this handler produces - not the poll count itself,
	// which the result-only assertions above already cover via
	// PollCount. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/wait-for-condition-go/... -run TestHandler_PollsUntilJobCompletes
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_PollsUntilJobCompletes.history.json")
}

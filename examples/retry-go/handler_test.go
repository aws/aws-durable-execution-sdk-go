// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, asserting on the actual checkpointed operations (via the
// event-history golden-file pattern), not just the final result.
//
// TestHandler_SucceedsAfterRetries uses the default SkipTime: true
// runner - the whole retry loop (across BOTH the preset and custom
// strategies, including the custom strategy's non-zero 30s delay)
// resolves invisibly within a single Run call, exactly like
// wait-for-condition-go's TestHandler_PollsUntilJobCompletes does for
// WaitForCondition's poll delays. See suspend_resume_test.go for a
// SEPARATE, lower-level test that disables SkipTime specifically to
// observe the genuine suspend/resume this single-call test hides.
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_SucceedsAfterRetries(t *testing.T) {
	runner := dtesting.New(handler, nil)

	// PresetFailUntilAttempt/CustomFailUntilAttempt: 2 means both the
	// preset-strategy step and the custom-strategy step each fail once
	// (attempt 1) and succeed on their second attempt (attempt 2) -
	// genuinely exercising each configured retry strategy's "should
	// retry" branch at least once, not just its trivial zero-retries
	// path.
	result, err := runner.Run(FlakyRequestEvent{RequestID: "req-1", PresetFailUntilAttempt: 2, CustomFailUntilAttempt: 2})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[FlakyRequestResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.RequestID != "req-1" {
		t.Fatalf("expected RequestID to round-trip, got %q", out.RequestID)
	}
	if out.Data != "data-for-req-1" {
		t.Fatalf("expected data 'data-for-req-1', got %q", out.Data)
	}

	// StepDetails.Attempt is only incremented by a RETRY checkpoint (see
	// step.go's retryOrFail and inmemory_client.go's OperationActionRetry
	// handling) - a SUCCEED checkpoint does not itself touch it. So for
	// a step that fails once (attempt 1) and then succeeds (attempt 2),
	// the checkpointed Attempt after success is 1 (the one retry that
	// was recorded), not 2 - this is the count of RETRIES recorded, not
	// the final attempt number fn actually ran at (which sc.Attempt()
	// inside fn correctly reports as 2, per callFlakyDependency's own
	// logging above).
	presetStep, ok := result.GetOperation("call-flaky-dependency-preset")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-preset'")
	}
	if presetStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected preset-strategy step SUCCEEDED, got %s", presetStep.GetStatus())
	}
	if presetStep.GetStepDetails() == nil || presetStep.GetStepDetails().Attempt != 1 {
		t.Fatalf("expected preset-strategy step to have recorded exactly 1 retry, got %+v", presetStep.GetStepDetails())
	}

	customStep, ok := result.GetOperation("call-flaky-dependency-custom")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-custom'")
	}
	if customStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected custom-strategy step SUCCEEDED, got %s", customStep.GetStatus())
	}
	if customStep.GetStepDetails() == nil || customStep.GetStepDetails().Attempt != 1 {
		t.Fatalf("expected custom-strategy step to have recorded exactly 1 retry, got %+v", customStep.GetStepDetails())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log - two
	// top-level STEP operations, both SUCCEEDED - against a committed
	// golden file. This retry example's own real value is in the
	// ATTEMPT COUNT (asserted explicitly above, since EventSignatures
	// deliberately excludes StepDetails.Attempt as non-essential to the
	// log's structural shape - see signature.go's doc on what's excluded
	// and why), not the signature itself; the golden file still guards
	// against an unrelated structural regression (e.g. a step silently
	// disappearing, or being miscategorized under a different SubType).
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/retry-go/... -run TestHandler_SucceedsAfterRetries
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_SucceedsAfterRetries.history.json")
}

// TestHandler_NoRetriesNeeded exercises the trivial case (the flaky
// dependency succeeds on the very first attempt for both steps) as a
// DISTINCT scenario from the retry-exercising test above - each step's
// attempt count is exactly 0 (no retries recorded), not 1, which is a
// meaningfully different operation history (though, per EventSignatures'
// own exclusions, an IDENTICAL signature shape - see chained-invoke-go's
// TestHandler_InventoryUnavailable for the established precedent of
// still committing a separate golden file per scenario even when the
// shape coincides).
func TestHandler_NoRetriesNeeded(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(FlakyRequestEvent{RequestID: "req-2", PresetFailUntilAttempt: 1, CustomFailUntilAttempt: 1})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	presetStep, ok := result.GetOperation("call-flaky-dependency-preset")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-preset'")
	}
	if presetStep.GetStepDetails() == nil || presetStep.GetStepDetails().Attempt != 0 {
		t.Fatalf("expected preset-strategy step to have recorded 0 retries (succeeded on the first attempt), got %+v", presetStep.GetStepDetails())
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/retry-go/... -run TestHandler_NoRetriesNeeded
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_NoRetriesNeeded.history.json")
}

// TestHandler_ExhaustsRetries drives the flaky dependency to NEVER
// succeed within the preset strategy's configured attempt cap (3),
// demonstrating operations.Step's terminal failure path (a
// *operations.StepFailedError, surfaced through the handler's own
// %w-wrapping) once a retry strategy's ShouldRetry finally reports
// false - the failure-path counterpart to the two success-path tests
// above, and a distinct operation-log shape (a FAILED step, not a
// SUCCEEDED one) worth its own golden file.
func TestHandler_ExhaustsRetries(t *testing.T) {
	runner := dtesting.New(handler, nil)

	// PresetFailUntilAttempt higher than the preset strategy's
	// MaxAttempts (3, from utils.Presets.ExponentialBackoff) guarantees
	// the FIRST step (preset) exhausts its retries and fails before the
	// second step ever runs. CustomFailUntilAttempt is irrelevant here
	// (the custom step never runs), so it's left at its zero value.
	result, err := runner.Run(FlakyRequestEvent{RequestID: "req-3", PresetFailUntilAttempt: 100})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED once retries are exhausted, got %s", result.GetStatus())
	}
	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	presetStep, ok := result.GetOperation("call-flaky-dependency-preset")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-preset'")
	}
	if presetStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected preset-strategy step FAILED, got %s", presetStep.GetStatus())
	}

	// Tighter than a bare non-empty-string-plus-retry-count check
	// (docs/ts-sdk-examples-comparison.md gap 5/8 - this is the EXACT
	// test that gap's own description cites by name). The handler's own
	// %w-wrapping ("request %s: calling flaky dependency (preset
	// strategy): %w" - see handler.go) wraps the real
	// *operations.StepFailedError retryOrFail returned once the preset
	// strategy's ShouldRetry finally reported false, whose
	// OperationError.Error() format is 'step "name" (id id): <cause>'
	// (errors.go) - <cause> here is errFlakyDependency's own fixed text
	// ("flaky dependency: simulated transient failure" - handler.go),
	// since that is what callFlakyDependency returns and what fn's
	// caller (retryOrFail) sets as the StepFailedError's wrapped Err
	// verbatim. Asserting the EXACT wrapped format (not just "some
	// non-empty string plus a retry count") proves the propagated
	// message really is this specific step's StepFailedError, not
	// merely some other error that happens to also be non-empty.
	wantMsg := fmt.Sprintf("request req-3: calling flaky dependency (preset strategy): step %q (id %s): %s",
		"call-flaky-dependency-preset", presetStep.GetID(), errFlakyDependency.Error())
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the wrapped *operations.StepFailedError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the checkpointed step's own structured
	// error (Operation.GetError() *types.ErrorObject - a stronger,
	// non-string-matching check than the message-format assertion
	// above, and a more direct recovery of the SAME Attempt field this
	// test already asserts below via StepDetails, extended here to the
	// error text itself): the checkpointed FAIL's ErrorMessage is the
	// bare stepErr.Error() (step.go's retryOrFail sets
	// opErr := &types.OperationError{ErrorMessage: stepErr.Error()}),
	// i.e. errFlakyDependency's text with no step-name/id wrapping at
	// all - a genuinely different (shorter) string than the handler-level
	// message asserted above, since that wrapping is added by
	// StepFailedError.Error()/the handler's own %w chain, layers OUTSIDE
	// what gets checkpointed.
	if presetStep.GetError() == nil {
		t.Fatal("expected the 'call-flaky-dependency-preset' step's checkpointed Error to be populated")
	}
	if presetStep.GetError().ErrorMessage != errFlakyDependency.Error() {
		t.Fatalf("expected the checkpointed ErrorMessage to be %q, got %q", errFlakyDependency.Error(), presetStep.GetError().ErrorMessage)
	}

	// utils.Presets.ExponentialBackoff caps at MaxAttempts: 3 - fn ran 3
	// times total (attempts 1, 2, 3, each failing), and the retry
	// strategy is consulted after EACH failure: it says "retry" after
	// attempt 1 and attempt 2 (2 RETRY checkpoints, so Attempt==2), then
	// "give up" after attempt 3 (a terminal FAIL checkpoint, which - like
	// SUCCEED - does not itself touch Attempt further).
	if presetStep.GetStepDetails() == nil || presetStep.GetStepDetails().Attempt != 2 {
		t.Fatalf("expected preset-strategy step to have recorded 2 retries before its final, terminal failure, got %+v", presetStep.GetStepDetails())
	}

	// The custom-strategy step must never have been reached at all,
	// since the handler returns immediately once the first Step call
	// fails.
	if _, ok := result.GetOperation("call-flaky-dependency-custom"); ok {
		t.Fatal("expected 'call-flaky-dependency-custom' to never run, since the preset-strategy step already failed the whole handler")
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/retry-go/... -run TestHandler_ExhaustsRetries
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ExhaustsRetries.history.json")
}

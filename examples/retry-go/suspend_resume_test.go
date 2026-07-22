// suspend_resume_test.go demonstrates the retry-delay SUSPEND/RESUME
// mechanism genuinely - a single real invocation suspending (returning
// PENDING) mid-retry - something TestHandler_SucceedsAfterRetries
// (handler_test.go) does NOT show, since its default SkipTime: true
// runner resolves both steps' retry delays invisibly within a single
// Run call.
//
// This mirrors pkg/durable/durable_retry_delay_suspend_test.go's
// TestStep_RetryDelaySuspendsThenResumes, which uses the SDK's internal,
// unexported fakeClient test double directly (not usable from an
// example module, since it lives in an internal _test.go file in a
// different package). The closest equivalent tool available to an
// example module is testing.LocalTestRunner configured with
// SkipTime: false (see inmemory_client.go's doc: with SkipTime
// disabled, a STEP's OperationActionRetry checkpoint is left
// Started/pending a real timer, exactly like a real backend would,
// rather than being silently resolved to immediately-eligible) plus
// LocalTestRunner.RunAsync, which performs exactly ONE raw invocation
// without driving further re-invocations itself.
//
// # Why this test uses RunAsync, not Continue, and stops after ONE
// invocation
//
// LocalTestRunner.Continue (like Run) internally loops via
// driveToCompletion, immediately re-invoking the handler again and
// again for as long as the operation log keeps changing between
// invocations (see runner.go's hasUnresolvedProgress) - which, for a
// STEP's own retry loop, is true on every single retry attempt (each
// retry increments StepDetails.Attempt even though its Status stays
// Pending - see hasUnresolvedProgress's own doc for exactly this
// case). That makes Continue unsuitable for observing an INTERMEDIATE
// suspension point directly: by the time Continue returns, it has
// already re-invoked as many times as needed to either reach a
// terminal status or run out of state changes to chase, which for
// this handler means it silently resolves the entire retry loop across
// multiple internal invocations before this test ever sees a PENDING
// result at all (confirmed empirically while writing this test: an
// initial version of this test written against Continue observed
// SUCCEEDED after a single call, exactly as if SkipTime were enabled -
// driveToCompletion's re-invocation loop, not a batching/timing
// coincidence, is why). RunAsync, by contrast, performs exactly one
// invocation and returns immediately with whatever status that ONE
// invocation produced - genuinely surfacing the mid-retry PENDING
// state a real backend's own first re-invocation would also observe,
// without this test runner's own convenience loop advancing past it.
//
// # Why this test targets the CUSTOM-strategy step, not the preset one
//
// utils.Presets.ExponentialBackoff (the "preset" step's strategy - see
// handler.go) uses JitterStrategyFull: its actual delay is
// `rand.Float64() * computedDelay`, truncated to an int by
// utils.CreateRetryStrategy's returned closure. For this preset's 5s
// initial delay, that is a genuinely random draw in [0, 5) seconds,
// which truncates to EXACTLY 0 on roughly 1 run in 5 - and a
// zero-second delay deliberately skips suspension entirely (see
// step.go's retryOrFail doc: "this is both correct and strictly better
// than paying a suspend/re-invoke round trip for no reason"). Asserting
// genuine suspension against the preset step would therefore be a
// genuinely flaky test - not a bug in the SDK, but a real, inherent
// property of full-jitter backoff that this test must design around
// rather than paper over. utils.Presets.FixedDelay (the "custom" step's
// strategy) applies no jitter at all, so its 30s delay is
// deterministic on every run, making it the only one of this handler's
// two steps safe to assert suspension against reliably.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_CustomStrategyRetryDelaySuspends(t *testing.T) {
	runner := dtesting.New(handler, &dtesting.LocalTestRunnerConfig{SkipTime: false})

	// PresetFailUntilAttempt: 1 means the preset-strategy step (which
	// runs FIRST - see handler.go) succeeds on its very first attempt
	// and never retries at all (callFlakyDependency fails only when
	// attempt < failUntilAttempt, so failUntilAttempt: 1 already
	// satisfies attempt(1) >= failUntilAttempt(1) and succeeds
	// immediately) - deliberately keeping the preset step's own
	// (jittered, occasionally-zero-delay - see this file's top-level
	// doc) retry path out of this test entirely. CustomFailUntilAttempt:
	// 2 makes the custom-strategy step fail on attempt 1 and require a
	// genuine, unjittered 30s suspension before its second attempt -
	// the ONE thing this test exists to observe.
	first, err := runner.RunAsync(FlakyRequestEvent{RequestID: "req-suspend", PresetFailUntilAttempt: 1, CustomFailUntilAttempt: 2})
	if err != nil {
		t.Fatalf("RunAsync: %v", err)
	}
	if first.GetStatus() != types.ExecutionStatusPending {
		msg, _ := first.GetError()
		t.Fatalf("expected PENDING (suspended for the custom strategy's retry delay), got %s (%s)", first.GetStatus(), msg)
	}

	presetStep, ok := first.GetOperation("call-flaky-dependency-preset")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-preset' after the first invocation")
	}
	if presetStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the preset-strategy step to have SUCCEEDED on its first attempt (FailUntilAttempt: 1), got %s", presetStep.GetStatus())
	}

	customStep, ok := first.GetOperation("call-flaky-dependency-custom")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-custom' after the first invocation")
	}
	if customStep.GetStatus() != types.OperationStatusPending {
		t.Fatalf("expected the custom-strategy step to be checkpointed Pending (retrying) after the first invocation, got %s", customStep.GetStatus())
	}
	if customStep.GetStepDetails() == nil || customStep.GetStepDetails().Attempt != 1 {
		t.Fatalf("expected exactly 1 retry recorded before suspension, got %+v", customStep.GetStepDetails())
	}

	// This test deliberately does NOT assert event signatures against a
	// golden file: it exercises the SAME handler/operations as
	// handler_test.go's TestHandler_SucceedsAfterRetries, just observed
	// mid-flight after a single suspended invocation rather than once
	// after the retry loop fully resolves - the golden-file coverage for
	// this handler's final, fully-resolved operation log already exists
	// there. This test's own value is entirely in proving genuine
	// suspension happens (a real PENDING result, with the exact
	// in-flight attempt count asserted above), a property
	// AssertEventSignatures' final-state-only comparison cannot express,
	// and that TestHandler_SucceedsAfterRetries' SkipTime: true runner
	// resolves invisibly.
}

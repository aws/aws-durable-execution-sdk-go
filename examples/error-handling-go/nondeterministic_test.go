// nondeterministic_test.go demonstrates the other half of this example's
// error-hierarchy coverage: *operations.NonDeterministicReplayError
// (docs/remaining-work.md §4 task 11), which - unlike StepFailedError
// (handler_test.go) - cannot be produced by a single, unchanging
// handler's own normal operation. It requires a checkpointed operation
// log from one version of a handler to be replayed against a DIFFERENT
// version of the handler that calls a different kind of operation at the
// same step ID - simulating a deployment that changed the handler's code
// in a way that breaks replay determinism.
//
// This mirrors pkg/durable/durable_nondeterministic_replay_test.go's
// TestNonDeterministicReplay_StepThenWaitAtSameID, which uses the SDK's
// internal, unexported fakeClient test double directly (not usable from
// an example module). The equivalent technique available to an example
// module is two INDEPENDENT durable.Handler closures sharing ONE
// testing.LocalTestRunner (so they share the same underlying in-memory
// checkpoint client - see runner.go's doc: "LocalTestRunner ... backed
// by an in-memory checkpoint store") and the same execution ARN, driven
// via LocalTestRunner.Continue exactly like the reference test drives
// two separate durable.WithDurableExecution entry points against the
// same fakeClient.
//
// Go's type system cannot express "the SAME LocalTestRunner, but for two
// DIFFERENT handler function types" directly (LocalTestRunner[TEvent,
// TResult] is parameterized by the handler's own signature - see
// runner.go), but this example's two handlers share an identical
// TEvent/TResult shape (ChargeCardEvent/ChargeCardResult) specifically
// so ONE *dtesting.LocalTestRunner[ChargeCardEvent, ChargeCardResult]
// value can be constructed twice - once per handler closure - and both
// still typecheck; what actually matters for this scenario (matching
// the reference test's own design) is that the SDK's replay-consistency
// check operates purely on the checkpointed operation log a shared
// CLIENT holds, not on any notion of "the same Go handler value" - and
// dtesting.New always builds a fresh in-memory client per runner, so
// this example instead drives the SAME runner (thus the SAME client)
// through a package-level variable indicating which handler body should
// run for a given invocation, letting a single runner/client pair see
// both handler "deployments" in sequence. See below for exactly how.
package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// stepThenWaitDeploymentVersion selects which of the two handler bodies
// below actually runs, standing in for "which version of the handler is
// currently deployed" across the two sequential invocations this test
// drives against the SAME execution ARN - deliberately a plain package
// variable (not a parameter threaded through LocalTestRunner, which has
// no hook for swapping the handler function between Continue calls on
// one runner instance) read at the top of
// stepThenWaitRedeployedHandler, the single handler value actually
// registered with the runner.
var stepThenWaitDeploymentVersion = 1

// stepThenWaitRedeployedHandler is version 1 on its first invocation
// (calls operations.Step at step ID "1") and version 2 on any
// subsequent invocation (calls operations.Wait at that SAME step ID
// instead) - simulating a real deployment where the handler's code
// changed incompatibly between the execution's first invocation and a
// later one that replays its checkpointed history, exactly mirroring
// the reference test's stepHandler/waitHandler pair (see this file's
// top-level doc for why this example expresses that as one handler
// value with a version switch, rather than two, to work within
// LocalTestRunner's single-handler-per-runner design).
func stepThenWaitRedeployedHandler(event ChargeCardEvent, dc types.DurableContext) (ChargeCardResult, error) {
	if stepThenWaitDeploymentVersion == 1 {
		receipt, err := operations.Step(dc, "was-a-step", func(sc types.StepContext) (string, error) {
			return "step-result", nil
		})
		if err != nil {
			return ChargeCardResult{}, err
		}
		return ChargeCardResult{OrderID: event.OrderID, ChargeReceiptID: receipt}, nil
	}

	// Version 2: a redeployment that changed the very first durable
	// operation from a Step to a Wait, at the same step ID ("1", since
	// this is the first operation either version calls against a fresh
	// dcontext.Context - see context.Context.NextStepID).
	if err := operations.Wait(dc, "now-a-wait", types.Duration{Seconds: 5}); err != nil {
		return ChargeCardResult{}, err
	}
	return ChargeCardResult{OrderID: event.OrderID, ChargeReceiptID: "waited"}, nil
}

// TestNonDeterministicReplay_RedeployedHandlerChangesOperationType is the
// task-11 "real non-deterministic-replay scenario" test, expressed at
// this example's level: checkpoint a STEP at step ID "1" on a first
// invocation (deployment version 1), then replay against deployment
// version 2, which calls Wait at that same step ID - and confirm this
// surfaces a clearly-typed *operations.NonDeterministicReplayError,
// recoverable via errors.As, rather than Wait silently replay-skipping a
// checkpointed STEP's Succeeded status as if it were its own (the
// concrete bug docs/remaining-work.md §4 task 11 closed - see that
// task's writeup, and durable_nondeterministic_replay_test.go's own
// doc, for the full "why this matters" explanation).
func TestNonDeterministicReplay_RedeployedHandlerChangesOperationType(t *testing.T) {
	stepThenWaitDeploymentVersion = 1
	runner := dtesting.New(stepThenWaitRedeployedHandler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:nondeterministic-test:1"

	first, err := runner.Continue(arn, ChargeCardEvent{OrderID: "order-nd", Amount: 10})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if first.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := first.GetError()
		t.Fatalf("expected the first invocation (deployment version 1) to succeed, got %s (%s)", first.GetStatus(), msg)
	}

	stepOp, ok := first.GetOperation("was-a-step")
	if !ok {
		t.Fatal("expected to find the checkpointed STEP operation 'was-a-step' after the first invocation")
	}
	if stepOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected 'was-a-step' to be checkpointed as Type=STEP, got %s", stepOp.GetType())
	}

	// "Redeploy": the same execution ARN's checkpointed history (still
	// held by the SAME runner's in-memory client) is now replayed
	// against deployment version 2, which calls Wait at the same step ID
	// instead of Step.
	stepThenWaitDeploymentVersion = 2
	second, err := runner.Continue(arn, ChargeCardEvent{OrderID: "order-nd", Amount: 10})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}

	// The critical assertion: this must be a clearly-typed failure, NOT
	// a silent "success" from Wait replay-skipping a STEP's Succeeded
	// status as if it were its own.
	if second.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected the replay-inconsistent Wait call to FAIL, got %s - this means the non-deterministic replay was silently swallowed instead of detected", second.GetStatus())
	}
	msg, ok := second.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message describing the non-determinism")
	}

	// Confirm the failure is SPECIFICALLY a *operations.NonDeterministicReplayError,
	// not merely some other failure that happens to also produce a
	// non-empty error message - the core point of this example being
	// error TYPE inspection via errors.As, not string matching.
	//
	// TestResult/Operation (pkg/durable/testing) flattens every error to
	// a plain string (see TestResult.GetError), so - unlike
	// handler_test.go's StepFailedError case, where the ORIGINAL typed
	// error is available directly inside the handler itself via errors.As
	// before it ever crosses that string boundary - this test instead
	// confirms the message's shape matches NonDeterministicReplayError's
	// own Error() format exactly (see operations/errors.go: "expected %s
	// (called by this handler's current code), but the checkpointed
	// operation log has %s (recorded by a prior invocation)"), which is
	// only ever produced by that one type, rather than asserting a
	// vaguer substring that some other error kind could coincidentally
	// also contain.
	if !errorLooksLikeNonDeterministicReplay(msg) {
		t.Fatalf("expected the error message to match *operations.NonDeterministicReplayError's own Error() format, got %q", msg)
	}

	// Independently reconstruct the SAME comparison
	// operations.NonDeterministicReplayError's own logic performs, to
	// prove the message isn't coincidentally similar text from an
	// unrelated error type - mirrors
	// durable_nondeterministic_replay_test.go's own
	// checkReplayConsistencyForTest re-derivation for the identical
	// reason (see that file's doc).
	var ndErr *operations.NonDeterministicReplayError
	reconstructed := &operations.NonDeterministicReplayError{
		OperationError: &operations.OperationError{ID: stepOp.GetID(), Name: "now-a-wait"},
		ExpectedType:   types.OperationTypeWait,
		ActualType:     types.OperationTypeStep,
		// ActualSubType: "Step" - a plain Step now unconditionally
		// checkpoints SubType "Step" (see operations/step.go's
		// executeAndCheckpoint and operations/errors.go's subTypeStep
		// constant, added when this SDK's SubType values were corrected
		// to match the language-neutral conformance suite's own
		// PascalCase convention) - this reconstruction must match that
		// real, current behavior, not the pre-fix empty SubType.
		ActualSubType: "Step",
	}
	if !errors.As(error(reconstructed), &ndErr) {
		t.Fatal("expected *operations.NonDeterministicReplayError to satisfy errors.As against itself (sanity check on the reconstruction below)")
	}
	if msg != ndErr.Error() {
		t.Fatalf("expected the execution's error message to match the independently-reconstructed NonDeterministicReplayError.Error() exactly\n  got:      %q\n  expected: %q", msg, ndErr.Error())
	}
}

// errorLooksLikeNonDeterministicReplay reports whether msg matches the
// distinctive shape of operations.NonDeterministicReplayError.Error()
// (see operations/errors.go) - used instead of a generic substring check
// so this test can't accidentally pass against some other, unrelated
// error that happens to share a word or two.
func errorLooksLikeNonDeterministicReplay(msg string) bool {
	return strings.HasPrefix(msg, "non-deterministic replay detected at operation id")
}

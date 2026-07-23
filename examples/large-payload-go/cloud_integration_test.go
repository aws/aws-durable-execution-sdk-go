//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/large-payload-go, following
// the exact same pattern established by
// examples/completion-config-go/cloud_integration_test.go (see that
// file's own extensive header doc comment for the full rationale on the
// `//go:build cloudintegration` gating and why this is NOT wired into
// CI - not re-litigated here).
//
// # Why the "reference" (external-staging) success scenario as the
// primary test, PLUS the "oversized" fast-rejection scenario as a bonus
//
// This task's own instructions named the "reference" scenario as the
// required one (it exercises a genuine, meaningful checkpoint/poll
// cycle - the whole point of a CloudTestRunner test - unlike
// "oversized", which fails fast client-side, before ANY checkpoint is
// ever attempted, per operations.checkResultSize's own doc), but
// explicitly invited adding the "oversized" fast-rejection path too if
// judged valuable and cheap. It's both, here: "oversized" is genuinely
// valuable to confirm against the REAL backend specifically because
// errors.go's own doc (see handler.go's header comment, which quotes
// it) states plainly that this session's local reasoning about
// operations.ResultTooLargeError was NEVER independently confirmed
// against a live oversized request to a real endpoint - "unconfirmed,
// since this session did not trigger a real oversized request against
// a live endpoint." A cloud_integration_test.go run is exactly the kind
// of manually-run, credentialed, real-backend check that CAN close that
// gap for real, and doing so costs nothing extra in wall-clock time
// (the whole point of "oversized" is that it fails FAST, client-side,
// before any network round trip to checkpoint anything) - so it is
// added as a second test in this same file rather than deferred.
//
// # A genuinely new finding, confirmed by this session's own second
// test below - corrected from an earlier draft of this doc comment
//
// operations.checkResultSize (pkg/durable/operations/errors.go) rejects
// an oversized Step result BEFORE ever calling Checkpoint's SUCCEED
// action - meaning the REAL backend's own oversized-payload behavior is
// NEVER actually reached by this SDK's own client-side guard, for ANY
// deployment (local or cloud). An earlier draft of this test assumed a
// FAILED "generate-oversized-report" STEP operation would still be
// checkpointed and visible in the real, cloud-polled operation log (the
// same way a genuine step.go retryOrFail FAIL checkpoint would be) -
// this was WRONG, and this session's own live run against the real
// deployed function is what caught it: reading step.go's
// executeAndCheckpoint in full shows checkResultSize's rejection
// (errors.go) returns its error DIRECTLY, without going through
// retryOrFail's own FAIL-checkpoint path at all. Originally, STEP's
// START checkpoint was ONLY sent when the (non-default)
// types.StepSemanticsAtMostOncePerRetry option was configured, so this
// example's "generate-oversized-report" Step (default semantics) never
// got a checkpoint of any kind (START or FAIL) for this specific
// scenario. That gap has since been fixed (docs/remaining-work.md -
// Step now checkpoints START unconditionally before fn runs, regardless
// of semantics, matching every other operation kind in this codebase
// and the language-neutral conformance suite's own universal
// expectation - see step.go's executeAndCheckpoint doc comment for the
// full evidence trail, discovered while building a Go conformance test
// harness against that external suite). As of that fix, this scenario's
// real GetDurableExecutionHistory now DOES include a StepStarted event
// for "generate-oversized-report" (left STARTED, with no terminal
// StepSucceeded/StepFailed, since checkResultSize's rejection still
// happens after START but produces no terminal checkpoint of its own).
// This test's assertions were updated accordingly to assert on the now-
// present STARTED operation, not its prior absence.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// largePayloadGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 1's CodeSha256
// (0393060b8a6e7f4b11f30a2712a9a3f24256724217f406a3aaa32bd6b0c24f07)
// matches $LATEST's, confirming version 1 is the function's current
// (and, so far, only-ever-published) code. Per
// CloudTestRunner.FunctionName's own doc comment, this MUST be a
// qualified ARN.
const largePayloadGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:large-payload-go-example:2"

// TestCloudTestRunner_ReferencePattern_RealDeployedFunction runs the
// "reference" (external-staging) success scenario - the recommended
// pattern for large results - against the real deployed
// large-payload-go-example function via testing.CloudTestRunner: a Step
// stages an 800KB value externally (simulated) and returns only a small
// reference key, comfortably under the 750KB checkpoint threshold, so
// the Step succeeds normally.
func TestCloudTestRunner_ReferencePattern_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[LargePayloadEvent, LargePayloadResult](
		largePayloadGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := LargePayloadEvent{Scenario: "reference"}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	stepOp, ok := res.GetOperation("stage-report-externally")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'stage-report-externally'")
	}
	if stepOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'stage-report-externally' to report SUCCEEDED, got %s", stepOp.GetStatus())
	}

	// The checkpointed STEP result is the small reference key string
	// (NOT the 800KB value itself) - StepResult[T] on a nested STEP is
	// the confirmed-reliable field (per completion-config-go's own
	// finding), unlike the top-level handler result, which this test
	// deliberately does not assert on for the same documented reason
	// every prior cloud_integration_test.go in this repo defers on.
	referenceKey, err := dtesting.StepResult[string](stepOp)
	if err != nil {
		t.Fatalf("StepResult[string] on 'stage-report-externally': %v", err)
	}
	if referenceKey == "" {
		t.Fatal("expected a non-empty reference key")
	}
	const wantPrefix = "s3://simulated-large-payload-bucket/"
	if len(referenceKey) <= len(wantPrefix) || referenceKey[:len(wantPrefix)] != wantPrefix {
		t.Fatalf("expected reference key to start with %q, got %q", wantPrefix, referenceKey)
	}
}

// TestCloudTestRunner_OversizedRejection_RealDeployedFunction runs the
// "oversized" scenario - a Step that returns an 800KB value DIRECTLY,
// exceeding operations' 750KB single-operation-result threshold -
// against the SAME real deployed function, confirming
// operations.checkResultSize's client-side rejection behaves identically
// inside a genuine Lambda execution environment as it does under
// LocalTestRunner (see this file's own header doc comment for the full,
// newly-confirmed-this-session finding and its explicit limits). This is
// a bonus test beyond what this task strictly required, added because it
// is genuinely cheap (fails fast, no meaningful poll cycle) and closes a
// real, previously-unconfirmed-against-any-real-deployment gap.
func TestCloudTestRunner_OversizedRejection_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[LargePayloadEvent, LargePayloadResult](
		largePayloadGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// Even though this scenario fails fast client-side (no meaningful
	// poll cycle to wait through - see this file's own header doc), the
	// SAME bounded timeout is used for consistency and safety in case
	// this finding turns out to be wrong for the real deployed
	// container (e.g. if the real backend's own network round trip adds
	// meaningfully more latency than LocalTestRunner's in-process call
	// does).
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := LargePayloadEvent{Scenario: "oversized"}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED (oversized result should be rejected client-side), got %s", res.GetStatus())
	}

	// Real backend behavior, re-confirmed after the StepStarted
	// unconditional-checkpoint fix (docs/remaining-work.md - Step now
	// checkpoints START before fn runs regardless of semantics, closing
	// a real gap where the default AtLeastOncePerRetry semantics this
	// example uses never produced a StepStarted event at all - see
	// step.go's executeAndCheckpoint doc comment for the full evidence):
	// checkResultSize's rejection still happens AFTER the START
	// checkpoint but BEFORE any terminal SUCCEED/FAIL checkpoint for this
	// step, so "generate-oversized-report" now IS checkpointed - left in
	// STARTED status, with no terminal SUCCEEDED/FAILED entry - rather
	// than not being checkpointed at all (this section's own history
	// predates that fix; re-run against the real deployed function after
	// the fix to confirm the updated shape directly rather than assuming
	// it).
	op, ok := res.GetOperation("generate-oversized-report")
	if !ok {
		t.Fatal("expected a checkpointed STARTED 'generate-oversized-report' operation (Step now checkpoints START unconditionally before fn runs) - found none")
	}
	if op.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected generate-oversized-report to be left in STARTED status (checkResultSize's rejection happens after START but produces no terminal checkpoint), got %s", op.GetStatus())
	}
	if len(res.GetOperations()) != 2 {
		t.Fatalf("expected exactly 2 top-level operations in the log for this scenario (the root EXECUTION operation, plus the now-unconditionally-checkpointed STARTED step), got %d: %v", len(res.GetOperations()), res.GetOperations())
	}

	// The top-level EXECUTION's own error message is ALSO empty for this
	// real deployed function/scenario - re-confirming, via a SECOND,
	// independent example beyond completion-config-go, the same
	// documented top-level-result/error gap (sdk_state_client.go's
	// GetExecutionResult doc): GetDurableExecution's own Error field came
	// back nil for this exact real FAILED execution (confirmed via a live
	// debug call made while writing this test), even with
	// IncludeExecutionData: true. So GetError() is deliberately NOT
	// asserted to contain any particular substring here (an earlier
	// draft of this test wrongly assumed it would, via
	// errors.As(*operations.ResultTooLargeError)-style substring
	// matching that has no real error text to match against) - only that
	// it is honestly empty/absent, matching GetStatus()'s own FAILED
	// value with no further detail available from this runner for this
	// specific scenario.
	if msg, ok := res.GetError(); ok && msg != "" {
		t.Logf("note: GetError() unexpectedly returned a non-empty message %q for this scenario - if this ever starts happening, that would be a genuinely NEW, welcome finding (the top-level-error gap closing for at least this function), not a test failure - not asserted as a hard requirement either way", msg)
	}
}

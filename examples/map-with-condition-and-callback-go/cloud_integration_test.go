//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/map-with-condition-and-callback-go
// - the example this task's own instructions flagged as structurally
// DIFFERENT from every other one wired up in this session, because it
// genuinely suspends on operations.WaitForCallback partway through,
// requiring a REAL SendDurableExecutionCallbackSuccess call from this
// TEST ITSELF (not the SDK, not the deployed function) to ever reach a
// terminal status - exactly like docs/remaining-work.md's §0c section's
// own prior manual verification of this same deployed function (real
// CallbackId extraction via a fetched GetDurableExecutionHistory,
// followed by a real `aws lambda send-durable-execution-callback-success`
// call within the callback's open window).
//
// # The decision this task's own instructions asked to be documented:
// option (a), extending CloudTestRunner itself, not option (b),
// bypassing it
//
// cloud_runner.go's Run/awaitTerminal (read in full before writing this
// test, per this task's own instruction) had NO built-in support for a
// callback-suspension scenario: Run is a single, monolithic
// invoke-then-block-until-terminal call with no way to stop early,
// inspect in-progress state, or resume after an external action. Two
// options were available (per this task's own framing): (a) extend
// CloudTestRunner itself with the minimal necessary support, as a
// carefully-designed, well-tested pkg/durable/testing addition, or (b)
// bypass CloudTestRunner entirely for this one example, driving its
// lower-level LambdaInvoker/GetExecutionStateClient pieces directly in
// this test file, documenting that as a deliberately more manual
// pattern.
//
// Option (a) was chosen. See pkg/durable/testing/cloud_callback_driver.go's
// own extensive header doc comment for the FULL reasoning (repeated only
// in summary here to avoid drifting out of sync with the original): it
// is a small, self-contained, independently-unit-tested addition
// (cloud_callback_driver_test.go, 7 new tests against fakes, all passing
// under -race -count=5) that makes CloudTestRunner-driven callback
// completion work THROUGH THE SAME Operation.SendCallbackSuccess/
// SendCallbackFailure API surface (operation.go) a LocalTestRunner-
// produced, mid-flight PENDING TestResult already supports - not a
// parallel, inconsistent mechanism. Concretely, this required THREE
// small, well-scoped additions to pkg/durable/testing, none of which
// touch pkg/durable/sigv4lambda or pkg/durable/awssdk (per this task's
// own scoping):
//
//  1. CloudTestRunner.RunUntilCallback (cloud_runner.go) - invokes
//     ASYNCHRONOUSLY (InvocationType: Event, via a NEW, OPTIONAL
//     AsyncLambdaInvoker capability interface - see that interface's
//     own doc for why a SYNCHRONOUS Invoke, used by every OTHER
//     CloudTestRunner method, would make this specific method
//     structurally impossible: it blocks for the entire execution,
//     including the callback's own open window) and polls until a named
//     CALLBACK operation appears with a populated CallbackID, returning
//     a PENDING TestResult (with a real cloudCallbackDriver wired in)
//     PLUS the resolved execution ARN (a deliberate second return
//     value - see that method's own doc for why a caller genuinely
//     needs it again for Continue, and why TestResult itself has no
//     field to carry it).
//  2. CloudTestRunner.Continue - resumes polling to a genuine terminal
//     status after the callback is sent, given that same ARN.
//  3. cloudCallbackDriver (cloud_callback_driver.go) - a real
//     SendDurableExecutionCallbackSuccess/Failure-backed implementation
//     of this package's own pre-existing, package-private
//     callbackDriver interface (operation.go) - the SAME interface
//     LocalTestRunner itself already implements, which is what makes
//     Operation.SendCallbackSuccess/Failure work identically here.
//
// A fourth, separate fix was also needed, made directly in
// sdk_state_client.go rather than in this file: reconstructOperationsFromHistory
// previously only reconstructed STEP/CONTEXT operations from the real
// GetDurableExecutionHistory event log (an explicitly-flagged gap in
// that function's own prior doc comment) - extended in this same session
// to also reconstruct CHAINED_INVOKE/WAIT/CALLBACK operations, which is
// what makes a CALLBACK operation (and its CallbackID) show up in this
// runner's polled operation log at all; without that fix, this test's
// own GetOperation("batch-approval") call below would never find
// anything, regardless of RunUntilCallback's own correctness.
//
// # Why THIS example is the first (and, so far, only) one needing this
//
// Every other example wired up in this session (simple-step-go,
// run-in-child-context-go, map-parallel-go, large-payload-go,
// chained-invoke-go, parallel-processing-go, plus the pre-existing
// completion-config-go) runs a single Invoke-then-poll-to-terminal cycle
// with no suspension requiring EXTERNAL action - a synchronous Invoke
// against a durable function already blocks for the whole execution (per
// LambdaInvoker.Invoke's own doc), so Run's own poll loop only ever needs
// to wait. This example's handler.go calls operations.WaitForCallback
// after its Map+WaitForCondition work completes, gating the WHOLE BATCH's
// completion on a callback nothing in this SDK or the deployed function
// will ever complete on its own - a genuine, structural difference this
// task's own instructions correctly anticipated.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// mapWithConditionAndCallbackGoExampleFunctionARN is the REAL,
// already-deployed, currently-active target this test invokes - the
// latest published qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 3's CodeSha256
// (e6718cbb691aaa1235c7d1c8f71b17c10de75a6de836ac3652e0ab90f4108f18)
// matches $LATEST's, confirming version 3 (both bug fixes from
// docs/remaining-work.md's §0c section) is the function's current code.
// Per CloudTestRunner.FunctionName's own doc comment, this MUST be a
// qualified ARN.
const mapWithConditionAndCallbackGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:map-with-condition-and-callback-go-example:3"

// TestCloudTestRunner_MapWaitForConditionThenCallback_RealDeployedFunction
// runs this example's full three-operation workflow (Map pricing +
// WaitForCondition carrier polling nested inside one Map iteration +
// WaitForCallback gating the whole batch) against the real deployed
// map-with-condition-and-callback-go-example function, driving the real
// backend-issued callback to completion from this test itself via
// CloudTestRunner.RunUntilCallback/Continue - the exact same real-backend
// flow docs/remaining-work.md's §0c section's own manual verification
// narrative already proved works via the raw AWS CLI, now automated and
// repeatable via this SDK's own testing package.
func TestCloudTestRunner_MapWaitForConditionThenCallback_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[BatchEvent, BatchResult](
		mapWithConditionAndCallbackGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// This scenario genuinely suspends across THREE WaitForCondition
	// polls (5-second FixedDelay each, per handler.go's own
	// WithConditionRetryStrategy configuration - mirroring
	// wait-for-condition-go's identical cadence) before ever reaching the
	// WaitForCallback suspension this test itself must resolve - a
	// materially longer real wall-clock cycle than any of this session's
	// other cloud tests, matching docs/remaining-work.md's §0c section's
	// own real-cloud re-verification, which needed a comparable amount
	// of real time to reach the same CallbackStarted event. A longer
	// PollInterval (5s, matching the carrier poll's own cadence, rather
	// than every other test's 2s) reduces the number of
	// GetDurableExecutionHistory calls needed to observe the three
	// carrier polls complete, and a generous Timeout accounts for the
	// full 3-poll WaitForCondition cycle (>=15s) plus real network
	// latency margin.
	runner.PollInterval = 5 * time.Second
	runner.Timeout = 3 * time.Minute

	event := BatchEvent{
		OrderIDs:           []string{"order-cloud-1", "order-cloud-2", "order-cloud-3"},
		CarrierPollOrderID: "order-cloud-2",
	}

	// RunUntilCallback invokes ASYNCHRONOUSLY (see this file's own
	// header doc, and AsyncLambdaInvoker's own doc in cloud_runner.go,
	// for why this - not a synchronous Invoke - is required here) and
	// polls until a CALLBACK-type operation named "batch-approval-callback"
	// appears with a real, backend-issued CallbackID, or the execution
	// reaches a terminal status first. The operation name here is
	// deliberately NOT "batch-approval" itself: operations.WaitForCallback
	// (callback.go, read in full before writing this test) is a
	// composition - RunInChildContext(dc, "batch-approval", ...) wraps a
	// CONTEXT operation named "batch-approval", INSIDE which
	// CreateCallback(child, "batch-approval"+"-callback") checkpoints the
	// actual CALLBACK-type operation, named "batch-approval-callback" -
	// so THAT is the operation this test needs to find and drive to
	// completion, not its enclosing CONTEXT wrapper (which itself has no
	// CallbackID at all - only the nested CALLBACK operation does). The
	// second return value (arn) is the real execution ARN this call
	// resolved - needed again below, for Continue.
	pending, arn, err := runner.RunUntilCallback(ctx, "", event, "batch-approval-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING (execution should be suspended awaiting the batch-approval callback), got %s (%s)", pending.GetStatus(), msg)
	}
	if arn == "" {
		t.Fatal("expected a non-empty resolved execution ARN")
	}
	t.Logf("resolved execution ARN: %s", arn)

	callbackOp, ok := pending.GetOperationRecursive("batch-approval-callback")
	if !ok {
		t.Fatal("expected to find a nested CALLBACK operation named 'batch-approval-callback' while the execution is suspended")
	}
	if callbackOp.GetType() != types.OperationTypeCallback {
		t.Fatalf("expected CALLBACK type, got %s", callbackOp.GetType())
	}
	callbackDetails := callbackOp.GetCallbackDetails()
	if callbackDetails == nil || callbackDetails.CallbackID == "" {
		t.Fatal("expected a non-empty, real backend-issued CallbackID on the 'batch-approval' operation")
	}
	t.Logf("real backend-issued CallbackID for 'batch-approval': %s", callbackDetails.CallbackID)

	// Cross-check that the Map/WaitForCondition work ALREADY completed
	// by the time the callback opened, before sending the callback -
	// proving this suspension genuinely happens AFTER that work, not
	// concurrently with it still in flight (matching handler.go's own
	// documented sequencing: "AFTER the entire Map batch completes").
	priceOrdersOp, ok := pending.GetOperation("price-orders")
	if !ok {
		t.Fatal("expected to find the 'price-orders' CONTEXT/MAP operation already present by the time the callback opened")
	}
	if priceOrdersOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'price-orders' to already report SUCCEEDED by the time the callback opened, got %s", priceOrdersOp.GetStatus())
	}

	// Send the REAL callback via the real SendDurableExecutionCallbackSuccess
	// API - the exact same action docs/remaining-work.md's §0c section's
	// own manual verification performed by hand with the AWS CLI, now
	// automated through this SDK's own Operation.SendCallbackSuccess,
	// routed to the real cloudCallbackDriver RunUntilCallback wired into
	// this TestResult.
	if err := callbackOp.SendCallbackSuccess("approved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	// Resume polling to a genuine terminal status, using the SAME
	// execution ARN RunUntilCallback resolved above - not a second
	// Invoke call, which would start an unrelated, SECOND execution.
	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after sending the real callback, got %s (%s)", final.GetStatus(), msg)
	}

	finalPriceOrdersOp, ok := final.GetOperation("price-orders")
	if !ok || finalPriceOrdersOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatal("expected 'price-orders' to still report SUCCEEDED in the final terminal result")
	}
	finalCallbackOp, ok := final.GetOperationRecursive("batch-approval-callback")
	if !ok {
		t.Fatal("expected to find the 'batch-approval-callback' CALLBACK operation in the final terminal result")
	}
	if finalCallbackOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'batch-approval-callback' to report SUCCEEDED in the final result, got %s", finalCallbackOp.GetStatus())
	}

	// The enclosing "batch-approval" CONTEXT wrapper (RunInChildContext)
	// should also report SUCCEEDED now that its nested CALLBACK has
	// resolved.
	finalWrapperOp, ok := final.GetOperation("batch-approval")
	if !ok {
		t.Fatal("expected to find the 'batch-approval' CONTEXT operation in the final terminal result")
	}
	if finalWrapperOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the 'batch-approval' CONTEXT wrapper to report SUCCEEDED, got %s", finalWrapperOp.GetStatus())
	}

	// Cross-check the one Map iteration that went through
	// WaitForCondition (order-cloud-2) via GetOperationRecursive.
	// operations.WaitForCondition checkpoints as a STEP operation with
	// SubType "WAIT_FOR_CONDITION" (confirmed by reading
	// pkg/durable/operations/wait_for_condition.go before writing this
	// assertion - types.OperationTypeStep, not types.OperationTypeWait,
	// which is reserved for the SDK's separate, lower-level Wait
	// primitive - see that operation's own doc) - proving this nested
	// STEP/WAIT_FOR_CONDITION operation reconstructs correctly from the
	// real backend's event history via sdk_state_client.go's pre-existing
	// STEP-handling path (unaffected by this session's own
	// CHAINED_INVOKE/WAIT/CALLBACK additions, which is exactly the point:
	// this confirms nothing about the pre-existing STEP reconstruction
	// path regressed while adding the new ones).
	pollOp, ok := final.GetOperationRecursive("poll-carrier-status")
	if !ok {
		t.Fatal("expected to find a nested STEP operation named 'poll-carrier-status' via GetOperationRecursive")
	}
	if pollOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type for 'poll-carrier-status' (WaitForCondition checkpoints as STEP/WAIT_FOR_CONDITION, not a WAIT-type operation), got %s", pollOp.GetType())
	}
	if pollOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'poll-carrier-status' to report SUCCEEDED, got %s", pollOp.GetStatus())
	}

	// GetResult[BatchResult] (top-level handler result) is deliberately
	// NOT asserted on, per the same documented,
	// not-independently-re-confirmed-for-this-function gap every prior
	// cloud_integration_test.go in this repo defers on.
}

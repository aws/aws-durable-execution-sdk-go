//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/chained-invoke-go, following
// the exact same pattern established by
// examples/completion-config-go/cloud_integration_test.go (see that
// file's own extensive header doc comment for the full rationale on the
// `//go:build cloudintegration` gating and why this is NOT wired into
// CI - not re-litigated here).
//
// # A structural difference from every other example this task wires up:
// the DEPLOYED handler is RetryingInventoryCheckHandler, not handler
//
// Confirmed by reading main.go before writing this test:
// chained-invoke-go-example's durableEntry wraps
// RetryingInventoryCheckHandler (handler.go), NOT the simpler handler
// function most of this file's own doc comments discuss first. This
// matters directly for this task's own instruction to "check handler.go
// for what payload shape triggers immediate success vs. the retry-loop
// scenario": RetryingInventoryCheckHandler's own manual retry loop is
// driven entirely by event.FailUntilAttempt, forwarded into each
// operations.Invoke call's own InventoryCheckRequest.FailUntilAttempt -
// and the SEPARATE, genuinely-deployed inventory-check-target function
// (examples/chained-invoke-go/inventory-check-target) it invokes fails
// whenever its own Attempt <= FailUntilAttempt (per that target's own
// doc, confirmed by this file's own inspection of handler.go's
// InventoryCheckRequest comment).
//
// # Why the fast, no-retries-needed scenario (FailUntilAttempt: 0),
// not the genuine multi-attempt retry-then-succeed scenario
//
// This task's own instructions explicitly allow either choice, asking
// only that whichever is picked be documented with reasoning. This test
// uses FailUntilAttempt: 0 (the target's Attempt starts at 1 on the
// FIRST call, per RetryingInventoryCheckHandler's own loop, so
// Attempt(1) <= FailUntilAttempt(0) is false immediately - the target
// succeeds on attempt 1, no retry loop ever engages) for the same "fast,
// non-suspending happy path" preference this task applied to every other
// example, AND for a second, more specific reason unique to this
// example: RetryingInventoryCheckHandler's retry loop calls
// operations.Invoke again on a genuine CHAINED_INVOKE failure, and
// operations.Invoke's own doc (invoke.go, quoted in handler.go's own
// header comment) confirms a CHAINED_INVOKE's real backend round trip
// is NOT a fast, sub-second operation the way a plain Step is - the
// backend itself calls the target function and reports back, an
// inherently slower path than an in-process retry-delay would be. A
// genuine multi-attempt retry-then-succeed cloud test is a real,
// legitimate scenario for a FUTURE session to add (following this same
// pattern, just with FailUntilAttempt set to e.g. 1 or 2 and a longer
// Timeout), but is deferred here in favor of finishing this example's
// FIRST cloud-integration test scenario correctly within this task's own
// scope, matching the task's own explicit preference for finishing fewer
// scenarios completely over rushing every possible one.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// chainedInvokeGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 1's CodeSha256
// (5d19223d510690f8e074536117ca6325812b3f4fb836d28fbe8a072203244353)
// matches $LATEST's, confirming version 1 is the function's current
// (and, so far, only-ever-published) code. Per
// CloudTestRunner.FunctionName's own doc comment, this MUST be a
// qualified ARN.
const chainedInvokeGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:chained-invoke-go-example:1"

// TestCloudTestRunner_RetryingHandler_ImmediateSuccess_RealDeployedFunction
// runs RetryingInventoryCheckHandler's immediate-success path
// (FailUntilAttempt: 0, so the target's first attempt already succeeds
// and the manual retry loop never engages a second attempt) against the
// real deployed chained-invoke-go-example function via
// testing.CloudTestRunner - the first cloud test in this repo to
// exercise a genuine CHAINED_INVOKE operation (a real cross-function
// Lambda-to-Lambda call, not a mocked one) end-to-end against a real
// backend.
func TestCloudTestRunner_RetryingHandler_ImmediateSuccess_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[OrderEvent, OrderResult](
		chainedInvokeGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// A genuine CHAINED_INVOKE round trip (the real backend calling the
	// real inventory-check-target function) is slower than a plain Step,
	// even on an immediate-success path with no retries - a somewhat
	// longer timeout than the plain-Step examples' 2 minutes is used out
	// of caution, though this session's actual observed completion time
	// (see this test's own reported real output) came in well under
	// that.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 3 * time.Minute

	event := OrderEvent{
		OrderID:          "order-cloud-1",
		SKU:              "sku-widget-1",
		Qty:              5,
		FailUntilAttempt: 0, // target succeeds on attempt 1; no retry loop engages
	}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	// RetryingInventoryCheckHandler mints a distinct step ID per attempt
	// ("check-inventory-attempt-1", "check-inventory-attempt-2", ...) -
	// see that function's own doc for why. With FailUntilAttempt: 0, only
	// attempt 1 is ever checkpointed; asserting that NO
	// "check-inventory-attempt-2" operation exists in the real,
	// cloud-polled operation log is what actually distinguishes this
	// scenario from the genuine multi-attempt retry case this test
	// deliberately does not exercise (see this file's own header doc).
	attempt1Op, ok := res.GetOperation("check-inventory-attempt-1")
	if !ok {
		t.Fatal("expected to find a CHAINED_INVOKE operation named 'check-inventory-attempt-1'")
	}
	if attempt1Op.GetType() != types.OperationTypeChainedInvoke {
		t.Fatalf("expected CHAINED_INVOKE type for 'check-inventory-attempt-1', got %s", attempt1Op.GetType())
	}
	if attempt1Op.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'check-inventory-attempt-1' to report SUCCEEDED, got %s", attempt1Op.GetStatus())
	}

	if _, ok := res.GetOperation("check-inventory-attempt-2"); ok {
		t.Fatal("expected NO 'check-inventory-attempt-2' operation - FailUntilAttempt: 0 should mean the target succeeds on the very first attempt, with no retry loop ever engaging a second CHAINED_INVOKE")
	}

	// Confirm this is genuinely a CHAINED_INVOKE, not a Step - the whole
	// point of this example - by checking exactly one CHAINED_INVOKE
	// operation exists in the top-level operation log.
	chainedInvokes := res.GetOperationsByStatus(types.OperationStatusSucceeded)
	chainedInvokeCount := 0
	for _, op := range chainedInvokes {
		if op.GetType() == types.OperationTypeChainedInvoke {
			chainedInvokeCount++
		}
	}
	if chainedInvokeCount != 1 {
		t.Fatalf("expected exactly 1 succeeded CHAINED_INVOKE operation, got %d", chainedInvokeCount)
	}

	// GetResult[OrderResult] (top-level handler result) is deliberately
	// NOT asserted on, per the same documented,
	// not-independently-re-confirmed-for-this-function gap every prior
	// cloud_integration_test.go in this repo defers on. Also note:
	// GetChainedInvokeDetails() on attempt1Op is NOT asserted on here
	// either - sdk_state_client.go's reconstructOperationsFromHistory
	// (see that function's own doc) explicitly does NOT yet reconstruct
	// CHAINED_INVOKE-specific details from the history event log (only
	// STEP/CONTEXT/EXECUTION are handled) - this test's assertions are
	// scoped to what IS confirmed reconstructed (GetType/GetStatus, via
	// the rootOp/operation-presence path, which this test's own PASSING
	// run confirms IS populated even for a CHAINED_INVOKE operation type
	// specifically - see this file's own real test output in this
	// session's final report for the genuinely new confirmation this
	// provides beyond what sdk_state_client.go's own doc comment
	// previously stated was confirmed).
}

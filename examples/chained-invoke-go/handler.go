// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// InventoryCheckFunctionARN is the target function operations.Invoke
// calls. In this example's local tests, it is mocked via
// LocalTestRunner.RegisterFunction/RegisterDurableFunction rather than
// actually deployed (see handler_test.go and
// docs.aws.amazon.com/durable-execution/testing/api-reference/,
// "Register mock handlers for invoke"). For real-cloud verification, it
// points at examples/chained-invoke-go/inventory-check-target - a
// SECOND, genuinely deployed plain Lambda function (see that target's
// own main.go doc comment) - rather than an unqualified/fictional ARN,
// so operations.Invoke's REAL cross-function invocation and REAL
// failure/retry behavior can be exercised end-to-end against actual AWS
// infrastructure, not just a local mock (see docs/remaining-work.md's
// writeup on this session's Invoke gap 4/8 closure for the full
// verification).
const InventoryCheckFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:inventory-check-target"

// OrderEvent is this example's input shape.
type OrderEvent struct {
	OrderID string `json:"orderId"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	// FailUntilAttempt is forwarded to the target function's own
	// InventoryCheckRequest.FailUntilAttempt field (see that type's doc)
	// - it exists purely to drive examples/chained-invoke-go/
	// inventory-check-target's real-cloud-verification failure
	// simulation deterministically from the TOP-LEVEL request, without
	// any external/persisted counter state. Defaults to 0 (the target
	// always succeeds) when omitted, which is what every pre-existing
	// local-runner test in this file relies on implicitly.
	FailUntilAttempt int `json:"failUntilAttempt,omitempty"`
}

// OrderResult is this example's output shape.
type OrderResult struct {
	OrderID   string `json:"orderId"`
	Available bool   `json:"available"`
}

// InventoryCheckRequest/Response are the target function's input/output
// shapes - a separate Lambda function this example's handler calls via
// operations.Invoke, demonstrating cross-function composition within a
// durable execution.
//
// Attempt/FailUntilAttempt exist specifically to support
// RetryingInventoryCheckHandler's real-cloud verification against a
// SECOND, genuinely deployed Lambda function
// (examples/chained-invoke-go/inventory-check-target) that can be made
// to fail a caller-controlled number of times WITHOUT needing any
// persisted counter state on the target's side (see that target's own
// doc comment for the full "why stateless" rationale) - the target
// simply fails whenever Attempt <= FailUntilAttempt, and this example's
// pre-existing mocked local-runner tests (handler.go's plain handler,
// and countingMockTarget in handler_test.go) don't read either field at
// all, so their behavior is unaffected by this addition.
type InventoryCheckRequest struct {
	SKU              string `json:"sku"`
	Qty              int    `json:"qty"`
	Attempt          int    `json:"attempt,omitempty"`
	FailUntilAttempt int    `json:"failUntilAttempt,omitempty"`
}

type InventoryCheckResponse struct {
	Available bool `json:"available"`
}

// handler demonstrates operations.Invoke: calling another Lambda function
// - here, a (mocked, in this example) inventory-check service - and
// durably awaiting its result. Per the confirmed real-backend flowchart
// (internal SDK Operation Diagrams design doc, "Invoke"), this
// checkpoints CHAINED_INVOKE/CHAINED_INVOKE START once; the backend is
// responsible for actually calling the target function and reporting the
// outcome back, and this SDK only awaits the result - it never manages
// the invocation call itself.
func handler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID, "sku": event.SKU})

	resp, err := operations.Invoke[InventoryCheckRequest, InventoryCheckResponse](
		dc, "check-inventory", InventoryCheckFunctionARN,
		InventoryCheckRequest{SKU: event.SKU, Qty: event.Qty},
	)
	if err != nil {
		return OrderResult{}, fmt.Errorf("order %s: checking inventory: %w", event.OrderID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"available": resp.Available})
	return OrderResult{OrderID: event.OrderID, Available: resp.Available}, nil
}

// MaxInventoryCheckAttempts bounds RetryingInventoryCheckHandler's manual
// retry loop (see that function's doc for why this is a caller-written
// loop, not an SDK-level retry-strategy option).
const MaxInventoryCheckAttempts = 3

// RetryingInventoryCheckHandler demonstrates a durable, replay-safe
// MANUAL retry loop around operations.Invoke - closing
// docs/ts-sdk-examples-comparison.md's "Invoke" gap 4/8
// (with-retry/invoke): "retry strategy applied to an Invoke call."
//
// # Why this is a caller-written loop, not a WithInvokeRetryStrategy option
//
// Confirmed by reading pkg/durable/operations/invoke.go and
// types.ChainedInvokeOptions in full before writing this: unlike
// operations.Step (which has WithStepRetryStrategy and an in-SDK
// retryOrFail loop - see step.go), operations.Invoke has NO
// retry-strategy option of any kind. It checkpoints exactly ONE
// CHAINED_INVOKE/START per call and awaits exactly one terminal outcome
// from the real backend (Succeeded, Failed, TimedOut, or Stopped/
// Cancelled - see execmgr.Manager.WaitForOperation and
// types.OperationStatus.IsTerminal). types.ChainedInvokeOptions itself
// (types/wire.go) only carries FunctionName/TenantID - there is no
// backend-level retry field to configure either. The real Lambda
// Durable Functions backend calls the target function ONCE per
// CHAINED_INVOKE checkpoint and reports back exactly one of those four
// outcomes; it does not itself retry a failed invocation on the SDK's
// behalf.
//
// So "retry scoped to Invoke" can only mean what this function does:
// the DURABLE HANDLER'S OWN CODE calls operations.Invoke again - at a
// genuinely NEW step ID each time, since every operations.Invoke call
// mints and checkpoints its own step ID via c.NextStepID() (see
// invoke.go) and there is no way to re-checkpoint the SAME CHAINED_INVOKE
// operation a second time - if the previous attempt failed, up to a
// caller-chosen bound. This is a manual, but still fully durable and
// replay-safe, retry LOOP: each attempt is its own real, independently
// checkpointed CHAINED_INVOKE operation (never hidden or merged - see
// handler_test.go's assertions on this), and the loop itself re-executes
// identically on replay because operations.Invoke's own replay-skip
// logic (checking c.ExecManager().GetOperation(stepID) before
// checkpointing) makes every attempt already resolved on a prior
// invocation return its checkpointed result/error immediately rather
// than re-invoking the target function - exactly the same replay-safety
// property every other durable operation in this SDK relies on.
//
// Each attempt gets a distinct, deterministic step ID
// ("check-inventory-attempt-1", "check-inventory-attempt-2", ...) rather
// than a shared one, both because operations.Invoke's own design
// requires a new ID per call (see above) and because a deterministic,
// index-derived ID (not e.g. one derived from wall-clock time) is what
// keeps this loop replay-safe: the same attempt number always maps to
// the same step ID across replays, exactly like Map/Parallel's own
// index-derived (not scheduling-order-derived) step IDs (see
// docs/remaining-work.md §1 task 5's bug #3 writeup for the general
// principle this mirrors).
func RetryingInventoryCheckHandler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID, "sku": event.SKU})

	var lastErr error
	for attempt := 1; attempt <= MaxInventoryCheckAttempts; attempt++ {
		stepID := fmt.Sprintf("check-inventory-attempt-%d", attempt)
		resp, err := operations.Invoke[InventoryCheckRequest, InventoryCheckResponse](
			dc, stepID, InventoryCheckFunctionARN,
			InventoryCheckRequest{SKU: event.SKU, Qty: event.Qty, Attempt: attempt, FailUntilAttempt: event.FailUntilAttempt},
		)
		if err == nil {
			dc.Logger().Info("handler completed", map[string]any{"available": resp.Available, "attempt": attempt})
			return OrderResult{OrderID: event.OrderID, Available: resp.Available}, nil
		}

		// Only a genuine, resolved CHAINED_INVOKE terminal failure
		// (*operations.InvokeFailedError - the checkpointed target
		// function actually reported Failed/TimedOut/Stopped/Cancelled)
		// is safe to retry by looping around and calling Invoke again.
		//
		// Any OTHER error shape returned from operations.Invoke is NOT
		// a retryable application-level outcome - most importantly, the
		// unexported errSuspended sentinel (pkg/durable/operations/
		// errors.go), which this SDK's own convention (see
		// docs/subagent-briefing.md, and RunInChildContext's/batch.go's
		// own errors.Is(err, errSuspended) checks in invoke.go/batch.go)
		// requires propagating IMMEDIATELY and UNCHECKPOINTED, never
		// treating as a failure to retry: it means THIS invocation of
		// the Lambda function is suspending (e.g. the real backend
		// hasn't yet reported the target function's outcome and this
		// invocation must end as PENDING, to be resumed later), not that
		// the inventory check itself failed. Looping and calling Invoke
		// AGAIN in that case would be a genuine correctness bug - it
		// would attempt a brand-new CHAINED_INVOKE checkpoint from a
		// goroutine that the SDK runtime has already decided to abandon
		// (see execmgr.Manager.WaitForOperation's doc: once suspension
		// wins the suspend-or-complete race, this goroutine's eventual
		// return value, whatever it is, is simply never read).
		//
		// errSuspended itself is intentionally unexported (no
		// errors.Is-checkable sentinel is part of this SDK's public
		// surface today - confirmed by grepping the whole repo), so
		// this example distinguishes the two cases the same way
		// error-handling-go/run-in-child-context-go already do for
		// other operations: recovering the SPECIFIC structured error
		// type operations.Invoke is documented to return for a real
		// resolved failure (errors.go's InvokeFailedError), rather than
		// retrying on any error whatsoever. An error that is NOT an
		// *operations.InvokeFailedError (nor anything wrapping one) is
		// treated as non-retryable and propagated immediately instead -
		// correctly covering errSuspended (which is a bare sentinel, not
		// an InvokeFailedError) without needing to import or reference
		// it directly.
		var invokeErr *operations.InvokeFailedError
		if !errors.As(err, &invokeErr) {
			return OrderResult{}, err
		}

		dc.Logger().Warn("inventory check attempt failed", map[string]any{"attempt": attempt, "status": string(invokeErr.Status), "error": err.Error()})
		lastErr = err
	}

	return OrderResult{}, fmt.Errorf("order %s: checking inventory: exhausted %d attempts: %w", event.OrderID, MaxInventoryCheckAttempts, lastErr)
}

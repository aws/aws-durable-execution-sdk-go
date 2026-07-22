// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria. Registers a mock for the target function
// (InventoryCheckFunctionARN) via RegisterDurableFunction, matching the
// official cross-SDK Testing API Reference's documented pattern for
// mocking operations.Invoke's target rather than deploying a second
// real Lambda function for local tests.
package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_InventoryAvailable(t *testing.T) {
	runner := dtesting.New(handler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN,
		func(req InventoryCheckRequest) (InventoryCheckResponse, error) {
			if req.SKU != "widget-42" {
				t.Fatalf("expected SKU 'widget-42', got %q", req.SKU)
			}
			return InventoryCheckResponse{Available: true}, nil
		})

	result, err := runner.Run(OrderEvent{OrderID: "order-1", SKU: "widget-42", Qty: 3})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.OrderID != "order-1" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if !out.Available {
		t.Fatal("expected Available=true")
	}

	invokeOp, ok := result.GetOperation("check-inventory")
	if !ok {
		t.Fatal("expected to find a CHAINED_INVOKE operation named 'check-inventory'")
	}
	if invokeOp.GetType() != types.OperationTypeChainedInvoke {
		t.Fatalf("expected CHAINED_INVOKE type, got %s", invokeOp.GetType())
	}
	if invokeOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected invoke SUCCEEDED, got %s", invokeOp.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log - a
	// single top-level CHAINED_INVOKE operation - against a committed
	// golden file. This is the first example in this repo whose
	// operation log includes a CHAINED_INVOKE entry, so the golden file
	// specifically guards against that Type (or the mocked target's
	// resolved Status) silently changing, which a bare result
	// comparison against out.Available would not catch on its own.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/chained-invoke-go/... -run TestHandler_InventoryAvailable
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_InventoryAvailable.history.json")
}

func TestHandler_InventoryUnavailable(t *testing.T) {
	runner := dtesting.New(handler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN,
		func(req InventoryCheckRequest) (InventoryCheckResponse, error) {
			return InventoryCheckResponse{Available: false}, nil
		})

	result, err := runner.Run(OrderEvent{OrderID: "order-2", SKU: "widget-99", Qty: 100})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Available {
		t.Fatal("expected Available=false")
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): a DISTINCT golden file from TestHandler_InventoryAvailable's
	// - even though both scenarios produce a structurally IDENTICAL
	// signature (a single SUCCEEDED top-level CHAINED_INVOKE named
	// "check-inventory", since the difference between "available" and
	// "unavailable" lives entirely in the invoke's returned payload,
	// which EventSignatures deliberately does not capture - see
	// signature.go's doc on what's excluded and why), each meaningfully
	// distinct scenario still gets its own committed file, matching how
	// a golden file is supposed to capture ONE scenario's shape rather
	// than being reused across scenarios whose result differs even when
	// their log shape doesn't. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/chained-invoke-go/... -run TestHandler_InventoryUnavailable
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_InventoryUnavailable.history.json")
}

// -----------------------------------------------------------------------
// RetryingInventoryCheckHandler: manual retry loop around operations.Invoke
// -----------------------------------------------------------------------
//
// Closes docs/ts-sdk-examples-comparison.md's Invoke gap 4/8
// (with-retry/invoke). See RetryingInventoryCheckHandler's own doc
// comment in handler.go for why this is a caller-written retry LOOP
// (each attempt its own genuine, distinctly-checkpointed CHAINED_INVOKE
// operation) rather than an SDK-level retry-strategy option: read in
// full before writing this test, pkg/durable/operations/invoke.go has no
// such option, and types.ChainedInvokeOptions (types/wire.go) has no
// backend-level retry field either - only FunctionName/TenantID.
//
// countingMockTarget builds a mock target function (for
// dtesting.RegisterDurableFunction) that fails the first failCount calls
// with a distinct error each time, then succeeds - simulating a flaky
// downstream service the handler's own manual retry loop compensates
// for. Matches the existing chained-invoke-go tests' established mocking
// pattern (RegisterDurableFunction/a closure implementing the mocked
// business logic) exactly, adding only a call counter, since each
// mocked call here corresponds to a genuinely NEW CHAINED_INVOKE
// operation (a new step ID per attempt - see handler.go), not repeated
// calls against the SAME operation the way a Step's own retry loop
// would.
func countingMockTarget(failCount int) func(InventoryCheckRequest) (InventoryCheckResponse, error) {
	calls := 0
	return func(req InventoryCheckRequest) (InventoryCheckResponse, error) {
		calls++
		if calls <= failCount {
			return InventoryCheckResponse{}, fmt.Errorf("inventory service transient failure (call %d)", calls)
		}
		return InventoryCheckResponse{Available: true}, nil
	}
}

// TestHandler_RetryingInventoryCheck_SucceedsAfterSimulatedFailures proves
// (a) from this task's own requirements: the manual retry loop eventually
// succeeds after N simulated failures (here N=2, succeeding on the 3rd
// and final permitted attempt, MaxInventoryCheckAttempts=3).
func TestHandler_RetryingInventoryCheck_SucceedsAfterSimulatedFailures(t *testing.T) {
	runner := dtesting.New(RetryingInventoryCheckHandler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN, countingMockTarget(2))

	result, err := runner.Run(OrderEvent{OrderID: "order-retry-1", SKU: "widget-42", Qty: 3})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !out.Available {
		t.Fatal("expected Available=true once the retry loop succeeds")
	}

	// (b) from this task's own requirements: each attempt is checkpointed
	// as its own DISTINCT CHAINED_INVOKE operation, not silently
	// merged/hidden - assert all 3 attempts are independently present
	// (attempt-1 and attempt-2 FAILED, attempt-3 SUCCEEDED), each with
	// the CHAINED_INVOKE type and its own distinct operation ID.
	var seenIDs = map[string]bool{}
	for i, wantStatus := range []types.OperationStatus{
		types.OperationStatusFailed, types.OperationStatusFailed, types.OperationStatusSucceeded,
	} {
		attempt := i + 1
		name := fmt.Sprintf("check-inventory-attempt-%d", attempt)
		op, ok := result.GetOperation(name)
		if !ok {
			t.Fatalf("expected a distinct CHAINED_INVOKE operation named %q, found none", name)
		}
		if op.GetType() != types.OperationTypeChainedInvoke {
			t.Fatalf("attempt %d: expected CHAINED_INVOKE type, got %s", attempt, op.GetType())
		}
		if op.GetStatus() != wantStatus {
			t.Fatalf("attempt %d: expected status %s, got %s", attempt, wantStatus, op.GetStatus())
		}
		if seenIDs[op.GetID()] {
			t.Fatalf("attempt %d: operation ID %s was already seen on a prior attempt - attempts must not share an ID", attempt, op.GetID())
		}
		seenIDs[op.GetID()] = true
	}
	if len(seenIDs) != 3 {
		t.Fatalf("expected 3 distinct CHAINED_INVOKE operation IDs (one per attempt), got %d", len(seenIDs))
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): pins the 3-attempt retry-then-succeed shape - 3 top-level
	// CHAINED_INVOKE operations (FAILED, FAILED, SUCCEEDED, in that
	// order), each independently named check-inventory-attempt-N.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/chained-invoke-go/... -run TestHandler_RetryingInventoryCheck_SucceedsAfterSimulatedFailures
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_RetryingInventoryCheck_SucceedsAfterSimulatedFailures.history.json")
}

// TestHandler_RetryingInventoryCheck_ExhaustsRetries proves (c) from this
// task's own requirements: a version of the test where retries are
// exhausted before success correctly fails the WHOLE execution (not just
// the individual CHAINED_INVOKE attempts) - the mock target always fails,
// exceeding MaxInventoryCheckAttempts (3).
func TestHandler_RetryingInventoryCheck_ExhaustsRetries(t *testing.T) {
	runner := dtesting.New(RetryingInventoryCheckHandler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN, countingMockTarget(MaxInventoryCheckAttempts+10))

	result, err := runner.Run(OrderEvent{OrderID: "order-retry-2", SKU: "widget-1", Qty: 1})
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

	// Tighter than a bare non-empty check (docs/ts-sdk-examples-comparison.md
	// gap 5/8): RetryingInventoryCheckHandler's own %w-wrapping (see
	// handler.go's final `fmt.Errorf("order %s: checking inventory:
	// exhausted %d attempts: %w", ...)`) is the OUTER layer here, wrapping
	// the LAST attempt's real *operations.InvokeFailedError as its %w
	// cause - so the flattened message must contain both the handler's
	// own "exhausted N attempts" framing AND, nested inside it via the
	// %w chain, that InvokeFailedError's own OperationError.Error()
	// format ('invoke "name" (id id): <cause>' - see errors.go's
	// OperationError.Error and invoke.go's invokeError, which sets Err to
	// the mocked target's own returned error text verbatim). Asserting
	// on the EXACT wrapped-error substring (not just "some non-empty
	// string") proves the propagated error is genuinely the last
	// attempt's InvokeFailedError, not some other unrelated failure that
	// happens to also produce non-empty text.
	lastAttemptOp, ok := result.GetOperation(fmt.Sprintf("check-inventory-attempt-%d", MaxInventoryCheckAttempts))
	if !ok {
		t.Fatalf("expected to find the last attempt's CHAINED_INVOKE operation %q", fmt.Sprintf("check-inventory-attempt-%d", MaxInventoryCheckAttempts))
	}
	wantExhaustedPrefix := fmt.Sprintf("order order-retry-2: checking inventory: exhausted %d attempts:", MaxInventoryCheckAttempts)
	wantInvokeErrSubstring := fmt.Sprintf("invoke %q (id %s): inventory service transient failure (call %d)", fmt.Sprintf("check-inventory-attempt-%d", MaxInventoryCheckAttempts), lastAttemptOp.GetID(), MaxInventoryCheckAttempts)
	if !strings.HasPrefix(msg, wantExhaustedPrefix) || !strings.Contains(msg, wantInvokeErrSubstring) {
		t.Fatalf("expected the error message to be the handler's exhaustion wrapper around the last attempt's *operations.InvokeFailedError (wanted prefix %q and substring %q), got %q", wantExhaustedPrefix, wantInvokeErrSubstring, msg)
	}

	// Also assert directly on the checkpointed operation's own
	// structured error (Operation.GetError() *types.ErrorObject - a
	// stronger, non-string-matching check than the message-format
	// assertion above), confirming the LAST attempt's own checkpointed
	// ErrorMessage matches the mock target's real returned error text.
	if lastAttemptOp.GetError() == nil {
		t.Fatal("expected the last attempt's CHAINED_INVOKE operation to carry a structured checkpointed Error")
	}
	wantLastAttemptErrorMessage := fmt.Sprintf("inventory service transient failure (call %d)", MaxInventoryCheckAttempts)
	if lastAttemptOp.GetError().ErrorMessage != wantLastAttemptErrorMessage {
		t.Fatalf("expected the last attempt's checkpointed ErrorMessage to be %q, got %q", wantLastAttemptErrorMessage, lastAttemptOp.GetError().ErrorMessage)
	}

	// All MaxInventoryCheckAttempts attempts should have genuinely run
	// (and failed) as their own distinct CHAINED_INVOKE operations -
	// exhaustion must not silently skip an attempt or merge attempts
	// together.
	var seenIDs = map[string]bool{}
	for attempt := 1; attempt <= MaxInventoryCheckAttempts; attempt++ {
		name := fmt.Sprintf("check-inventory-attempt-%d", attempt)
		op, ok := result.GetOperation(name)
		if !ok {
			t.Fatalf("expected a distinct CHAINED_INVOKE operation named %q, found none", name)
		}
		if op.GetStatus() != types.OperationStatusFailed {
			t.Fatalf("attempt %d: expected status FAILED, got %s", attempt, op.GetStatus())
		}
		if seenIDs[op.GetID()] {
			t.Fatalf("attempt %d: operation ID %s was already seen on a prior attempt", attempt, op.GetID())
		}
		seenIDs[op.GetID()] = true
	}
	if len(seenIDs) != MaxInventoryCheckAttempts {
		t.Fatalf("expected %d distinct CHAINED_INVOKE operation IDs, got %d", MaxInventoryCheckAttempts, len(seenIDs))
	}
	if extra, ok := result.GetOperation(fmt.Sprintf("check-inventory-attempt-%d", MaxInventoryCheckAttempts+1)); ok {
		t.Fatalf("expected no attempt beyond the configured bound, but found one: %+v", extra)
	}

	// Event-history/signature assertion: pins the retries-exhausted shape
	// - 3 top-level CHAINED_INVOKE operations, all FAILED, and the whole
	// execution FAILED as a result. Distinct golden file from the
	// succeeds-after-failures scenario above, per this repo's "one
	// golden file per meaningfully distinct scenario" convention (see
	// docs/remaining-work.md §7 task 17c) - the 3rd attempt's status
	// (FAILED here vs. SUCCEEDED there) makes this a genuinely different
	// operation-log shape, not just a different final result over an
	// identical shape.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/chained-invoke-go/... -run TestHandler_RetryingInventoryCheck_ExhaustsRetries
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_RetryingInventoryCheck_ExhaustsRetries.history.json")
}

// TestHandler_RetryingInventoryCheck_SucceedsFirstTry is a baseline
// sanity check: with no simulated failures at all, the loop takes exactly
// one attempt (no wasted/phantom retries when nothing is actually
// failing).
func TestHandler_RetryingInventoryCheck_SucceedsFirstTry(t *testing.T) {
	runner := dtesting.New(RetryingInventoryCheckHandler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN, countingMockTarget(0))

	result, err := runner.Run(OrderEvent{OrderID: "order-retry-3", SKU: "widget-7", Qty: 5})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	if _, ok := result.GetOperation("check-inventory-attempt-1"); !ok {
		t.Fatal("expected attempt-1 to exist")
	}
	if _, ok := result.GetOperation("check-inventory-attempt-2"); ok {
		t.Fatal("expected no attempt-2 when the first attempt succeeds")
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_RetryingInventoryCheck_SucceedsFirstTry.history.json")
}

func TestHandler_InventoryServiceError(t *testing.T) {
	runner := dtesting.New(handler, nil)
	dtesting.RegisterDurableFunction(runner, InventoryCheckFunctionARN,
		func(req InventoryCheckRequest) (InventoryCheckResponse, error) {
			return InventoryCheckResponse{}, errors.New("inventory service unavailable")
		})

	result, err := runner.Run(OrderEvent{OrderID: "order-3", SKU: "widget-1", Qty: 1})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	invokeOp, ok := result.GetOperation("check-inventory")
	if !ok {
		t.Fatal("expected to find a CHAINED_INVOKE operation named 'check-inventory'")
	}
	if invokeOp.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected invoke FAILED, got %s", invokeOp.GetStatus())
	}

	// Tighter than a bare non-empty check (docs/ts-sdk-examples-comparison.md
	// gap 5/8): handler's own %w-wrapping ("order %s: checking
	// inventory: %w" - see handler.go) wraps the real
	// *operations.InvokeFailedError invokeError produced for this
	// checkpointed CHAINED_INVOKE failure, whose OperationError.Error()
	// format is 'invoke "name" (id id): <cause>' (errors.go/invoke.go) -
	// <cause> here is exactly the mocked target's own returned error
	// text ("inventory service unavailable"), since invokeError sets
	// Err to fmt.Errorf("%s", ...ChainedInvokeDetails.Error.ErrorMessage)
	// verbatim. Asserting the EXACT wrapped format (not a vague
	// substring) proves the propagated message really is this
	// InvokeFailedError, not merely some other error that happens to
	// mention similar words.
	wantMsg := fmt.Sprintf("order order-3: checking inventory: invoke %q (id %s): inventory service unavailable", "check-inventory", invokeOp.GetID())
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the wrapped *operations.InvokeFailedError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the checkpointed operation's own
	// structured error (Operation.GetError() *types.ErrorObject) as a
	// stronger, non-string-matching check.
	if invokeOp.GetError() == nil {
		t.Fatal("expected the 'check-inventory' CHAINED_INVOKE operation to carry a structured checkpointed Error")
	}
	if invokeOp.GetError().ErrorMessage != "inventory service unavailable" {
		t.Fatalf("expected the checkpointed ErrorMessage to be %q, got %q", "inventory service unavailable", invokeOp.GetError().ErrorMessage)
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): a DISTINCT golden file from the two success-path scenarios
	// above, since a FAILED CHAINED_INVOKE is a meaningfully different
	// operation-log shape (Status: FAILED, not SUCCEEDED) that those
	// golden files would correctly reject if reused here - confirming
	// the failure path's own shape is independently pinned, not just
	// inferred from the success-path assertions.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/chained-invoke-go/... -run TestHandler_InventoryServiceError
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_InventoryServiceError.history.json")
}

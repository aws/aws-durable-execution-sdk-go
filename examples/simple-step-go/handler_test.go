// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner (and, eventually, the cloud runner - not yet implemented, see
// that document's task 17a) rather than being verified only by one-off
// manual `aws lambda invoke` calls.
package main

import (
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_CreatesGreeting(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "go-local-test", Message: "Local Runner"})
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
	if out.OrderID != "go-local-test" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if out.Greeting != "Hello, Local Runner!" {
		t.Fatalf("expected greeting 'Hello, Local Runner!', got %q", out.Greeting)
	}

	step, ok := result.GetOperation("create-greeting")
	if !ok {
		t.Fatal("expected to find the 'create-greeting' step operation in the history")
	}
	if step.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", step.GetType())
	}
	if step.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected step SUCCEEDED, got %s", step.GetStatus())
	}

	stepResult, err := dtesting.StepResult[string](step)
	if err != nil {
		t.Fatalf("StepResult: %v", err)
	}
	if stepResult != "Hello, Local Runner!" {
		t.Fatalf("expected checkpointed step result 'Hello, Local Runner!', got %q", stepResult)
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log
	// (type/subType/status/name for every checkpointed operation, in
	// order) against a committed golden file, catching accidental
	// non-determinism or unintended operation-log changes that the
	// result-only assertions above would miss entirely (e.g. an extra
	// step silently added, or this step's SubType changing). Regenerate
	// with: UPDATE_GOLDEN=1 go test ./examples/simple-step-go/... -run TestHandler_CreatesGreeting
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_CreatesGreeting.history.json")
}

// TestHandler_FailsValidationBeforeAnyOperation closes
// docs/ts-sdk-examples-comparison.md's "Error handling / determinism"
// gap 6/8 (the TS SDK's handler-error example): a handler that fails
// immediately, before calling any durable operation at all, must (a)
// fail the whole execution, and (b) leave result.GetOperations() at
// length 0 - proving no operation was ever checkpointed. This is the
// Go-side analog of the TS test's own core assertion,
// expect(result.getOperations()).toHaveLength(0), applied against
// ValidatingHandler (handler.go) rather than the plain handler used by
// TestHandler_CreatesGreeting above, so that test's own existing
// single-operation scenario is left completely unchanged.
func TestHandler_FailsValidationBeforeAnyOperation(t *testing.T) {
	runner := dtesting.New(ValidatingHandler, nil)

	// An empty Message fails ValidatingHandler's own validation check
	// (handler.go) before operations.Step("create-greeting", ...) is
	// ever reached.
	result, err := runner.Run(OrderEvent{OrderID: "go-local-test-invalid", Message: ""})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// (a) the whole execution FAILS - matching the TS example's
	// structured-error-equality assertion in spirit (Go has no directly
	// portable stackTrace/errorType shape to compare byte-for-byte
	// against - see docs/ts-sdk-examples-comparison.md's own "Notes on
	// TS assertion rigor" section - but the failure itself, and the
	// error message's content, are asserted below).
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
	msg, ok := result.GetError()
	if !ok {
		t.Fatal("expected GetError to report an error for a FAILED execution")
	}
	if !strings.Contains(msg, "validation failed") || !strings.Contains(msg, "message") {
		t.Fatalf("expected the error message to reflect the validation failure, got %q", msg)
	}

	// (b) result.GetOperations() reports zero NON-EXECUTION operations -
	// the exact assertion this gap is named for: PROOF that no durable
	// operation (STEP, WAIT, CALLBACK, CHAINED_INVOKE, CONTEXT -
	// anything the handler's OWN code would have caused) was ever
	// checkpointed before the handler's validation error returned,
	// matching the TS handler-error example's
	// expect(result.getOperations()).toHaveLength(0) in substance.
	//
	// One real, honest API-shape divergence from the TS SDK's own
	// getOperations(), found while writing this exact test and
	// documented here rather than silently worked around: this Go SDK's
	// TestResult.GetOperations() (result.go) unconditionally includes
	// the root EXECUTION operation itself (confirmed by running this
	// test before adding the filter below - it failed with length 1,
	// listing exactly {Type:EXECUTION, ID:exec-root, Status:STARTED}).
	// That operation is seeded by the runner before the handler ever
	// starts (see invocation_input.go's executionRootID) - it is not
	// something ValidatingHandler's own code chose to run, and every
	// OTHER place in this SDK that draws the same "operations the
	// handler caused" distinction (testing.EventSignatures, see
	// signature.go's own doc and its explicit
	// `if op.Type == types.OperationTypeExecution { continue }` check)
	// already excludes it for exactly this reason. TestResult.GetOperations'
	// own doc comment does not yet make this exclusion explicit the way
	// EventSignatures' does - this is a real, narrow documentation gap in
	// result.go, out of scope to fix in this task (a shared-package
	// change with no test currently relying on the exclusion), but
	// worth flagging: EventSignatures is authoritative for "operations
	// the handler's own code caused," GetOperations is not. Filtering
	// EXECUTION out below makes this test measure the same thing the TS
	// example's assertion measures, rather than either quietly passing
	// for the wrong reason or failing on an irrelevant technicality.
	var nonExecutionOps []dtesting.Operation
	for _, op := range result.GetOperations() {
		if op.GetType() != types.OperationTypeExecution {
			nonExecutionOps = append(nonExecutionOps, op)
		}
	}
	if len(nonExecutionOps) != 0 {
		t.Fatalf("expected zero checkpointed durable operations before the handler-level failure, got %d: %+v", len(nonExecutionOps), nonExecutionOps)
	}

	// Belt-and-suspenders: also confirm the specific "create-greeting"
	// step this handler would otherwise have run was never checkpointed
	// at all (as opposed to merely absent from the top-level slice above
	// for some other, GetOperations()-specific reason).
	if _, found := result.GetOperation("create-greeting"); found {
		t.Fatal("expected the create-greeting step to never have been checkpointed, but found it")
	}

	// Event-history/signature golden-file assertion, for consistency
	// with every other terminal-state test in this repo (see
	// docs/remaining-work.md §7 task 17c). For this zero-operation
	// scenario, testing.EventSignatures necessarily returns an empty
	// slice (there is nothing to walk - see that function's own doc:
	// it only ever visits operations present in the result, and this
	// result has none), so the golden file itself is the trivial `[]`.
	// Committing it anyway - rather than skipping the call on the
	// reasoning that "there's nothing to diff beyond emptiness" - keeps
	// this test structurally consistent with every other example test in
	// this repo (every one of which calls AssertEventSignatures
	// unconditionally once it reaches a terminal state), and still has
	// real diagnostic value going forward: if a future change to
	// ValidatingHandler or the SDK's own checkpoint-on-error paths ever
	// caused an operation to be checkpointed BEFORE the validation
	// error is returned (a real regression - exactly what this whole
	// gap exists to catch), this golden file would immediately catch it
	// as a non-empty-vs-empty mismatch, exactly like any other golden
	// file catches an unexpected operation-log change.
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_FailsValidationBeforeAnyOperation.history.json")
}

// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner (and, eventually, the cloud runner - not yet implemented, see
// that document's task 17a), asserting on the actual checkpointed
// operations, not just the final result.
package main

import (
	"fmt"
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_ProcessesOrderThroughChildContext(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "go-child-ctx-test", Amount: 42.50})
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
	if out.OrderID != "go-child-ctx-test" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if out.PaymentReceipt != "receipt-go-child-ctx-test-42.50" {
		t.Fatalf("expected payment receipt 'receipt-go-child-ctx-test-42.50', got %q", out.PaymentReceipt)
	}
	if out.ShippingLabelID != "label-go-child-ctx-test" {
		t.Fatalf("expected shipping label 'label-go-child-ctx-test', got %q", out.ShippingLabelID)
	}

	// Assert on the CONTEXT operation itself: type, status, and its own
	// checkpointed result (the "charge-card" step's return value, since
	// that's what the child function returns).
	paymentCtx, ok := result.GetOperation("payment")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'payment'")
	}
	if paymentCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected 'payment' to be a CONTEXT operation, got %s", paymentCtx.GetType())
	}
	if paymentCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'payment' context SUCCEEDED, got %s", paymentCtx.GetStatus())
	}
	ctxResult, err := dtesting.ContextResult[string](paymentCtx)
	if err != nil {
		t.Fatalf("ContextResult: %v", err)
	}
	if ctxResult != "receipt-go-child-ctx-test-42.50" {
		t.Fatalf("expected context result 'receipt-go-child-ctx-test-42.50', got %q", ctxResult)
	}

	// Assert on the two steps NESTED inside the child context - they must
	// appear as the context's direct children, not as top-level
	// operations, and must be independently inspectable.
	children := result.GetChildOperations(paymentCtx.GetID())
	if len(children) != 2 {
		t.Fatalf("expected 2 operations nested under 'payment', got %d", len(children))
	}
	childNames := map[string]bool{}
	for _, child := range children {
		childNames[child.GetName()] = true
		if child.GetType() != types.OperationTypeStep {
			t.Fatalf("expected child operation %q to be a STEP, got %s", child.GetName(), child.GetType())
		}
		if child.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected child operation %q to be SUCCEEDED, got %s", child.GetName(), child.GetStatus())
		}
	}
	if !childNames["validate-amount"] || !childNames["charge-card"] {
		t.Fatalf("expected child operations 'validate-amount' and 'charge-card', got %v", childNames)
	}

	// Assert on the SIBLING step at the root context - it must exist as
	// its own top-level operation, entirely independent of the "payment"
	// child context.
	shippingStep, ok := result.GetOperation("create-shipping-label")
	if !ok {
		t.Fatal("expected to find a top-level STEP operation named 'create-shipping-label'")
	}
	if shippingStep.GetType() != types.OperationTypeStep {
		t.Fatalf("expected 'create-shipping-label' to be a STEP operation, got %s", shippingStep.GetType())
	}
	stepResult, err := dtesting.StepResult[string](shippingStep)
	if err != nil {
		t.Fatalf("StepResult: %v", err)
	}
	if stepResult != "label-go-child-ctx-test" {
		t.Fatalf("expected shipping step result 'label-go-child-ctx-test', got %q", stepResult)
	}

	// The root-level shipping step must NOT be nested under the
	// 'payment' context.
	for _, child := range children {
		if child.GetName() == "create-shipping-label" {
			t.Fatal("expected 'create-shipping-label' to be a sibling of 'payment', not nested under it")
		}
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log - a
	// CONTEXT/RUN_IN_CHILD_CONTEXT operation nesting two STEP children,
	// followed by a sibling top-level STEP - against a committed golden
	// file. This is exactly the kind of structural assertion this
	// test's existing GetChildOperations-based checks above already
	// verify by hand, but doing it via a single golden-file comparison
	// also catches the FULL log's shape (ordering, nesting depth,
	// SubType) in one assertion rather than requiring a bespoke check
	// per structural property, and would catch a regression in an
	// UNRELATED part of the log this test doesn't otherwise inspect (a
	// property especially valuable for RunInChildContext specifically,
	// since it's the first example in this repo with real nesting).
	// Regenerate with: UPDATE_GOLDEN=1 go test ./examples/run-in-child-context-go/... -run TestHandler_ProcessesOrderThroughChildContext
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ProcessesOrderThroughChildContext.history.json")
}

// TestHandler_InvalidAmount verifies that a validation failure inside the
// child context fails the whole context (and thus the execution), and
// that the failure is visible on both the context operation and its
// nested step.
func TestHandler_InvalidAmount(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "bad-order", Amount: 0})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// validate-amount returns (false, nil) for a non-positive amount
	// rather than an error, so this handler as written actually proceeds
	// to charge-card regardless - this test documents that current
	// behavior (the example intentionally keeps its validation simple;
	// a real implementation would return an error from validate-amount
	// and this test would instead assert FAILED). Confirms the
	// child-context machinery doesn't accidentally short-circuit on a
	// falsy step result.
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED (validate-amount's false result is not itself an error), got %s (%s)", result.GetStatus(), msg)
	}

	validateStep, ok := result.GetOperation("validate-amount")
	if !ok {
		t.Fatal("expected to find the 'validate-amount' step operation")
	}
	valid, err := dtesting.StepResult[bool](validateStep)
	if err != nil {
		t.Fatalf("StepResult: %v", err)
	}
	if valid {
		t.Fatal("expected validate-amount to have returned false for a zero amount")
	}
}

// TestHandler_PaymentStepFails verifies the gap identified in
// docs/ts-sdk-examples-comparison.md's RunInChildContext section
// (run-in-child-context/with-failing-step): a genuinely FAILING nested
// step (charge-card returning a real error, errCardChargeDeclined - see
// handler.go's doc for why a negative amount triggers a real error here,
// as opposed to validate-amount's own zero-amount falsy-but-non-error
// case covered by TestHandler_InvalidAmount above) propagates failure up
// through the enclosing RunInChildContext, and that BOTH the nested STEP
// operation AND the enclosing CONTEXT/RUN_IN_CHILD_CONTEXT operation
// itself end up checkpointed FAILED - not just the nested step.
func TestHandler_PaymentStepFails(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "declined-order", Amount: -10})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// (a) The whole execution FAILS.
	if result.GetStatus() != types.ExecutionStatusFailed {
		msg, _ := result.GetError()
		t.Fatalf("expected FAILED once charge-card genuinely fails, got %s (%s)", result.GetStatus(), msg)
	}

	// (b) errors.As-style inspection: the real Go error value produced by
	// operations.RunInChildContext (a *operations.ChildContextFailedError)
	// is only observable INSIDE the same invocation that produced it -
	// see handler.go's own errors.As(err, &childErr) inspection, which
	// enriches the propagated error message with the recovered
	// ChildContextFailedError's ID/Reconstructed fields before it crosses
	// the durable.WithDurableExecution boundary. That boundary flattens
	// every error to a plain string (types.DurableExecutionOutput.
	// ErrorMessage - see durable.go's outcomeToResponse and
	// RunInChildContext's own checkpointed
	// types.OperationError{ErrorMessage: err.Error()} for the FAIL
	// checkpoint itself), and testing.TestResult.GetError() only ever
	// exposes that flattened string (errorMessage *string in result.go) -
	// there is no richer, error-value-returning accessor on the public
	// LocalTestRunner/TestResult API to errors.As against from outside
	// the handler (confirmed by reading result.go/runner.go in full: the
	// only two exported error-surfacing points are GetError() string,true
	// and the checkpointed Operation's own GetError() *types.ErrorObject,
	// both plain data, not a live Go error chain). This matches
	// error-handling-go's identical, pre-existing pattern
	// (TestHandler_ChargeExhaustsRetries) of asserting on the
	// errors.As-enriched message's CONTENT via GetError()'s string
	// surface, rather than re-running errors.As from the test itself -
	// which is the only option available from the testing package's
	// public API, per this test's own verification of both alternatives.
	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}
	paymentCtxForID, ok := result.GetOperation("payment")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'payment' even though it failed")
	}
	wantIDSubstring := fmt.Sprintf("context id %s, reconstructed=false", paymentCtxForID.GetID())
	if !strings.Contains(msg, "payment child context failed") || !strings.Contains(msg, wantIDSubstring) {
		t.Fatalf("expected the error message to reflect handler.go's errors.As-inspected ChildContextFailedError fields (looking for %q), got %q", wantIDSubstring, msg)
	}
	if !strings.Contains(msg, errCardChargeDeclined.Error()) {
		t.Fatalf("expected the error message to still surface the original charge-card cause %q (via the wrapped %%w chain), got %q", errCardChargeDeclined.Error(), msg)
	}

	// (c) The nested STEP operation itself is checkpointed FAILED.
	chargeStep, ok := result.GetOperation("charge-card")
	if !ok {
		t.Fatal("expected to find the 'charge-card' step operation even though it failed")
	}
	if chargeStep.GetType() != types.OperationTypeStep {
		t.Fatalf("expected 'charge-card' to be a STEP operation, got %s", chargeStep.GetType())
	}
	if chargeStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected 'charge-card' step to be checkpointed FAILED, got %s", chargeStep.GetStatus())
	}
	if chargeStep.GetStepDetails() == nil || chargeStep.GetStepDetails().Error == nil {
		t.Fatal("expected 'charge-card' step's checkpointed StepDetails to carry a real Error")
	}
	if !strings.Contains(chargeStep.GetStepDetails().Error.ErrorMessage, errCardChargeDeclined.Error()) {
		t.Fatalf("expected the checkpointed step error to contain %q, got %q", errCardChargeDeclined.Error(), chargeStep.GetStepDetails().Error.ErrorMessage)
	}

	// validate-amount, which ran and succeeded BEFORE charge-card failed,
	// must still be checkpointed SUCCEEDED - only charge-card and the
	// enclosing context should show the failure, not a step that already
	// completed successfully before the failure occurred.
	validateStep, ok := result.GetOperation("validate-amount")
	if !ok {
		t.Fatal("expected to find the 'validate-amount' step operation")
	}
	if validateStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'validate-amount' to remain SUCCEEDED (it ran and completed before charge-card failed), got %s", validateStep.GetStatus())
	}

	// (d) The enclosing CONTEXT/RUN_IN_CHILD_CONTEXT operation is ALSO
	// checkpointed FAILED - not just its nested child. This is the exact
	// property docs/ts-sdk-examples-comparison.md's gap description
	// calls out: "a step inside the child context fails, propagating
	// failure to the whole context."
	paymentCtx, ok := result.GetOperation("payment")
	if !ok {
		t.Fatal("expected to find the 'payment' CONTEXT operation even though it failed")
	}
	if paymentCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected 'payment' to be a CONTEXT operation, got %s", paymentCtx.GetType())
	}
	if paymentCtx.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected the ENCLOSING 'payment' CONTEXT operation to be checkpointed FAILED (not just its nested charge-card step), got %s", paymentCtx.GetStatus())
	}
	if paymentCtx.GetError() == nil {
		t.Fatal("expected the 'payment' CONTEXT operation's checkpointed Error to be populated")
	}
	if !strings.Contains(paymentCtx.GetError().ErrorMessage, errCardChargeDeclined.Error()) {
		t.Fatalf("expected the CONTEXT operation's checkpointed error to contain the original cause %q, got %q", errCardChargeDeclined.Error(), paymentCtx.GetError().ErrorMessage)
	}

	// charge-card must still be reachable as a child of the now-FAILED
	// 'payment' context, exactly like the happy-path test's structural
	// assertions above - failure doesn't remove the parent/child
	// relationship.
	children := result.GetChildOperations(paymentCtx.GetID())
	foundChargeCard := false
	for _, child := range children {
		if child.GetName() == "charge-card" {
			foundChargeCard = true
		}
	}
	if !foundChargeCard {
		t.Fatalf("expected 'charge-card' to remain a child of the failed 'payment' context, got children %+v", children)
	}

	// The sibling root-level "create-shipping-label" step must NEVER
	// have run at all: the handler returns immediately once
	// RunInChildContext itself fails, before reaching that step.
	if _, ok := result.GetOperation("create-shipping-label"); ok {
		t.Fatal("expected 'create-shipping-label' to have never run, since the enclosing payment context failed first")
	}

	// (e) Event-history/signature golden-file assertion - the new
	// failure-propagation shape: a CONTEXT/RUN_IN_CHILD_CONTEXT operation
	// that itself ends FAILED, nesting one SUCCEEDED step
	// (validate-amount) and one FAILED step (charge-card), with NO
	// sibling create-shipping-label step at all (since the handler never
	// reaches it) - a genuinely different shape from
	// TestHandler_ProcessesOrderThroughChildContext's all-succeeded
	// golden file, per this repo's "one golden file per distinct
	// scenario" convention (docs/remaining-work.md §7 task 17c).
	// Regenerate with: UPDATE_GOLDEN=1 go test ./examples/run-in-child-context-go/... -run TestHandler_PaymentStepFails
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_PaymentStepFails.history.json")
}

// TestHandler_ReconciliationUsesCustomChildSerdes closes
// docs/ts-sdk-examples-comparison.md's gap 8/8: it exercises handler with
// OrderEvent.UseCustomChildSerdes: true (customSerdesHandlerScenario in
// handler.go), which applies operations.WithChildSerdes
// (screamingSnakeCaseSerdes, the SAME custom Serdes implementation
// examples/custom-config-go's own TestHandler_ChecksSnapshotUsesCustomSerdes
// proves at Step scope, duplicated here rather than reinvented - see
// handler.go's doc) to a RunInChildContext call's OWN checkpointed result,
// not to any Step nested inside it.
//
// Following this repo's own established rigor for proving a custom
// Serdes genuinely ran (custom-config-go's identical pattern, cited by
// this gap's own description): this test asserts on the RAW checkpointed
// payload STRING for the "reconciliation" CONTEXT operation, not just the
// deserialized Go value returned to the handler. A round-tripping Go
// value alone would be insufficient proof - the DEFAULT JSON Serdes would
// ALSO successfully round-trip the exact same childSnapshot struct, so
// only the wire-level SCREAMING_SNAKE_CASE re-keying is genuine evidence
// WithChildSerdes actually ran, as opposed to being silently ignored by
// RunInChildContext (e.g. if a future refactor accidentally always used
// utils.DefaultSerdes() instead of cfg.serdes for the CONTEXT/SUCCEED
// checkpoint - see invoke.go's RunInChildContext implementation, which
// this test guards against regressing).
func TestHandler_ReconciliationUsesCustomChildSerdes(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "recon-order-1", Amount: 123.45, UseCustomChildSerdes: true})
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
	if out.OrderID != "recon-order-1" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if out.ReconciledTotal != 123.45 {
		t.Fatalf("expected ReconciledTotal 123.45, got %v", out.ReconciledTotal)
	}

	// (a) The CONTEXT operation itself: type, status, and its own
	// checkpointed result, deserialized back through the SAME custom
	// Serdes on the read path (proving the round trip works end-to-end,
	// not just that the wire bytes look different - mirroring
	// custom-config-go's TestHandler_ChecksSnapshotUsesCustomSerdes's own
	// closing comment).
	reconCtx, ok := result.GetOperation("reconciliation")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'reconciliation'")
	}
	if reconCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected 'reconciliation' to be a CONTEXT operation, got %s", reconCtx.GetType())
	}
	if reconCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'reconciliation' context SUCCEEDED, got %s", reconCtx.GetStatus())
	}
	ctxResult, err := dtesting.ContextResult[childSnapshot](reconCtx)
	if err != nil {
		t.Fatalf("ContextResult: %v", err)
	}
	if ctxResult.ReconciledTotal != 123.45 {
		t.Fatalf("expected context result ReconciledTotal 123.45, got %v", ctxResult.ReconciledTotal)
	}
	if ctxResult.AuditNote != "reconciled order recon-order-1" {
		t.Fatalf("expected context result AuditNote 'reconciled order recon-order-1', got %q", ctxResult.AuditNote)
	}

	// (b) THE KEY ASSERTION: the RAW checkpointed wire payload for the
	// 'reconciliation' CONTEXT operation must genuinely be
	// SCREAMING_SNAKE_CASE JSON - proving WithChildSerdes applied at the
	// CONTEXT level, exactly as rigorously as custom-config-go's own test
	// proves it at Step level. If this string instead contained the
	// original snake_case keys ("reconciled_total"/"audit_note"),
	// WithChildSerdes was silently not applied by RunInChildContext.
	details := reconCtx.GetContextDetails()
	if details == nil || details.Result == nil {
		t.Fatal("expected the reconciliation context to have a recorded checkpointed result")
	}
	raw := *details.Result
	if !strings.Contains(raw, `"RECONCILED_TOTAL"`) {
		t.Fatalf("expected the checkpointed CONTEXT payload to contain the rekeyed field \"RECONCILED_TOTAL\" (proving screamingSnakeCaseSerdes ran at CONTEXT scope via WithChildSerdes), got %q", raw)
	}
	if !strings.Contains(raw, `"AUDIT_NOTE"`) {
		t.Fatalf("expected the checkpointed CONTEXT payload to contain the rekeyed field \"AUDIT_NOTE\" (proving screamingSnakeCaseSerdes ran at CONTEXT scope via WithChildSerdes), got %q", raw)
	}
	if strings.Contains(raw, `"reconciled_total"`) || strings.Contains(raw, `"audit_note"`) {
		t.Fatalf("expected the checkpointed CONTEXT payload to NOT contain the original snake_case keys (screamingSnakeCaseSerdes should have rekeyed them), got %q", raw)
	}

	// (c) The NESTED step ("compute-reconciled-total") must remain in the
	// DEFAULT Serdes format - proving the custom Serdes is genuinely
	// scoped to the CONTEXT's own result (via WithChildSerdes), not
	// accidentally leaking into every operation nested inside the child
	// context (which would indicate WithChildSerdes was wired at the
	// wrong layer - e.g. propagated into the child dcontext.Context
	// itself rather than applied only at RunInChildContext's own
	// CONTEXT/SUCCEED checkpoint in invoke.go). The step returns a bare
	// float64 (no object keys to rekey either way), so its checkpointed
	// payload is just the plain JSON number - this asserts it is NOT
	// wrapped or transformed into anything SCREAMING_SNAKE_CASE-shaped.
	computeStep, ok := result.GetOperation("compute-reconciled-total")
	if !ok {
		t.Fatal("expected to find the 'compute-reconciled-total' step operation")
	}
	stepDetails := computeStep.GetStepDetails()
	if stepDetails == nil || stepDetails.Result == nil {
		t.Fatal("expected the compute-reconciled-total step to have a recorded checkpointed result")
	}
	stepRaw := *stepDetails.Result
	if stepRaw != "123.45" {
		t.Fatalf("expected the nested step's checkpointed payload to be the plain default-Serdes JSON number \"123.45\" (unaffected by the CONTEXT-level custom Serdes), got %q", stepRaw)
	}
	stepVal, err := dtesting.StepResult[float64](computeStep)
	if err != nil {
		t.Fatalf("StepResult: %v", err)
	}
	if stepVal != 123.45 {
		t.Fatalf("expected the nested step's deserialized result 123.45, got %v", stepVal)
	}

	// (d) Structural check: the nested step is a child of the
	// 'reconciliation' context, matching the pre-existing 'payment'
	// example's own structural assertions above.
	children := result.GetChildOperations(reconCtx.GetID())
	if len(children) != 1 || children[0].GetName() != "compute-reconciled-total" {
		t.Fatalf("expected exactly 1 child ('compute-reconciled-total') nested under 'reconciliation', got %+v", children)
	}

	// (e) Event-history/signature golden-file assertion, per this
	// repo's task 17c convention.
	// Regenerate with: UPDATE_GOLDEN=1 go test ./examples/run-in-child-context-go/... -run TestHandler_ReconciliationUsesCustomChildSerdes
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ReconciliationUsesCustomChildSerdes.history.json")
}

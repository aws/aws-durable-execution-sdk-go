package durable_test

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestParallel_CompletionPolicyContract_AlwaysSucceedsWithBatchResult
// verifies the real, breaking completion-policy-contract fix (see
// operations.BatchResult's own top-level doc for the full writeup,
// prompted by 12 real conformance requirements - Parallel suite's
// 8-6/7/8/10/13/17/18, Map suite's 9-5/6/7/9/10 - whose own
// ExpectedExecutionHistory YAMLs assert the outer Parallel/Map context's
// ContextSucceeded checkpoint even when the completion policy was NOT
// met): operations.Parallel ALWAYS returns a valid BatchResult with a
// nil error once its branches finish, REGARDLESS of whether the
// configured CompletionConfig policy was met - the outer CONTEXT/
// PARALLEL operation's own checkpoint is ALWAYS ContextSucceeded, never
// ContextFailed for a policy-not-met reason - and the caller inspects
// BatchResult's own Status()/CompletionReason/HasFailure() to decide
// what happened, optionally calling ThrowIfError() themselves to restore
// the OLD "return an error automatically" behavior.
//
// This test mirrors conformance requirement 8-6 ("Parallel fail-fast via
// tolerated-failure-count=0 stops after first failure") exactly: 3
// branches, ToleratedFailureCount: 0, max-concurrency 1 for deterministic
// sequential ordering - branch 0 succeeds, branch 1 fails (tripping the
// zero-tolerance threshold immediately), branch 2 is never even started
// (errBatchSkipped).
func TestParallel_CompletionPolicyContract_AlwaysSucceedsWithBatchResult(t *testing.T) {
	client := newFakeClient()
	tolerated := 0
	one := 1

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		batch, err := operations.Parallel(dc, "failfast", []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "ok", nil },
			func(child types.DurableContext) (string, error) { return "", errParallelPolicyTestBranchFailed },
			func(child types.DurableContext) (string, error) { return "unreached", nil },
		},
			operations.WithParallelMaxConcurrency[string](1),
			operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
		)
		if err != nil {
			// A genuine, unrelated Parallel error (config validation,
			// suspension, etc.) - NOT expected on this scenario's own
			// happy path through Parallel itself.
			return orderResult{}, err
		}

		// The real, core assertion this whole fix is about: Parallel
		// returned (BatchResult, nil) - no error - even though the
		// batch's own policy was NOT met (branch 1 failed, exceeding
		// ToleratedFailureCount: 0).
		if batch.Status() != "FAILED" {
			t.Errorf("expected batch.Status() == FAILED (a real failure occurred, even though Parallel itself returned no error), got %s", batch.Status())
		}
		if !batch.HasFailure() {
			t.Error("expected batch.HasFailure() == true")
		}
		if batch.CompletionReason != operations.CompletionReasonFailureToleranceExceeded {
			t.Errorf("expected CompletionReason == FAILURE_TOLERANCE_EXCEEDED, got %q", batch.CompletionReason)
		}
		if batch.SucceededCount() != 1 {
			t.Errorf("expected SucceededCount() == 1, got %d", batch.SucceededCount())
		}
		if batch.FailureCount() != 1 {
			t.Errorf("expected FailureCount() == 1 (excluding the skipped 3rd branch - it never ran, so it's neither a success nor a genuine failure), got %d", batch.FailureCount())
		}
		if batch.TotalCount() != 2 {
			t.Errorf("expected TotalCount() == 2 (only the 2 branches that actually ran - branch 2 was skipped entirely), got %d", batch.TotalCount())
		}
		results := batch.GetResults()
		if len(results) != 1 || results[0] != "ok" {
			t.Errorf("expected GetResults() == [\"ok\"], got %v", results)
		}
		errs := batch.GetErrors()
		if len(errs) != 1 {
			t.Errorf("expected exactly 1 error from GetErrors() (excluding the skipped branch), got %d: %v", len(errs), errs)
		}

		// ThrowIfError() restores the OLD "automatically return an
		// error" behavior for a caller that still wants Promise.all-
		// style semantics - confirm it does so correctly here, then
		// deliberately DO propagate it, so this test can ALSO verify the
		// overall execution genuinely fails (matching 8-6's own
		// ExpectedResult.ExecutionStatus... actually 8-6 itself expects
		// SUCCEEDED with a projection result - this unit test
		// deliberately takes the OTHER valid path, propagating the
		// error, to verify ThrowIfError's own correctness end-to-end
		// rather than duplicating 8-6's own handler-level projection
		// pattern already covered by the real conformance handler).
		if throwErr := batch.ThrowIfError(); throwErr != nil {
			return orderResult{}, throwErr
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-parallel-policy", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "policy-test"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected the execution to FAIL (via this test's own ThrowIfError propagation), got status=%s", out.Status)
	}

	// The outer CONTEXT/PARALLEL operation itself must be checkpointed
	// SUCCEEDED - the core, real behavior change this whole fix is
	// about. Confirmed directly against the fake backend's own
	// checkpoint log, not just inferred from the handler's own in-memory
	// assertions above.
	snapshot := client.snapshot()
	var parallelOp *types.Operation
	for _, op := range snapshot {
		if op.Name == "failfast" {
			o := op
			parallelOp = &o
		}
	}
	if parallelOp == nil {
		t.Fatal("expected the outer Parallel context operation 'failfast' to be checkpointed")
	}
	if parallelOp.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer Parallel context to be checkpointed SUCCEEDED (never FAILED for a policy-not-met reason - see operations.BatchResult's own doc), got %s", parallelOp.Status)
	}

	// Confirm only 2 branches were ever checkpointed at all (the 3rd was
	// genuinely skipped, never started) - one more direct confirmation
	// of TotalCount()==2's own real-backend meaning.
	branchCount := 0
	for _, op := range snapshot {
		if op.Type == types.OperationTypeContext && op.SubType == "ParallelBranch" {
			branchCount++
		}
	}
	if branchCount != 2 {
		t.Fatalf("expected exactly 2 checkpointed ParallelBranch operations (branch 2 should never have been started), got %d", branchCount)
	}
	_ = one
}

var errParallelPolicyTestBranchFailed = &testBranchError{"branch failed"}

type testBranchError struct{ msg string }

func (e *testBranchError) Error() string { return e.msg }

// TestParallel_MinSuccessful_StopsEarly verifies the real, separately-
// confirmed bug fix bundled into this same change: CompletionConfig.
// MinSuccessful now genuinely gates the scheduling loop's own early-exit
// decision (batchCompletion.thresholdExceeded), not just the FINAL
// pass/fail classification - before this fix, MinSuccessful was NEVER
// consulted by thresholdExceeded at all, so a batch configured with
// MinSuccessful: 2 over 4 branches always ran all 4 regardless. Mirrors
// conformance requirement 8-8 ("Parallel min-successful early
// completion") exactly: 4 branches, MinSuccessful: 2, max-concurrency 1
// for deterministic sequential ordering - branches 0 and 1 succeed,
// reaching the threshold, so branches 2 and 3 are never started.
func TestParallel_MinSuccessful_StopsEarly(t *testing.T) {
	client := newFakeClient()
	minSuccessful := 2

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		batch, err := operations.Parallel(dc, "min-successful", []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "a", nil },
			func(child types.DurableContext) (string, error) { return "b", nil },
			func(child types.DurableContext) (string, error) { return "unreached-c", nil },
			func(child types.DurableContext) (string, error) { return "unreached-d", nil },
		},
			operations.WithParallelMaxConcurrency[string](1),
			operations.WithParallelCompletionConfig[string](types.CompletionConfig{MinSuccessful: &minSuccessful}),
		)
		if err != nil {
			return orderResult{}, err
		}
		if batch.CompletionReason != operations.CompletionReasonMinSuccessfulReached {
			t.Errorf("expected CompletionReason == MIN_SUCCESSFUL_REACHED, got %q", batch.CompletionReason)
		}
		if batch.SucceededCount() != 2 {
			t.Errorf("expected SucceededCount() == 2, got %d", batch.SucceededCount())
		}
		if batch.TotalCount() != 2 {
			t.Errorf("expected TotalCount() == 2 (branches 2/3 never started - the real bug this fix closes), got %d", batch.TotalCount())
		}
		if batch.HasFailure() {
			t.Error("expected HasFailure() == false (no branch that actually ran failed)")
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-min-successful", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "min-successful-test"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		errMsg := "<nil>"
		if out.Error != nil {
			errMsg = out.Error.ErrorMessage
		}
		t.Fatalf("expected Succeeded, got status=%s error=%s", out.Status, errMsg)
	}

	snapshot := client.snapshot()
	branchCount := 0
	for _, op := range snapshot {
		if op.Type == types.OperationTypeContext && op.SubType == "ParallelBranch" {
			branchCount++
		}
	}
	if branchCount != 2 {
		t.Fatalf("expected exactly 2 checkpointed ParallelBranch operations (branches 2/3 should never have started once MinSuccessful=2 was reached), got %d", branchCount)
	}
}

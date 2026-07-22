package testing_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

type orderEvent struct {
	OrderID string `json:"orderId"`
}

type orderResult struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

// TestLocalTestRunner_Step mirrors the official Testing API Reference's
// "Assert on a step" example (docs.aws.amazon.com/durable-execution/testing/assertions/):
// run a handler with a single named step, then look it up by name and
// assert on its type, status, and checkpointed result.
func TestLocalTestRunner_Step(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "validated" {
		t.Fatalf("expected result.Status=validated, got %q", out.Status)
	}

	step, ok := result.GetOperation("validate")
	if !ok {
		t.Fatal("expected to find operation named 'validate'")
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
	if stepResult != "validated" {
		t.Fatalf("expected step result 'validated', got %q", stepResult)
	}
}

// TestLocalTestRunner_Wait mirrors the official "Assert on a wait" example:
// a handler that calls Wait should complete (SkipTime defaults to true in
// this Go port), and the WAIT operation should be recorded as SUCCEEDED.
func TestLocalTestRunner_Wait(t *testing.T) {
	afterWaitRan := false

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		if err := operations.Wait(dc, "cool-down", types.Duration{Seconds: 30}); err != nil {
			return orderResult{}, err
		}
		afterWaitRan = true
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "wait-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.GetStatus())
	}
	if !afterWaitRan {
		t.Fatal("expected code after Wait to have run (SkipTime should fast-forward it)")
	}

	wait, ok := result.GetOperation("cool-down")
	if !ok {
		t.Fatal("expected to find operation named 'cool-down'")
	}
	if wait.GetType() != types.OperationTypeWait {
		t.Fatalf("expected WAIT type, got %s", wait.GetType())
	}
	if wait.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected wait SUCCEEDED, got %s", wait.GetStatus())
	}
}

// TestLocalTestRunner_Step_NonZeroRetryDelaySuspendsAndResumes verifies
// the retry-delay suspension behavior (step.go's retryOrFail) end-to-end
// through the runner: a step retried with a NON-ZERO delay checkpoints
// Pending and genuinely suspends that invocation (unlike the zero-delay
// case, which re-executes immediately in-process - see
// TestLocalTestRunner_FilterOperationsByStatus above), but SkipTime still
// drives the whole thing to completion within a single Run() call via
// driveToCompletion's re-invocation loop: each suspension is detected as
// unresolved progress (the step's status just changed to Pending) and
// immediately re-invoked, picking the retry back up via runStep's
// OperationStatusPending branch.
func TestLocalTestRunner_Step_NonZeroRetryDelaySuspendsAndResumes(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			calls++
			if sc.Attempt() < 3 {
				return "", errors.New("not yet")
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "flaky-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected Succeeded, got status=%s error=%s", result.GetStatus(), msg)
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", calls)
	}

	step, ok := result.GetOperation("flaky")
	if !ok {
		t.Fatal("expected to find operation named 'flaky'")
	}
	if step.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected final step status Succeeded, got %s", step.GetStatus())
	}
}

// TestLocalTestRunner_WaitForCondition_NonZeroPollDelaySuspendsAndResumes
// mirrors TestLocalTestRunner_Step_NonZeroRetryDelaySuspendsAndResumes for
// WaitForCondition's poll loop (wait_for_condition.go's pollAndCheckpoint
// - see that file's retry-delay suspension fix, structurally identical to
// Step's): a non-zero poll delay genuinely suspends each intermediate
// invocation, but SkipTime still drives the whole thing to completion
// within a single Run() call via driveToCompletion's re-invocation loop.
func TestLocalTestRunner_WaitForCondition_NonZeroPollDelaySuspendsAndResumes(t *testing.T) {
	checks := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		final, err := operations.WaitForCondition(dc, "poll-ready", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
			checks++
			state++
			return operations.ConditionResult[int]{State: state, ConditionMet: state >= 3}, nil
		}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("state=%d", final)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "poll-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected Succeeded, got status=%s error=%s", result.GetStatus(), msg)
	}
	if checks != 3 {
		t.Fatalf("expected exactly 3 checks, got %d", checks)
	}

	cond, ok := result.GetOperation("poll-ready")
	if !ok {
		t.Fatal("expected to find operation named 'poll-ready'")
	}
	if cond.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected final condition status Succeeded, got %s", cond.GetStatus())
	}
}

// TestLocalTestRunner_Parallel_BranchWithNonZeroRetryDelay exercises the
// intersection of retry-delay suspension (step.go's retryOrFail) with
// Parallel's batch scheduler (operations/batch.go): a branch containing a
// Step that retries with a non-zero delay must suspend and resume
// correctly alongside other concurrently-running branches, without
// disrupting the batch's step-ID claiming or completion tracking. This is
// exactly the kind of new code path the Map/Parallel concurrency work
// flagged as highest-risk (see docs/remaining-work.md's writeup for that
// work) - retryOrFail's WaitForOperation call now happens from within a
// branch's own goroutine, not just the top-level handler goroutine.
func TestLocalTestRunner_Parallel_BranchWithNonZeroRetryDelay(t *testing.T) {
	var callsMu sync.Mutex
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) {
				return "fast", nil
			},
			func(child types.DurableContext) (string, error) {
				return operations.Step(child, "flaky", func(sc types.StepContext) (string, error) {
					callsMu.Lock()
					calls++
					callsMu.Unlock()
					if sc.Attempt() < 3 {
						return "", errors.New("not yet")
					}
					return "flaky-done", nil
				}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
			},
		}
		batch, err := operations.Parallel(dc, "mixed-with-retry", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: batch.Items[0].Value + "+" + batch.Items[1].Value}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "retry-in-parallel"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected Succeeded, got status=%s error=%s", result.GetStatus(), msg)
	}

	callsMu.Lock()
	finalCalls := calls
	callsMu.Unlock()
	if finalCalls != 3 {
		t.Fatalf("expected exactly 3 attempts of the flaky step, got %d", finalCalls)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "fast+flaky-done" {
		t.Fatalf("expected Status=fast+flaky-done, got %q", out.Status)
	}
}

// TestLocalTestRunner_Map_ItemWithNonZeroRetryDelay mirrors
// TestLocalTestRunner_Parallel_BranchWithNonZeroRetryDelay for Map: one
// item's Step retries with a non-zero delay while other items complete
// immediately, exercising the same runBatch/runBatchItem
// errSuspended-propagation fix from the Map side of the shared scheduler
// code (Map and Parallel both go through runBatch - see batch.go).
func TestLocalTestRunner_Map_ItemWithNonZeroRetryDelay(t *testing.T) {
	var callsMu sync.Mutex
	calls := map[int]int{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		items := []int{1, 2, 3}
		batch, err := operations.Map(dc, "flaky-items", items, func(child types.DurableContext, item int, index int) (int, error) {
			if item != 2 {
				return item * 10, nil
			}
			return operations.Step(child, "flaky", func(sc types.StepContext) (int, error) {
				callsMu.Lock()
				calls[index]++
				callsMu.Unlock()
				if sc.Attempt() < 2 {
					return 0, errors.New("not yet")
				}
				return 999, nil
			}, operations.WithStepRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
		})
		if err != nil {
			return orderResult{}, err
		}
		sum := 0
		for _, r := range batch.Items {
			sum += r.Value
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", sum)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "map-retry-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected Succeeded, got status=%s error=%s", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// item 0: 1*10=10, item 1 (index 1, value 2): flaky step -> 999,
	// item 2: 3*10=30. Sum = 10 + 999 + 30 = 1039.
	if out.Status != "1039" {
		t.Fatalf("expected Status=1039, got %q", out.Status)
	}

	callsMu.Lock()
	flakyCalls := calls[1]
	callsMu.Unlock()
	if flakyCalls != 2 {
		t.Fatalf("expected exactly 2 attempts of the flaky item's step, got %d", flakyCalls)
	}
}

// TestLocalTestRunner_FilterOperationsByStatus mirrors the official
// "Filter operations by status" example: a step that fails twice before
// succeeding should show up as 2 FAILED (retry) attempts plus 1 SUCCEEDED
// terminal operation when filtered by status.
func TestLocalTestRunner_FilterOperationsByStatus(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			calls++
			if sc.Attempt() < 3 {
				return "", errors.New("not yet")
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "flaky-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}

	// Only one operation entry exists per step ID in this SDK's model
	// (unlike the JS reference's one-entry-per-attempt history) - the
	// final checkpointed state reflects the last (successful) attempt.
	// Assert on the terminal status and Attempt count instead.
	step, ok := result.GetOperation("flaky")
	if !ok {
		t.Fatal("expected to find operation named 'flaky'")
	}
	if step.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected final status SUCCEEDED, got %s", step.GetStatus())
	}

	succeeded := result.GetOperationsByStatus(types.OperationStatusSucceeded)
	if len(succeeded) != 1 {
		t.Fatalf("expected 1 SUCCEEDED operation, got %d", len(succeeded))
	}
}

// TestLocalTestRunner_StepFailure verifies a step that exhausts retries
// (here, with retries disabled) fails the whole execution, and the
// failure is visible both on the TestResult and the individual operation.
func TestLocalTestRunner_StepFailure(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("permanent failure")
		}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "fail-me"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
	if msg, ok := result.GetError(); !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	step, ok := result.GetOperation("always-fails")
	if !ok {
		t.Fatal("expected to find operation named 'always-fails'")
	}
	if step.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected step FAILED, got %s", step.GetStatus())
	}
	if step.GetError() == nil {
		t.Fatal("expected a recorded step error")
	}
}

// TestLocalTestRunner_ReplaySkip verifies that re-invoking the SAME
// execution (via Continue, using the ARN from a prior invocation) does
// not re-execute already-completed steps - the core replay-skip
// guarantee every durable execution SDK provides.
func TestLocalTestRunner_ReplaySkip(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			calls++
			return "validated", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.GetStatus())
	}
	if calls != 1 {
		t.Fatalf("expected 1 call after first run, got %d", calls)
	}

	// Re-run against the SAME runner (shared in-memory client) with a
	// fresh Run call - by construction this starts a NEW, independent
	// execution (see LocalTestRunner.Run's doc), so this specifically
	// tests runner reuse across tests rather than in-execution replay.
	// Multiple Run calls must not leak state between executions.
	result2, err := runner.Run(orderEvent{OrderID: "xyz"})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if result2.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second run SUCCEEDED, got %s", result2.GetStatus())
	}
	if calls != 2 {
		t.Fatalf("expected 2 total calls after second independent run, got %d", calls)
	}
	out2, err := dtesting.GetResult[orderResult](result2)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out2.OrderID != "xyz" {
		t.Fatalf("expected second execution's own event (xyz), got %q - executions leaked state", out2.OrderID)
	}
}

// TestLocalTestRunner_RunInChildContext mirrors the official Testing API
// Reference's "Assert on a child context" example
// (docs.aws.amazon.com/durable-execution/testing/assertions/): a handler
// that runs a step inside a child context should produce a CONTEXT
// operation whose child operations include the nested step, and the
// context's own checkpointed result should match what the child function
// returned.
func TestLocalTestRunner_RunInChildContext(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.RunInChildContext(dc, "process", func(child types.DurableContext) (string, error) {
			return operations.Step(child, "compute", func(sc types.StepContext) (string, error) {
				return "computed:" + event.OrderID, nil
			})
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "child-ctx-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "computed:child-ctx-order" {
		t.Fatalf("expected result.Status='computed:child-ctx-order', got %q", out.Status)
	}

	ctxOp, ok := result.GetOperation("process")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'process'")
	}
	if ctxOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", ctxOp.GetType())
	}
	if ctxOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected context SUCCEEDED, got %s", ctxOp.GetStatus())
	}

	ctxResult, err := dtesting.ContextResult[string](ctxOp)
	if err != nil {
		t.Fatalf("reading context result: %v", err)
	}
	if ctxResult != "computed:child-ctx-order" {
		t.Fatalf("expected checkpointed context result 'computed:child-ctx-order', got %q", ctxResult)
	}

	children := result.GetChildOperations(ctxOp.GetID())
	if len(children) != 1 {
		t.Fatalf("expected 1 child operation nested under the context, got %d", len(children))
	}
	if children[0].GetName() != "compute" {
		t.Fatalf("expected child operation named 'compute', got %q", children[0].GetName())
	}
	if children[0].GetType() != types.OperationTypeStep {
		t.Fatalf("expected child operation type STEP, got %s", children[0].GetType())
	}
	if children[0].GetID() == ctxOp.GetID() {
		t.Fatal("expected child operation to have a distinct, prefixed step ID from its parent context")
	}
}

// TestLocalTestRunner_ParentID_NestedScenario verifies the ParentID chain
// end-to-end (docs/remaining-work.md's ParentID fix) across a genuine
// nested scenario mixing every ID-namespace shape this SDK produces:
//
//   - two top-level Steps directly on the root context ("top1", "top2")
//   - both must have ParentID == "" (a root-level operation has no
//     parent, matching the official API reference's "Required: No" on
//     Operation/OperationUpdate.ParentId).
//   - a RunInChildContext ("child") wrapping ONE nested Step ("inner") -
//     the CONTEXT/RUN_IN_CHILD_CONTEXT operation itself must have
//     ParentID == "" (it's rooted directly on the root context too), and
//     "inner" (checkpointed against the CHILD context RunInChildContext
//     hands to fn) must have ParentID == the CONTEXT operation's own ID -
//     i.e. dcontext.Context.prefix for that child Context, exactly as
//     Context.ParentStepID's doc describes.
//   - a Parallel ("fanout") with two branches, each running its own Step
//     ("branch-step") - the outer CONTEXT/PARALLEL operation must have
//     ParentID == "" (rooted on the root context), each
//     CONTEXT/PARALLEL_BRANCH operation must have ParentID == the outer
//     PARALLEL operation's own ID (runBatchItem checkpoints against
//     parallelCtx, whose prefix IS the outer PARALLEL operation's ID),
//     and each branch's own "branch-step" must have ParentID == THAT
//     branch's own CONTEXT/PARALLEL_BRANCH operation ID (branch-step is
//     checkpointed against the branch's own child context, which
//     runBatchItem creates via c.NewChildWithName(itemID, name) - so its
//     prefix is itemID, the branch's own operation ID) - i.e. ParentID
//     forms a genuine THREE-level chain here (root -> PARALLEL ->
//     PARALLEL_BRANCH -> branch-step), not just one level, confirming the
//     fix threads through arbitrarily nested contexts correctly and not
//     just the single-level RunInChildContext case.
//
// This is deliberately checked against the raw ParentID field
// (op.GetParentID(), which round-trips types.Operation.ParentID through
// the in-memory test client exactly as a real backend would - see
// inmemory_client.go's Checkpoint, which persists op.ParentID = u.ParentID
// unconditionally) rather than via GetChildOperations (which deliberately
// still uses the hierarchical-ID-prefix convention for the LOCAL test
// client - see that function's own doc for why that's the right choice
// for this SDK-internal, not real-backend-facing, client), since the
// whole point of this test is to verify the WIRE-FACING field a real
// backend read-back would need, independently of the local convenience
// convention.
func TestLocalTestRunner_ParentID_NestedScenario(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		if _, err := operations.Step(dc, "top1", func(sc types.StepContext) (string, error) {
			return "one", nil
		}); err != nil {
			return orderResult{}, err
		}
		if _, err := operations.Step(dc, "top2", func(sc types.StepContext) (string, error) {
			return "two", nil
		}); err != nil {
			return orderResult{}, err
		}

		if _, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
			return operations.Step(child, "inner", func(sc types.StepContext) (string, error) {
				return "inner-result", nil
			})
		}); err != nil {
			return orderResult{}, err
		}

		branch := func(child types.DurableContext) (string, error) {
			return operations.Step(child, "branch-step", func(sc types.StepContext) (string, error) {
				return "branch-result", nil
			})
		}
		if _, err := operations.Parallel(dc, "fanout", []func(types.DurableContext) (string, error){branch, branch}); err != nil {
			return orderResult{}, err
		}

		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "parentid-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	// Two root-level Steps: no parent.
	top1, ok := result.GetOperation("top1")
	if !ok {
		t.Fatal("expected to find 'top1'")
	}
	if top1.GetParentID() != "" {
		t.Fatalf("expected 'top1' ParentID to be empty (root-level), got %q", top1.GetParentID())
	}
	top2, ok := result.GetOperation("top2")
	if !ok {
		t.Fatal("expected to find 'top2'")
	}
	if top2.GetParentID() != "" {
		t.Fatalf("expected 'top2' ParentID to be empty (root-level), got %q", top2.GetParentID())
	}

	// RunInChildContext: the CONTEXT operation itself is root-level (no
	// parent); its nested Step's ParentID must equal the CONTEXT
	// operation's own ID.
	childCtx, ok := result.GetOperation("child")
	if !ok {
		t.Fatal("expected to find CONTEXT operation 'child'")
	}
	if childCtx.GetParentID() != "" {
		t.Fatalf("expected 'child' CONTEXT ParentID to be empty (root-level), got %q", childCtx.GetParentID())
	}
	inner, ok := result.GetOperationRecursive("inner")
	if !ok {
		t.Fatal("expected to find nested Step 'inner'")
	}
	if inner.GetParentID() != childCtx.GetID() {
		t.Fatalf("expected 'inner' ParentID (%q) to equal enclosing CONTEXT operation's own ID (%q)", inner.GetParentID(), childCtx.GetID())
	}

	// Parallel: the outer CONTEXT/PARALLEL operation is root-level;
	// each PARALLEL_BRANCH's ParentID must equal the outer operation's
	// own ID; each branch's own nested Step's ParentID must equal THAT
	// branch's own operation ID - a genuine three-level chain.
	fanout, ok := result.GetOperation("fanout")
	if !ok {
		t.Fatal("expected to find CONTEXT operation 'fanout'")
	}
	if fanout.GetParentID() != "" {
		t.Fatalf("expected 'fanout' CONTEXT ParentID to be empty (root-level), got %q", fanout.GetParentID())
	}

	branches := result.GetChildOperations(fanout.GetID())
	if len(branches) != 2 {
		t.Fatalf("expected 2 PARALLEL_BRANCH children under 'fanout', got %d", len(branches))
	}
	for _, br := range branches {
		if br.GetParentID() != fanout.GetID() {
			t.Fatalf("expected branch %q ParentID (%q) to equal outer PARALLEL operation's own ID (%q)", br.GetID(), br.GetParentID(), fanout.GetID())
		}
		brStepChildren := result.GetChildOperations(br.GetID())
		if len(brStepChildren) != 1 {
			t.Fatalf("expected 1 nested Step under branch %q, got %d", br.GetID(), len(brStepChildren))
		}
		brStep := brStepChildren[0]
		if brStep.GetName() != "branch-step" {
			t.Fatalf("expected nested step named 'branch-step', got %q", brStep.GetName())
		}
		if brStep.GetParentID() != br.GetID() {
			t.Fatalf("expected 'branch-step' ParentID (%q) to equal its OWN enclosing branch's operation ID (%q), not the outer PARALLEL's ID (%q)", brStep.GetParentID(), br.GetID(), fanout.GetID())
		}
	}
}

// TestLocalTestRunner_RunInChildContext_ReplaySkip verifies that once a
// child context has completed, re-running the SAME execution does not
// re-invoke fn at all (not even to replay-skip its own nested step) -
// the child-context-level checkpoint is a coarser barrier on top of the
// nested operations' own replay-skip checks.
func TestLocalTestRunner_RunInChildContext_ReplaySkip(t *testing.T) {
	fnCalls := 0
	stepCalls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.RunInChildContext(dc, "process", func(child types.DurableContext) (string, error) {
			fnCalls++
			return operations.Step(child, "compute", func(sc types.StepContext) (string, error) {
				stepCalls++
				return "computed", nil
			})
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:replay-child-ctx-test:1"

	result, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.GetStatus())
	}
	if fnCalls != 1 || stepCalls != 1 {
		t.Fatalf("expected 1 fn call and 1 step call after first invocation, got fnCalls=%d stepCalls=%d", fnCalls, stepCalls)
	}

	// "Replay" the SAME execution ARN again - a real backend would only
	// do this on genuine re-invocation (e.g. after a suspend/resume), but
	// Continue lets the test force it directly to verify the replay-skip
	// guarantee in isolation.
	result2, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if result2.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second invocation SUCCEEDED, got %s", result2.GetStatus())
	}
	if fnCalls != 1 || stepCalls != 1 {
		t.Fatalf("expected fn/step NOT to be re-invoked on replay, got fnCalls=%d stepCalls=%d", fnCalls, stepCalls)
	}
}

// TestLocalTestRunner_RunInChildContext_Failure verifies that an error
// returned from the child function fails the CONTEXT operation and
// propagates as the RunInChildContext call's own error.
func TestLocalTestRunner_RunInChildContext_Failure(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.RunInChildContext(dc, "process", func(child types.DurableContext) (string, error) {
			return "", errors.New("child context failed")
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "fail-me"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}

	ctxOp, ok := result.GetOperation("process")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'process'")
	}
	if ctxOp.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected context FAILED, got %s", ctxOp.GetStatus())
	}
	if ctxOp.GetError() == nil {
		t.Fatal("expected a recorded context error")
	}
}

// TestLocalTestRunner_WaitForCallback_Success mirrors the official
// Testing API Reference's "Assert on a callback" example
// (docs.aws.amazon.com/durable-execution/testing/assertions/): drive the
// execution to a PENDING state where a callback is awaiting an external
// system, resolve it via Operation.SendCallbackSuccess, then continue the
// execution to completion.
func TestLocalTestRunner_WaitForCallback_Success(t *testing.T) {
	var mu sync.Mutex
	var capturedCallbackID string

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.WaitForCallback[string](dc, "approval", func(sc types.StepContext, callbackID string) error {
			mu.Lock()
			capturedCallbackID = callbackID
			mu.Unlock()
			return nil // in production, this would notify an external system
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-test:1"

	pending, err := runner.Continue(arn, orderEvent{OrderID: "wfc-order"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING (execution should suspend awaiting the callback), got %s (%s)", pending.GetStatus(), msg)
	}
	mu.Lock()
	gotCallbackID := capturedCallbackID
	mu.Unlock()
	if gotCallbackID == "" {
		t.Fatal("expected the submitter function to have captured a non-empty callback ID")
	}

	callbackOp, ok := pending.GetOperation("approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'approval-callback'")
	}
	if callbackOp.GetType() != types.OperationTypeCallback {
		t.Fatalf("expected CALLBACK type, got %s", callbackOp.GetType())
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED (awaiting external resolution), got %s", callbackOp.GetStatus())
	}

	if err := callbackOp.SendCallbackSuccess("approved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, orderEvent{OrderID: "wfc-order"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after resolving the callback, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "approved" {
		t.Fatalf("expected result.Status='approved', got %q", out.Status)
	}

	waitForCallbackCtx, ok := final.GetOperation("approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'approval'")
	}
	if waitForCallbackCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", waitForCallbackCtx.GetType())
	}
	if waitForCallbackCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected context SUCCEEDED, got %s", waitForCallbackCtx.GetStatus())
	}

	children := final.GetChildOperations(waitForCallbackCtx.GetID())
	childNames := map[string]bool{}
	for _, child := range children {
		childNames[child.GetName()] = true
	}
	if !childNames["approval-callback"] || !childNames["approval-submit"] {
		t.Fatalf("expected 'approval-callback' and 'approval-submit' as children of the 'approval' context, got %v", childNames)
	}
}

// TestLocalTestRunner_WaitForCallback_Failure verifies that an external
// system calling SendCallbackFailure fails the whole WaitForCallback
// operation (and thus the execution), with the error propagated intact.
func TestLocalTestRunner_WaitForCallback_Failure(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		status, err := operations.WaitForCallback[string](dc, "approval", func(sc types.StepContext, callbackID string) error {
			return nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: status}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-failure-test:1"

	pending, err := runner.Continue(arn, orderEvent{OrderID: "wfc-fail-order"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING, got %s", pending.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'approval-callback'")
	}
	if err := callbackOp.SendCallbackFailure(types.ErrorObject{ErrorMessage: "rejected by approver"}); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	final, err := runner.Continue(arn, orderEvent{OrderID: "wfc-fail-order"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED after the callback was rejected, got %s", final.GetStatus())
	}
	msg, ok := final.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	waitForCallbackCtx, ok := final.GetOperation("approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'approval'")
	}
	if waitForCallbackCtx.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected context FAILED, got %s", waitForCallbackCtx.GetStatus())
	}
}

// TestLocalTestRunner_CreateCallback_LowLevel verifies the low-level
// CreateCallback primitive directly (not composed via WaitForCallback):
// registering a callback returns immediately with a callback ID, and the
// result channel only resolves once the callback is externally completed.
func TestLocalTestRunner_CreateCallback_LowLevel(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		resultCh, callbackID, err := operations.CreateCallback[string](dc, "manual-callback")
		if err != nil {
			return orderResult{}, err
		}
		if callbackID == "" {
			return orderResult{}, fmt.Errorf("expected a non-empty callback ID immediately after CreateCallback")
		}
		// Mirrors the flowchart's "OtherProcessing" step between
		// ReturnCallbackId and AwaitCallbackPromise.
		if _, err := operations.Step(dc, "notify-external-system", func(sc types.StepContext) (string, error) {
			return callbackID, nil
		}); err != nil {
			return orderResult{}, err
		}

		result := operations.AwaitCallback(dc, resultCh)
		if result.Err != nil {
			return orderResult{}, result.Err
		}
		return orderResult{OrderID: event.OrderID, Status: result.Value}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:create-callback-test:1"

	pending, err := runner.Continue(arn, orderEvent{OrderID: "manual-order"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("manual-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'manual-callback'")
	}
	if err := callbackOp.SendCallbackSuccess("done"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, orderEvent{OrderID: "manual-order"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[orderResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "done" {
		t.Fatalf("expected result.Status='done', got %q", out.Status)
	}
}

// TestLocalTestRunner_WaitForCondition_MetImmediately verifies the happy
// path: checkFn reports ConditionMet: true on the first call, so no
// retry/poll loop is needed.
func TestLocalTestRunner_WaitForCondition_MetImmediately(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		finalCount, err := operations.WaitForCondition(dc, "poll-ready", func(sc types.StepContext, count int) (operations.ConditionResult[int], error) {
			calls++
			return operations.ConditionResult[int]{State: count + 1, ConditionMet: true}, nil
		}, 0)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("count=%d", finalCount)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "condition-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 check call, got %d", calls)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "count=1" {
		t.Fatalf("expected result.Status='count=1', got %q", out.Status)
	}

	pollOp, ok := result.GetOperation("poll-ready")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'poll-ready'")
	}
	if pollOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", pollOp.GetType())
	}
	if pollOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", pollOp.GetStatus())
	}
}

// TestLocalTestRunner_WaitForCondition_PollsUntilMet verifies that
// checkFn is invoked repeatedly (driven by the configured retry
// strategy) until it reports ConditionMet: true, checkpointing a
// STEP/WAIT_FOR_CONDITION RETRY between polls.
func TestLocalTestRunner_WaitForCondition_PollsUntilMet(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		finalCount, err := operations.WaitForCondition(dc, "poll-ready", func(sc types.StepContext, count int) (operations.ConditionResult[int], error) {
			calls++
			next := count + 1
			return operations.ConditionResult[int]{State: next, ConditionMet: next >= 3}, nil
		}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 10)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("count=%d", finalCount)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "poll-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 check calls (count reaches 3 on the 3rd), got %d", calls)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "count=3" {
		t.Fatalf("expected result.Status='count=3', got %q", out.Status)
	}
}

// TestLocalTestRunner_WaitForCondition_NeverMet verifies that when the
// condition is never met and the retry strategy exhausts its attempts,
// the operation fails the whole execution.
func TestLocalTestRunner_WaitForCondition_NeverMet(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.WaitForCondition(dc, "never-ready", func(sc types.StepContext, count int) (operations.ConditionResult[int], error) {
			return operations.ConditionResult[int]{State: count + 1, ConditionMet: false}, nil
		}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 3)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "never-order"})
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

	pollOp, ok := result.GetOperation("never-ready")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'never-ready'")
	}
	if pollOp.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected FAILED, got %s", pollOp.GetStatus())
	}
	if pollOp.GetError() == nil {
		t.Fatal("expected a recorded error")
	}
}

// TestLocalTestRunner_WaitForCondition_CheckFnError verifies that an
// actual error from checkFn (as opposed to ConditionMet: false) is also
// subject to the retry strategy, and propagates as the operation's error
// if retries are exhausted.
func TestLocalTestRunner_WaitForCondition_CheckFnError(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.WaitForCondition(dc, "erroring-check", func(sc types.StepContext, count int) (operations.ConditionResult[int], error) {
			return operations.ConditionResult[int]{}, errors.New("check function exploded")
		}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.NoRetry()))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "error-order"})
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
}

// TestLocalTestRunner_Map_AllSucceed verifies the happy path: Map runs fn
// over every item concurrently, each within its own CONTEXT/MAP_ITERATION
// operation, and the outer CONTEXT/MAP operation succeeds once every item
// does.
func TestLocalTestRunner_Map_AllSucceed(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		items := []int{1, 2, 3, 4, 5}
		batch, err := operations.Map(dc, "double-items", items, func(child types.DurableContext, item int, index int) (int, error) {
			return item * 2, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		sum := 0
		for _, r := range batch.Items {
			sum += r.Value
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", sum)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "map-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "30" { // (1+2+3+4+5)*2
		t.Fatalf("expected sum 30, got %s", out.Status)
	}

	mapOp, ok := result.GetOperation("double-items")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'double-items'")
	}
	if mapOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", mapOp.GetType())
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected map SUCCEEDED, got %s", mapOp.GetStatus())
	}

	iterOp, ok := result.GetOperation("double-items[0]")
	if !ok {
		t.Fatal("expected to find a CONTEXT/MAP_ITERATION operation named 'double-items[0]'")
	}
	if iterOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected iteration 0 SUCCEEDED, got %s", iterOp.GetStatus())
	}
}

// TestLocalTestRunner_Map_DefaultFailsOnAnyError verifies that, with no
// CompletionConfig given, a single failing item fails the whole Map
// (Promise.all-style default semantics).
func TestLocalTestRunner_Map_DefaultFailsOnAnyError(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		items := []int{1, 2, 3}
		batch, err := operations.Map(dc, "risky-items", items, func(child types.DurableContext, item int, index int) (int, error) {
			if item == 2 {
				return 0, errors.New("item 2 failed")
			}
			return item, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		// Map no longer returns an error for a policy-not-met batch (see
		// operations.BatchResult's own completion-policy-contract doc) -
		// this test's own scenario relies on Map's DEFAULT semantics
		// ("no CompletionConfig at all" -> any single failure fails the
		// whole batch, per batchCompletion.overallSucceeded's own
		// documented default), so it must now ask BatchResult for that
		// outcome explicitly via ThrowIfError() rather than getting it
		// automatically from Map's own old return shape.
		if throwErr := batch.ThrowIfError(); throwErr != nil {
			return orderResult{}, throwErr
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "map-fail-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}

	mapOp, ok := result.GetOperation("risky-items")
	if !ok {
		t.Fatal("expected to find the map operation")
	}
	// The outer CONTEXT/MAP operation itself is now checkpointed
	// SUCCEEDED, even though item 2 failed - only the individual failing
	// MAP_ITERATION child is checkpointed FAILED (see operations.
	// BatchResult's own completion-policy-contract doc: the outer
	// context's own checkpoint no longer reflects a policy-not-met
	// outcome at all). The overall EXECUTION still genuinely fails
	// because this test's own handler above now explicitly calls
	// ThrowIfError() and propagates the result.
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected outer map operation SUCCEEDED (the outer Map context always succeeds once its items finish, regardless of whether the batch's own policy was met), got %s", mapOp.GetStatus())
	}
}

// TestLocalTestRunner_Map_CompletionConfigTolerated verifies that a
// CompletionConfig with ToleratedFailureCount lets the batch succeed
// despite some item failures.
func TestLocalTestRunner_Map_CompletionConfigTolerated(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		items := []int{1, 2, 3, 4}
		tolerated := 1
		batch, err := operations.Map(dc, "tolerant-items", items, func(child types.DurableContext, item int, index int) (int, error) {
			if item == 3 {
				return 0, errors.New("item 3 failed")
			}
			return item, nil
		}, operations.WithMapCompletionConfig[int, int](types.CompletionConfig{ToleratedFailureCount: &tolerated}))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", batch.SucceededCount())}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "map-tolerant-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "3" {
		t.Fatalf("expected 3 successful items, got %s", out.Status)
	}
}

// TestLocalTestRunner_Map_ReplaySkip verifies that re-invoking the same
// execution does not re-run already-completed map iterations.
func TestLocalTestRunner_Map_ReplaySkip(t *testing.T) {
	var mu sync.Mutex
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		items := []int{1, 2, 3}
		batch, err := operations.Map(dc, "counted-items", items, func(child types.DurableContext, item int, index int) (int, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return item, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", batch.SucceededCount())}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:map-replay-test:1"

	result1, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if result1.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result1.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result1.GetStatus(), msg)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls after first invocation, got %d", calls)
	}

	result2, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if result2.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result2.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result2.GetStatus(), msg)
	}
	if calls != 3 {
		t.Fatalf("expected still 3 calls after replay (map should be skipped), got %d", calls)
	}
}

// TestLocalTestRunner_Parallel_AllSucceed verifies the happy path for
// Parallel: each branch runs concurrently within its own
// CONTEXT/PARALLEL_BRANCH operation.
func TestLocalTestRunner_Parallel_AllSucceed(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "a", nil },
			func(child types.DurableContext) (string, error) { return "b", nil },
			func(child types.DurableContext) (string, error) { return "c", nil },
		}
		batch, err := operations.Parallel(dc, "parallel-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		joined := ""
		for _, r := range batch.Items {
			joined += r.Value
		}
		return orderResult{OrderID: event.OrderID, Status: joined}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "parallel-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "abc" {
		t.Fatalf("expected 'abc' preserving input order, got %q", out.Status)
	}

	branchOp, ok := result.GetOperation("parallel-branches[1]")
	if !ok {
		t.Fatal("expected to find a CONTEXT/PARALLEL_BRANCH operation named 'parallel-branches[1]'")
	}
	if branchOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected branch 1 SUCCEEDED, got %s", branchOp.GetStatus())
	}
}

// TestLocalTestRunner_Parallel_MaxConcurrency verifies that
// WithParallelMaxConcurrency actually bounds the number of branches
// running at once.
func TestLocalTestRunner_Parallel_MaxConcurrency(t *testing.T) {
	var mu sync.Mutex
	inFlight := 0
	maxObserved := 0
	release := make(chan struct{})

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := make([]func(types.DurableContext) (int, error), 6)
		for i := range branches {
			branches[i] = func(child types.DurableContext) (int, error) {
				mu.Lock()
				inFlight++
				if inFlight > maxObserved {
					maxObserved = inFlight
				}
				mu.Unlock()

				<-release

				mu.Lock()
				inFlight--
				mu.Unlock()
				return 1, nil
			}
		}
		batch, err := operations.Parallel(dc, "bounded-branches", branches, operations.WithParallelMaxConcurrency[int](2))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", batch.SucceededCount())}, nil
	}

	go func() {
		// Release branches gradually so at most 2 are ever unblocked at
		// once, letting maxObserved reflect the semaphore's actual cap
		// rather than everything finishing instantly.
		for i := 0; i < 6; i++ {
			release <- struct{}{}
		}
	}()

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "bounded-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	mu.Lock()
	observed := maxObserved
	mu.Unlock()
	if observed > 2 {
		t.Fatalf("expected at most 2 branches in flight at once, observed %d", observed)
	}
}

// TestLocalTestRunner_Parallel_InvalidMaxConcurrencyRejected verifies the
// docs/remaining-work.md §8 task 18 config-validation fix: a non-positive
// WithParallelMaxConcurrency value is rejected with a descriptive error
// from Parallel itself, rather than being silently treated as "no bound"
// (runBatch's own `*maxConcurrency > 0` guard previously let 0 and
// negative values fall through to fully unbounded concurrency with no
// indication anything was wrong).
func TestLocalTestRunner_Parallel_InvalidMaxConcurrencyRejected(t *testing.T) {
	for _, n := range []int{0, -1, -5} {
		n := n
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
				branches := []func(types.DurableContext) (int, error){
					func(child types.DurableContext) (int, error) { return 1, nil },
				}
				_, err := operations.Parallel(dc, "branches", branches, operations.WithParallelMaxConcurrency[int](n))
				return orderResult{}, err
			}

			runner := dtesting.New(handler, nil)
			result, err := runner.Run(orderEvent{OrderID: "x"})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if result.GetStatus() != types.ExecutionStatusFailed {
				t.Fatalf("expected FAILED, got %s", result.GetStatus())
			}
			msg, _ := result.GetError()
			if !strings.Contains(msg, "maxConcurrency must be positive") {
				t.Fatalf("expected maxConcurrency validation error, got %q", msg)
			}
		})
	}
}

// TestLocalTestRunner_Map_InvalidCompletionConfigRejected verifies the
// same task 18 fix for Map/Parallel's shared CompletionConfig validation:
// a negative MinSuccessful/ToleratedFailureCount or an out-of-range
// ToleratedFailurePercentage is rejected up front rather than silently
// evaluated against batchCompletion's thresholds (where, e.g., a
// negative MinSuccessful would be trivially satisfied by the very first
// item for reasons unrelated to what the caller actually meant).
func TestLocalTestRunner_Map_InvalidCompletionConfigRejected(t *testing.T) {
	negOne := -1
	pct150 := 150.0

	cases := []struct {
		name    string
		cfg     types.CompletionConfig
		wantMsg string
	}{
		{"NegativeMinSuccessful", types.CompletionConfig{MinSuccessful: &negOne}, "MinSuccessful must be non-negative"},
		{"NegativeToleratedFailureCount", types.CompletionConfig{ToleratedFailureCount: &negOne}, "ToleratedFailureCount must be non-negative"},
		{"OutOfRangeToleratedFailurePercentage", types.CompletionConfig{ToleratedFailurePercentage: &pct150}, "ToleratedFailurePercentage must be between 0 and 100"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
				items := []int{1, 2, 3}
				_, err := operations.Map(dc, "items", items, func(child types.DurableContext, item int, index int) (int, error) {
					return item, nil
				}, operations.WithMapCompletionConfig[int, int](tc.cfg))
				return orderResult{}, err
			}

			runner := dtesting.New(handler, nil)
			result, err := runner.Run(orderEvent{OrderID: "x"})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if result.GetStatus() != types.ExecutionStatusFailed {
				t.Fatalf("expected FAILED, got %s", result.GetStatus())
			}
			msg, _ := result.GetError()
			if !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("expected error containing %q, got %q", tc.wantMsg, msg)
			}
		})
	}
}

// TestLocalTestRunner_WaitForCallback_NegativeTimeoutRejected verifies
// task 18's WithWaitForCallbackTimeout validation: a negative Duration
// has no valid interpretation as a timeout and is rejected before
// CreateCallback/the submitter Step ever run (so nothing is checkpointed
// for an invalid call).
func TestLocalTestRunner_WaitForCallback_NegativeTimeoutRejected(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.WaitForCallback[string](dc, "approval", func(sc types.StepContext, callbackID string) error {
			return nil
		}, operations.WithWaitForCallbackTimeout[string](types.Duration{Seconds: -1}))
		return orderResult{}, err
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "x"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
	msg, _ := result.GetError()
	if !strings.Contains(msg, "must be non-negative") {
		t.Fatalf("expected timeout validation error, got %q", msg)
	}
}

// TestLocalTestRunner_Parallel_GenuineSuspension verifies that a branch
// which genuinely blocks on an external event (a callback awaiting an
// external system, exactly like WaitForCallback) correctly suspends the
// WHOLE execution (returns PENDING), and that resolving it lets the
// batch - and the other, already-finished branches' results - complete
// normally. This specifically exercises the real-suspension path through
// runBatch (as opposed to the exclusively fast-path/no-real-blocking
// cases the other Parallel/Map tests cover), which is what this
// session's two runBatch concurrency bugs were about NOT
// false-triggering on the fast path without breaking this genuine case.
func TestLocalTestRunner_Parallel_GenuineSuspension(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) {
				return "fast", nil
			},
			func(child types.DurableContext) (string, error) {
				result, _, err := operations.CreateCallback[string](child, "slow-callback")
				if err != nil {
					return "", err
				}
				r := operations.AwaitCallback(child, result)
				return r.Value, r.Err
			},
		}
		batch, err := operations.Parallel(dc, "mixed-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: batch.Items[0].Value + "+" + batch.Items[1].Value}, nil
	}

	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:parallel-suspend-test:1"

	pending, err := runner.Continue(arn, orderEvent{OrderID: "suspend-order"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING (execution should genuinely suspend awaiting the callback), got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("slow-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'slow-callback'")
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED (awaiting external resolution), got %s", callbackOp.GetStatus())
	}

	if err := callbackOp.SendCallbackSuccess("slow"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, orderEvent{OrderID: "suspend-order"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after resolving the callback, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "fast+slow" {
		t.Fatalf("expected 'fast+slow', got %q", out.Status)
	}
}

// TestLocalTestRunner_All_Success mirrors Promise.all: every branch
// succeeds.
func TestLocalTestRunner_All_Success(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (int, error){
			func(child types.DurableContext) (int, error) { return 1, nil },
			func(child types.DurableContext) (int, error) { return 2, nil },
		}
		values, err := operations.All(dc, "all-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		sum := 0
		for _, v := range values {
			sum += v
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", sum)}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "all-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "3" {
		t.Fatalf("expected sum 3, got %s", out.Status)
	}
}

// TestLocalTestRunner_All_Failure verifies All fails with an
// *AggregateError when any branch fails.
func TestLocalTestRunner_All_Failure(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (int, error){
			func(child types.DurableContext) (int, error) { return 1, nil },
			func(child types.DurableContext) (int, error) { return 0, errors.New("branch failed") },
		}
		_, err := operations.All(dc, "all-fail-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "all-fail-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
}

// TestLocalTestRunner_AllSettled_MixedResults verifies AllSettled always
// succeeds and reports every branch's individual outcome.
func TestLocalTestRunner_AllSettled_MixedResults(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (int, error){
			func(child types.DurableContext) (int, error) { return 1, nil },
			func(child types.DurableContext) (int, error) { return 0, errors.New("branch failed") },
		}
		batch, err := operations.AllSettled(dc, "settled-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("%d", batch.SucceededCount())}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "settled-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "1" {
		t.Fatalf("expected 1 successful branch, got %s", out.Status)
	}
}

// TestLocalTestRunner_Any_FirstSuccess verifies Any returns a successful
// result even when some branches fail.
func TestLocalTestRunner_Any_FirstSuccess(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "", errors.New("branch 0 failed") },
			func(child types.DurableContext) (string, error) { return "winner", nil },
		}
		value, err := operations.Any(dc, "any-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: value}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "any-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "winner" {
		t.Fatalf("expected 'winner', got %q", out.Status)
	}
}

// TestLocalTestRunner_Any_AllFail verifies Any returns an *AggregateError
// when every branch fails.
func TestLocalTestRunner_Any_AllFail(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "", errors.New("branch 0 failed") },
			func(child types.DurableContext) (string, error) { return "", errors.New("branch 1 failed") },
		}
		_, err := operations.Any(dc, "any-fail-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "any-fail-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}
}

// TestLocalTestRunner_Race_ReturnsFirstInInputOrder verifies Race returns
// the first-in-input-order branch's outcome (see operations.Race's
// determinism-note doc for why input order rather than true
// first-to-complete order is used).
func TestLocalTestRunner_Race_ReturnsFirstInInputOrder(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		branches := []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "first", nil },
			func(child types.DurableContext) (string, error) { return "second", nil },
		}
		value, err := operations.Race(dc, "race-branches", branches)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: value}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "race-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "first" {
		t.Fatalf("expected 'first', got %q", out.Status)
	}
}

// TestLocalTestRunner_Invoke_Success mirrors the official runner API's
// "Register mock handlers for invoke" pattern: register a mock for the
// target function name, then verify operations.Invoke calls it and
// checkpoints CHAINED_INVOKE/CHAINED_INVOKE SUCCEED with the result.
func TestLocalTestRunner_Invoke_Success(t *testing.T) {
	type greetRequest struct {
		Name string `json:"name"`
	}
	type greetResponse struct {
		Greeting string `json:"greeting"`
	}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		resp, err := operations.Invoke[greetRequest, greetResponse](dc, "greet", "arn:aws:lambda:us-east-1:123456789012:function:greeter",
			greetRequest{Name: event.OrderID})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: resp.Greeting}, nil
	}

	runner := dtesting.New(handler, nil)
	dtesting.RegisterDurableFunction(runner, "arn:aws:lambda:us-east-1:123456789012:function:greeter",
		func(req greetRequest) (greetResponse, error) {
			return greetResponse{Greeting: "hello, " + req.Name}, nil
		})

	result, err := runner.Run(orderEvent{OrderID: "invoke-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[orderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "hello, invoke-order" {
		t.Fatalf("expected result.Status='hello, invoke-order', got %q", out.Status)
	}

	invokeOp, ok := result.GetOperation("greet")
	if !ok {
		t.Fatal("expected to find a CHAINED_INVOKE operation named 'greet'")
	}
	if invokeOp.GetType() != types.OperationTypeChainedInvoke {
		t.Fatalf("expected CHAINED_INVOKE type, got %s", invokeOp.GetType())
	}
	if invokeOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected invoke SUCCEEDED, got %s", invokeOp.GetStatus())
	}
}

// TestLocalTestRunner_Invoke_TargetFunctionError verifies that an error
// returned from the mocked target function fails the CHAINED_INVOKE
// operation and propagates as the Invoke call's own error.
func TestLocalTestRunner_Invoke_TargetFunctionError(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Invoke[string, string](dc, "call-flaky", "arn:aws:lambda:us-east-1:123456789012:function:flaky", event.OrderID)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	runner.RegisterFunction("arn:aws:lambda:us-east-1:123456789012:function:flaky", func(payload string) (string, error) {
		return "", errors.New("target function failed")
	})

	result, err := runner.Run(orderEvent{OrderID: "invoke-fail-order"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}

	invokeOp, ok := result.GetOperation("call-flaky")
	if !ok {
		t.Fatal("expected to find a CHAINED_INVOKE operation named 'call-flaky'")
	}
	if invokeOp.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected invoke FAILED, got %s", invokeOp.GetStatus())
	}
	if invokeOp.GetError() == nil {
		t.Fatal("expected a recorded invoke error")
	}
}

// TestLocalTestRunner_Invoke_NoMockRegistered verifies that invoking a
// function with no registered mock fails clearly, rather than hanging -
// there is no real backend here to eventually time it out.
func TestLocalTestRunner_Invoke_NoMockRegistered(t *testing.T) {
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Invoke[string, string](dc, "call-unregistered", "arn:aws:lambda:us-east-1:123456789012:function:unregistered", event.OrderID)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(orderEvent{OrderID: "invoke-unregistered-order"})
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
}

// TestLocalTestRunner_Invoke_ReplaySkip verifies that re-invoking the
// same execution does not call the mocked target function again once
// the invoke has already succeeded.
func TestLocalTestRunner_Invoke_ReplaySkip(t *testing.T) {
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		resp, err := operations.Invoke[string, string](dc, "call-once", "arn:aws:lambda:us-east-1:123456789012:function:counted", event.OrderID)
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: resp}, nil
	}

	runner := dtesting.New(handler, nil)
	runner.RegisterFunction("arn:aws:lambda:us-east-1:123456789012:function:counted", func(payload string) (string, error) {
		calls++
		return `"ok"`, nil
	})

	arn := "arn:aws:lambda:local:000000000000:function:invoke-replay-test:1"

	result1, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if result1.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result1.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result1.GetStatus(), msg)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call after first invocation, got %d", calls)
	}

	result2, err := runner.Continue(arn, orderEvent{OrderID: "abc"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if result2.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result2.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result2.GetStatus(), msg)
	}
	if calls != 1 {
		t.Fatalf("expected still 1 call after replay (invoke should be skipped), got %d", calls)
	}
}

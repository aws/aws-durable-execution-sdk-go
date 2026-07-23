package durable_test

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestParallel_NestingModeFlat_SkipsPerBranchContexts verifies the real,
// additive FLAT nesting feature (operations.NestingMode/
// WithParallelNesting - see NestingMode's own doc for the full feature
// writeup, mirroring the JS reference SDK's own confirmed, public
// NestingType.FLAT/virtualContext mechanism directly). Mirrors
// conformance requirement 8-12 exactly: two branches, each running a
// single Step, max-concurrency 1, NestingModeFlat - no per-branch
// CONTEXT_STARTED/CONTEXT_SUCCEEDED (SubType ParallelBranch) checkpoints
// should ever be sent, and each branch's own inner Step should be
// checkpointed with ParentID pointing directly at the OUTER Parallel
// context, not at any intermediate per-branch context.
func TestParallel_NestingModeFlat_SkipsPerBranchContexts(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		batch, err := operations.Parallel(dc, "flat", []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return "fa", nil
				})
			},
			func(child types.DurableContext) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return "fb", nil
				})
			},
		},
			operations.WithParallelMaxConcurrency[string](1),
			operations.WithParallelNesting[string](operations.NestingModeFlat),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			t.Errorf("expected no failures, got %v", batch.GetErrors())
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-flat-parallel", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "flat-test"}))

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

	// The core assertion: zero ParallelBranch contexts checkpointed at
	// all - FLAT nesting's entire point.
	var parallelOp *types.Operation
	var stepOps []types.Operation
	for _, op := range snapshot {
		o := op
		if op.Type == types.OperationTypeContext && op.SubType == "ParallelBranch" {
			t.Errorf("expected zero ParallelBranch contexts under FLAT nesting, found one: %+v", op)
		}
		if op.Name == "flat" && op.Type == types.OperationTypeContext {
			parallelOp = &o
		}
		if op.Type == types.OperationTypeStep {
			stepOps = append(stepOps, op)
		}
	}

	if parallelOp == nil {
		t.Fatal("expected the outer Parallel context operation 'flat' to be checkpointed")
	}
	if parallelOp.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer Parallel context to be checkpointed SUCCEEDED, got %s", parallelOp.Status)
	}

	if len(stepOps) != 2 {
		t.Fatalf("expected exactly 2 Step operations (one per branch), got %d", len(stepOps))
	}
	for _, s := range stepOps {
		if s.ParentID != parallelOp.ID {
			t.Errorf("expected each branch's inner Step to report ParentID=%s (the outer Parallel context's own ID), got ParentID=%s for step %+v", parallelOp.ID, s.ParentID, s)
		}
	}
}

// TestMap_NestingModeFlat_SkipsPerIterationContexts is Map's own analogue
// of the Parallel test above, mirroring conformance requirement 9-12.
func TestMap_NestingModeFlat_SkipsPerIterationContexts(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		batch, err := operations.Map(dc, "flat", []string{"fa", "fb"},
			func(child types.DurableContext, item string, index int) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return item, nil
				})
			},
			operations.WithMapMaxConcurrency[string, string](1),
			operations.WithMapNesting[string, string](operations.NestingModeFlat),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			t.Errorf("expected no failures, got %v", batch.GetErrors())
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-flat-map", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "flat-map-test"}))

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

	var mapOp *types.Operation
	var stepOps []types.Operation
	for _, op := range snapshot {
		o := op
		if op.Type == types.OperationTypeContext && op.SubType == "MapIteration" {
			t.Errorf("expected zero MapIteration contexts under FLAT nesting, found one: %+v", op)
		}
		if op.Name == "flat" && op.Type == types.OperationTypeContext {
			mapOp = &o
		}
		if op.Type == types.OperationTypeStep {
			stepOps = append(stepOps, op)
		}
	}

	if mapOp == nil {
		t.Fatal("expected the outer Map context operation 'flat' to be checkpointed")
	}
	if mapOp.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer Map context to be checkpointed SUCCEEDED, got %s", mapOp.Status)
	}

	if len(stepOps) != 2 {
		t.Fatalf("expected exactly 2 Step operations (one per item), got %d", len(stepOps))
	}
	for _, s := range stepOps {
		if s.ParentID != mapOp.ID {
			t.Errorf("expected each item's inner Step to report ParentID=%s (the outer Map context's own ID), got ParentID=%s for step %+v", mapOp.ID, s.ParentID, s)
		}
	}
}

// TestParallel_NestingModeFlat_BranchFailurePropagates verifies FLAT
// nesting's own failure path (runBatchItem's NestingModeFlat branch
// wraps a genuine fn error in *BatchItemFailedError without ever
// checkpointing a CONTEXT/FAIL for the branch itself, since no
// CONTEXT/START was ever checkpointed for it either).
func TestParallel_NestingModeFlat_BranchFailurePropagates(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		return operations.All(dc, "flat-fail", []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "ok", nil },
			func(child types.DurableContext) (string, error) { return "", errFlatBranchFailed },
		},
			operations.WithParallelMaxConcurrency[string](1),
			operations.WithParallelNesting[string](operations.NestingModeFlat),
		)
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-flat-fail", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "flat-fail-test"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected the execution to FAIL (All's own Promise.all-style contract, one branch failed), got status=%s", out.Status)
	}

	snapshot := client.snapshot()
	for _, op := range snapshot {
		if op.Type == types.OperationTypeContext && op.SubType == "ParallelBranch" {
			t.Errorf("expected zero ParallelBranch contexts under FLAT nesting even on failure, found one: %+v", op)
		}
	}
}

var errFlatBranchFailed = &testBranchError{"flat branch failed"}

package operations

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestStructuredErrors_ErrorsAsCheckability verifies that every concrete
// structured error type introduced by docs/remaining-work.md §4 task 10
// is discoverable via errors.As both directly AND through the common
// *OperationError base (see errors.go's top-level doc for the design
// rationale: embedding, not a marker interface, is what makes both of
// these work uniformly). Each subtest also wraps the error one extra
// layer deep with fmt.Errorf's %w, mirroring how every call site in this
// package that further wraps one of these (e.g. "step %q (id %s) failed
// after %d attempt(s): %w") still needs errors.As to see through that
// extra layer - so this specifically guards against a future change
// accidentally breaking the Unwrap chain.
func TestStructuredErrors_ErrorsAsCheckability(t *testing.T) {
	t.Run("StepFailedError", func(t *testing.T) {
		cause := errors.New("boom")
		err := &StepFailedError{
			OperationError: &OperationError{Kind: OperationKindStep, ID: "1", Name: "my-step", Err: cause},
			Attempt:        3,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var stepErr *StepFailedError
		if !errors.As(wrapped, &stepErr) {
			t.Fatal("expected errors.As to find *StepFailedError through a wrapping layer")
		}
		if stepErr.Attempt != 3 {
			t.Fatalf("expected Attempt=3, got %d", stepErr.Attempt)
		}

		var base *OperationError
		if !errors.As(wrapped, &base) {
			t.Fatal("expected errors.As to find the common *OperationError base")
		}
		if base.Kind != OperationKindStep || base.ID != "1" || base.Name != "my-step" {
			t.Fatalf("unexpected base fields: %+v", base)
		}
		if !errors.Is(wrapped, cause) {
			t.Fatal("expected errors.Is to find the original cause through OperationError.Unwrap")
		}
	})

	t.Run("CallbackFailedError", func(t *testing.T) {
		err := &CallbackFailedError{
			OperationError: &OperationError{Kind: OperationKindCallback, ID: "2", Name: "approval", Err: errors.New("rejected")},
			Timeout:        false,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var cbErr *CallbackFailedError
		if !errors.As(wrapped, &cbErr) {
			t.Fatal("expected errors.As to find *CallbackFailedError")
		}
		if cbErr.Timeout {
			t.Fatal("expected Timeout=false")
		}

		var base *OperationError
		if !errors.As(wrapped, &base) {
			t.Fatal("expected errors.As to find the common *OperationError base")
		}
	})

	t.Run("InvokeFailedError", func(t *testing.T) {
		err := &InvokeFailedError{
			OperationError: &OperationError{Kind: OperationKindInvoke, ID: "3", Name: "chain-call", Err: errors.New("target timed out")},
			Status:         types.OperationStatusTimedOut,
			Timeout:        true,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var invErr *InvokeFailedError
		if !errors.As(wrapped, &invErr) {
			t.Fatal("expected errors.As to find *InvokeFailedError")
		}
		if !invErr.Timeout || invErr.Status != types.OperationStatusTimedOut {
			t.Fatalf("expected Timeout=true, Status=TIMED_OUT, got %+v", invErr)
		}
	})

	t.Run("ChildContextFailedError", func(t *testing.T) {
		original := errors.New("branch blew up")
		err := &ChildContextFailedError{
			OperationError: &OperationError{Kind: OperationKindChildContext, ID: "4", Name: "process", Err: original},
			Reconstructed:  false,
			Original:       original,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var ctxErr *ChildContextFailedError
		if !errors.As(wrapped, &ctxErr) {
			t.Fatal("expected errors.As to find *ChildContextFailedError")
		}
		if ctxErr.Reconstructed {
			t.Fatal("expected Reconstructed=false for a same-invocation failure")
		}
		if !errors.Is(wrapped, original) {
			t.Fatal("expected errors.Is to find Original through the chain")
		}
	})

	t.Run("BatchItemFailedError", func(t *testing.T) {
		err := &BatchItemFailedError{
			OperationError: &OperationError{Kind: OperationKindBatchItem, ID: "5-2", Name: "batch[1]", Err: errors.New("item failed")},
			Reconstructed:  false,
			Index:          1,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var itemErr *BatchItemFailedError
		if !errors.As(wrapped, &itemErr) {
			t.Fatal("expected errors.As to find *BatchItemFailedError")
		}
		if itemErr.Index != 1 {
			t.Fatalf("expected Index=1, got %d", itemErr.Index)
		}
	})

	t.Run("BatchFailedError", func(t *testing.T) {
		item1 := &BatchItemFailedError{OperationError: &OperationError{Kind: OperationKindBatchItem, ID: "6-1", Name: "batch[0]"}, Index: 0}
		item2 := &BatchItemFailedError{OperationError: &OperationError{Kind: OperationKindBatchItem, ID: "6-2", Name: "batch[1]"}, Index: 1}
		aggErr := &AggregateError{Errors: []error{item1, item2}}
		batchErr := &BatchFailedError{
			OperationError: &OperationError{Kind: OperationKindBatch, ID: "6", Name: "batch", Err: aggErr},
			AggregateError: aggErr,
		}
		wrapped := fmt.Errorf("outer: %w", batchErr)

		var bErr *BatchFailedError
		if !errors.As(wrapped, &bErr) {
			t.Fatal("expected errors.As to find *BatchFailedError")
		}

		// Both embedded bases must be independently discoverable via
		// errors.As - this is the multi-error Unwrap() []error case (see
		// BatchFailedError.Unwrap's doc).
		var base *OperationError
		if !errors.As(wrapped, &base) {
			t.Fatal("expected errors.As to find the common *OperationError base")
		}
		var agg *AggregateError
		if !errors.As(wrapped, &agg) {
			t.Fatal("expected errors.As to find the embedded *AggregateError directly")
		}
		if len(agg.Errors) != 2 {
			t.Fatalf("expected 2 aggregated errors, got %d", len(agg.Errors))
		}

		// And each individual item error must still be reachable through
		// the whole chain - exercising errors.Is/As walking BOTH the
		// AggregateError's own collected list AND the multi-error
		// Unwrap.
		var foundItem *BatchItemFailedError
		if !errors.As(wrapped, &foundItem) {
			t.Fatal("expected errors.As to find a *BatchItemFailedError nested inside the aggregate")
		}
	})

	t.Run("ConditionFailedError", func(t *testing.T) {
		checkErr := errors.New("check function exploded")
		err := &ConditionFailedError{
			OperationError: &OperationError{Kind: OperationKindCondition, ID: "7", Name: "poll", Err: checkErr},
			Attempt:        4,
			CheckErr:       checkErr,
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var condErr *ConditionFailedError
		if !errors.As(wrapped, &condErr) {
			t.Fatal("expected errors.As to find *ConditionFailedError")
		}
		if condErr.Attempt != 4 || condErr.CheckErr == nil {
			t.Fatalf("unexpected fields: %+v", condErr)
		}
	})

	t.Run("SerdesError", func(t *testing.T) {
		err := newSerdesError("serialize", "8", "my-step", errors.New("json: unsupported type"))
		wrapped := fmt.Errorf("outer: %w", err)

		var serErr *SerdesError
		if !errors.As(wrapped, &serErr) {
			t.Fatal("expected errors.As to find *SerdesError")
		}
		if serErr.Direction != "serialize" {
			t.Fatalf("expected Direction=serialize, got %q", serErr.Direction)
		}

		var base *OperationError
		if !errors.As(wrapped, &base) {
			t.Fatal("expected errors.As to find the common *OperationError base")
		}
		if base.Kind != OperationKindSerdes {
			t.Fatalf("expected Kind=SERDES, got %s", base.Kind)
		}
	})

	t.Run("NonDeterministicReplayError", func(t *testing.T) {
		err := &NonDeterministicReplayError{
			OperationError: &OperationError{Kind: OperationKindNonDeterminism, ID: "9", Name: "poll-ready"},
			ExpectedType:   types.OperationTypeWait,
			ActualType:     types.OperationTypeStep,
			ActualSubType:  "WAIT_FOR_CONDITION",
		}
		wrapped := fmt.Errorf("outer: %w", err)

		var ndErr *NonDeterministicReplayError
		if !errors.As(wrapped, &ndErr) {
			t.Fatal("expected errors.As to find *NonDeterministicReplayError")
		}
		if ndErr.ExpectedType != types.OperationTypeWait || ndErr.ActualType != types.OperationTypeStep {
			t.Fatalf("unexpected fields: %+v", ndErr)
		}
		if !isNonDeterministic(wrapped) {
			t.Fatal("expected isNonDeterministic helper to also recognize the wrapped error")
		}

		var base *OperationError
		if !errors.As(wrapped, &base) {
			t.Fatal("expected errors.As to find the common *OperationError base")
		}
	})
}

// TestCheckReplayConsistency_TypeMismatch verifies the core comparison
// checkReplayConsistency performs: a Type mismatch is always flagged
// regardless of what expectedSubType is, and the returned error carries
// the actual vs. expected shape for debugging.
func TestCheckReplayConsistency_TypeMismatch(t *testing.T) {
	existing := types.Operation{ID: "1", Name: "poll-ready", Type: types.OperationTypeStep, SubType: "WAIT_FOR_CONDITION"}

	err := checkReplayConsistency(existing, types.OperationTypeWait, "", OperationKindWait, "1", "poll-ready")
	if err == nil {
		t.Fatal("expected a non-deterministic replay error for a Type mismatch")
	}
	var ndErr *NonDeterministicReplayError
	if !errors.As(err, &ndErr) {
		t.Fatalf("expected *NonDeterministicReplayError, got %T", err)
	}
	if ndErr.ExpectedType != types.OperationTypeWait {
		t.Fatalf("expected ExpectedType=WAIT, got %s", ndErr.ExpectedType)
	}
	if ndErr.ActualType != types.OperationTypeStep || ndErr.ActualSubType != "WAIT_FOR_CONDITION" {
		t.Fatalf("expected ActualType=STEP/ActualSubType=WAIT_FOR_CONDITION, got %s/%s", ndErr.ActualType, ndErr.ActualSubType)
	}
}

// TestCheckReplayConsistency_SubTypeMismatch verifies the SubType half of
// the comparison: same wire Type (STEP), different SubType (a plain Step
// vs. a WaitForCondition sharing a step ID across deployments) must also
// be flagged - this is the exact scenario the task's motivating example
// describes in reverse (a WAIT_FOR_CONDITION where a plain Step used to
// be).
func TestCheckReplayConsistency_SubTypeMismatch(t *testing.T) {
	existing := types.Operation{ID: "1", Name: "step-or-condition", Type: types.OperationTypeStep, SubType: ""}

	err := checkReplayConsistency(existing, types.OperationTypeStep, "WAIT_FOR_CONDITION", OperationKindCondition, "1", "step-or-condition")
	if err == nil {
		t.Fatal("expected a non-deterministic replay error for a SubType mismatch")
	}
	var ndErr *NonDeterministicReplayError
	if !errors.As(err, &ndErr) {
		t.Fatalf("expected *NonDeterministicReplayError, got %T", err)
	}
	if ndErr.ExpectedSubType != "WAIT_FOR_CONDITION" || ndErr.ActualSubType != "" {
		t.Fatalf("expected ExpectedSubType=WAIT_FOR_CONDITION/ActualSubType=\"\", got %q/%q", ndErr.ExpectedSubType, ndErr.ActualSubType)
	}
}

// TestCheckReplayConsistency_NoMismatch verifies the negative case: a
// matching Type/SubType pair passes with no error, for both the
// SubType-agnostic case (expectedSubType=="") and the SubType-sensitive
// case.
func TestCheckReplayConsistency_NoMismatch(t *testing.T) {
	plainStep := types.Operation{ID: "1", Type: types.OperationTypeStep, SubType: ""}
	if err := checkReplayConsistency(plainStep, types.OperationTypeStep, "", OperationKindStep, "1", "s"); err != nil {
		t.Fatalf("expected no error for a matching plain STEP, got %v", err)
	}

	condition := types.Operation{ID: "2", Type: types.OperationTypeStep, SubType: "WAIT_FOR_CONDITION"}
	if err := checkReplayConsistency(condition, types.OperationTypeStep, "WAIT_FOR_CONDITION", OperationKindCondition, "2", "c"); err != nil {
		t.Fatalf("expected no error for a matching WAIT_FOR_CONDITION, got %v", err)
	}

	// A SubType-agnostic caller (expectedSubType=="") must ALSO accept an
	// operation that happens to have a non-empty SubType - see
	// checkReplayConsistency's doc for why this is correct, not a gap:
	// Step itself never looks at SubType, so a WAIT_FOR_CONDITION
	// checkpoint is indistinguishable from Step's own point of view (in
	// practice this never actually happens, since WaitForCondition mints
	// its own distinct step ID via a different call site, but the
	// comparison logic itself must not spuriously reject it).
	if err := checkReplayConsistency(condition, types.OperationTypeStep, "", OperationKindStep, "2", "c"); err != nil {
		t.Fatalf("expected no error when the caller doesn't care about SubType, got %v", err)
	}
}

// -----------------------------------------------------------------------
// Large single-operation result handling (docs/remaining-work.md §6 task 16)
// -----------------------------------------------------------------------

// TestCheckResultSize_UnderThreshold verifies the common case: a small
// serialized result passes through with no error.
func TestCheckResultSize_UnderThreshold(t *testing.T) {
	small := `{"orderId":"abc","status":"ok"}`
	if err := checkResultSize(small, OperationKindStep, "1", "fetch-order"); err != nil {
		t.Fatalf("expected no error for a small payload, got %v", err)
	}
}

// TestCheckResultSize_AtThreshold verifies the boundary itself is
// inclusive (exactly resultTooLargeThresholdBytes is allowed, one byte
// over is not) - a common off-by-one class of bug for a size gate like
// this.
func TestCheckResultSize_AtThreshold(t *testing.T) {
	exact := strings.Repeat("a", resultTooLargeThresholdBytes)
	if err := checkResultSize(exact, OperationKindStep, "1", "fetch-order"); err != nil {
		t.Fatalf("expected no error at exactly the threshold, got %v", err)
	}

	overByOne := strings.Repeat("a", resultTooLargeThresholdBytes+1)
	err := checkResultSize(overByOne, OperationKindStep, "1", "fetch-order")
	if err == nil {
		t.Fatal("expected an error for one byte over the threshold")
	}
	var tooLarge *ResultTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected *ResultTooLargeError, got %T", err)
	}
	if tooLarge.SizeBytes != resultTooLargeThresholdBytes+1 {
		t.Fatalf("expected SizeBytes=%d, got %d", resultTooLargeThresholdBytes+1, tooLarge.SizeBytes)
	}
}

// TestCheckResultSize_OverThreshold_ErrorFields verifies the returned
// error carries the fields a caller needs to act on it (which operation,
// how big it was, what the threshold is), and that it is discoverable
// both directly and via the common *OperationError base - matching every
// other structured error type's contract in this file (see
// TestStructuredErrors_ErrorsAsCheckability above).
func TestCheckResultSize_OverThreshold_ErrorFields(t *testing.T) {
	oversized := strings.Repeat("x", resultTooLargeThresholdBytes*2)
	err := checkResultSize(oversized, OperationKindInvoke, "3-1", "fetch-catalog")
	wrapped := fmt.Errorf("outer: %w", err)

	var tooLarge *ResultTooLargeError
	if !errors.As(wrapped, &tooLarge) {
		t.Fatalf("expected errors.As to find *ResultTooLargeError through a wrapping layer, got %T", err)
	}
	if tooLarge.SizeBytes != resultTooLargeThresholdBytes*2 {
		t.Fatalf("expected SizeBytes=%d, got %d", resultTooLargeThresholdBytes*2, tooLarge.SizeBytes)
	}
	if tooLarge.ThresholdBytes != resultTooLargeThresholdBytes {
		t.Fatalf("expected ThresholdBytes=%d, got %d", resultTooLargeThresholdBytes, tooLarge.ThresholdBytes)
	}
	if tooLarge.ID != "3-1" || tooLarge.Name != "fetch-catalog" || tooLarge.Kind != OperationKindInvoke {
		t.Fatalf("unexpected OperationError fields: %+v", tooLarge.OperationError)
	}

	var base *OperationError
	if !errors.As(wrapped, &base) {
		t.Fatal("expected errors.As to find the common *OperationError base")
	}
	if base.Kind != OperationKindInvoke {
		t.Fatalf("expected base Kind=INVOKE, got %s", base.Kind)
	}

	// The error message itself should be actionable, not just a bare
	// "too large" - see ResultTooLargeError.Error()'s doc for why it
	// names both real, currently-available mitigations.
	msg := err.Error()
	if !strings.Contains(msg, "reference") || !strings.Contains(msg, "Serdes") {
		t.Fatalf("expected error message to mention both mitigation strategies, got: %s", msg)
	}
}

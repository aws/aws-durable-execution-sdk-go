package durable_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

type orderEvent struct {
	OrderID string `json:"orderId"`
}

type orderResult struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

func mustMarshalPayload(t *testing.T, v any) *string {
	t.Helper()
	// Reuse the SDK's default serdes so the wire format matches exactly
	// what the runtime itself would produce/consume.
	s, err := utils.DefaultSerdes().Serialize(v, "test", "")
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	return &s
}

// newInvocationInput builds a types.DurableExecutionInvocationInput
// matching the REAL shape confirmed from a live invocation (see
// docs/checkpoint-replay-design.md): the event is always delivered via
// InitialExecutionState.Operations containing a root EXECUTION operation
// whose ExecutionDetails.InputPayload holds the JSON-encoded event - there
// is no separate "first invocation" shape without it. executionID should
// be a distinct value per logical execution (mirroring the real backend's
// per-execution UUID-like operation ID) but any unique string works for
// tests. Additional replay operations (e.g. previously-completed steps)
// can be passed via extraOps.
func newInvocationInput(executionID, arn, checkpointToken string, eventPayload *string, extraOps ...types.Operation) types.DurableExecutionInvocationInput {
	ops := append([]types.Operation{{
		ID:     executionID,
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusStarted,
		ExecutionDetails: &types.ExecutionDetails{
			InputPayload: eventPayload,
		},
	}}, extraOps...)

	return types.DurableExecutionInvocationInput{
		DurableExecutionArn:   arn,
		CheckpointToken:       checkpointToken,
		InitialExecutionState: types.InitialExecutionState{Operations: ops},
	}
}

// expectedStepID computes the SHA-256 hashed step ID this SDK's
// dcontext.Context.NextStepID would mint for the Nth call (n, 1-based)
// within a context whose hash-input prefix is contextID (the root
// EXECUTION operation's own ID for a root context - see
// dcontext.NewRoot's doc - or a parent context's own already-hashed step
// ID for a child context - see dcontext.Context.NewChild's doc).
// Deliberately duplicates dcontext's private hashOperationID/
// hashInputPrefix formula here rather than importing/exporting it,
// exactly the way a black-box test SHOULD verify a hashing scheme: by
// independently recomputing the expected hash from the documented
// input-construction rule (this SDK's docs/remaining-work.md "SHA-256
// operation ID hashing" writeup, itself matching the confirmed real Java
// reference SDK's OperationIdGenerator), not by reaching into the
// package's internals to ask it what it computed.
func expectedStepID(contextID string, n int) string {
	input := fmt.Sprintf("%d", n)
	if contextID != "" {
		input = fmt.Sprintf("%s-%d", contextID, n)
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// TestStep_HappyPath verifies a single successful step: the handler runs
// to completion, the step's result is checkpointed, and the invocation
// returns Succeeded with the handler's result.
func TestStep_HappyPath(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-1", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}
	if out.ResultPayload == nil {
		t.Fatal("expected a result payload")
	}

	ops := client.snapshot()
	step, ok := ops[expectedStepID("exec-1", 1)]
	if !ok {
		t.Fatal("expected step '1' (hashed) to be checkpointed")
	}
	if step.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected step to be Succeeded, got %v", step.Status)
	}
}

// TestStep_ReplaySkip verifies that when the operation log already has a
// completed step, the step's function body is NOT re-executed - the
// checkpointed result is returned directly.
func TestStep_ReplaySkip(t *testing.T) {
	client := newFakeClient()
	calls := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			calls++
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		second, err := operations.Step(dc, "confirm", func(sc types.StepContext) (string, error) {
			calls++
			return "confirmed", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated + "/" + second}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "abc"})

	// First invocation: both steps run for real.
	firstInput := newInvocationInput("exec-2", "arn:test:1", "token-0", eventPayload)
	out1, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("first invocation error: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected first invocation Succeeded, got %s", out1.Status)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls after first invocation, got %d", calls)
	}

	// Second invocation ("replay"): seed InitialExecutionState with the
	// checkpointed step operations from the fake backend, plus the root
	// execution operation with the same event payload, exactly as a real
	// re-invocation would receive them.
	snapshot := client.snapshot()
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}

	secondInput := newInvocationInput("exec-2", "arn:test:1", "token-1", eventPayload, extraOps...)
	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation error: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second invocation Succeeded, got %s", out2.Status)
	}
	if calls != 2 {
		t.Fatalf("expected still 2 calls after replay (steps should be skipped), got %d", calls)
	}
}

// TestStep_RetryThenSucceed verifies that a step failing once, then
// succeeding on retry, checkpoints a RETRY action and ultimately returns
// the successful result - without the handler observing an error.
func TestStep_RetryThenSucceed(t *testing.T) {
	client := newFakeClient()
	attempts := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			attempts++
			if sc.Attempt() < 2 {
				return "", errors.New("transient failure")
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-3", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "xyz"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded after retry, got status=%s error=%v", out.Status, out.Error)
	}
	if attempts != 2 {
		t.Fatalf("expected exactly 2 attempts, got %d", attempts)
	}
}

// TestStep_FailsAfterExhaustingRetries verifies that a step which never
// succeeds, with retries disabled, fails the whole execution.
func TestStep_FailsAfterExhaustingRetries(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("permanent failure")
		}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-4", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "fail-me"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected Failed, got status=%s", out.Status)
	}
	if out.Error == nil {
		t.Fatal("expected an error message")
	}
}

// TestStep_OversizedResult_FailsClearly verifies docs/remaining-work.md §6
// task 16's end-to-end behavior: a step whose serialized result exceeds
// the single-operation checkpoint threshold fails the execution with a
// clear, actionable *operations.ResultTooLargeError - rather than the
// PRE-EXISTING behavior of silently enqueueing the oversized payload and
// letting the fake/real backend either accept it unrealistically or fail
// with an opaque low-level error. This is the "fail clearly instead of
// failing outright" fallback documented at length in
// operations/errors.go's "Large single-operation result handling" block
// (this session found no confirmed automatic offload mechanism to
// implement instead - see that block's doc for the full research).
//
// Critically, the step's checkpoint START/attempt bookkeeping must NOT
// have been corrupted by the rejected SUCCEED checkpoint: the size check
// happens strictly before Checkpoint().Enqueue is ever called for the
// oversized result, so no partial/invalid checkpoint reaches the fake
// backend at all - verified here by asserting the step never actually
// got checkpointed as Succeeded (fakeClient would otherwise have recorded
// an oversized Payload).
func TestStep_OversizedResult_FailsClearly(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Step(dc, "huge-result", func(sc types.StepContext) (string, error) {
			// One byte over checkpoint.DefaultLimits().MaxPayloadBytes
			// (750KB) once JSON-encoded - see operations/errors.go's
			// resultTooLargeThresholdBytes doc for why this specific
			// number is the real, confirmed threshold reused here.
			return strings.Repeat("a", 800*1024), nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "unreachable"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-oversized", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "big"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected Failed, got status=%s", out.Status)
	}
	if out.Error == nil {
		t.Fatal("expected an error message")
	}
	if !strings.Contains(out.Error.ErrorMessage, "exceeds the") || !strings.Contains(out.Error.ErrorMessage, "checkpoint threshold") {
		t.Fatalf("expected the ResultTooLargeError's own descriptive message to surface, got: %s", out.Error.ErrorMessage)
	}

	// The rejected step must never have reached a checkpointed Succeeded
	// state with the oversized payload - the whole point of checking
	// BEFORE Enqueue is that no such checkpoint is ever attempted.
	for _, op := range client.snapshot() {
		if op.Name == "huge-result" && op.Status == types.OperationStatusSucceeded {
			t.Fatal("expected the oversized step to never be checkpointed as Succeeded")
		}
	}
}

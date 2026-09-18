// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// --- Helpers ---

type orderInput struct {
	OrderID string `json:"orderId"`
	Amount  int    `json:"amount"`
}

type orderResult struct {
	Status  string `json:"status"`
	OrderID string `json:"orderId"`
}

// --- Test cases ---

func TestStepSucceeded(t *testing.T) {
	handler := func(ctx durable.Context, event orderInput) (orderResult, error) {
		result, err := durable.Step[string](ctx, "validate", func(_ durable.StepContext) (string, error) {
			return "validated-" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{Status: result, OrderID: event.OrderID}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.Run(t, orderInput{OrderID: "ord-123", Amount: 50})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[orderResult](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if output.Status != "validated-ord-123" {
		t.Errorf("output.Status = %q, want %q", output.Status, "validated-ord-123")
	}
	if output.OrderID != "ord-123" {
		t.Errorf("output.OrderID = %q, want %q", output.OrderID, "ord-123")
	}

	// Verify operations are tracked.
	if len(result.Operations) == 0 {
		t.Fatal("expected at least one operation, got none")
	}
	found := false
	for _, op := range result.Operations {
		if op.Name == "validate" && op.Status == "SUCCEEDED" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a SUCCEEDED operation named 'validate'")
	}
}

func TestStepMultiple(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		a, err := durable.Step[string](ctx, "step-a", func(_ durable.StepContext) (string, error) {
			return "A", nil
		})
		if err != nil {
			return "", err
		}
		b, err := durable.Step[string](ctx, "step-b", func(_ durable.StepContext) (string, error) {
			return "B", nil
		})
		if err != nil {
			return "", err
		}
		return a + b, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.Run(t, "input")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if output != "AB" {
		t.Errorf("result = %q, want %q", output, "AB")
	}
}

func TestWaitSuspends(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		if err := durable.Wait(ctx, "pause", 30*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.Run(t, "go")

	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Verify a WAIT operation was checkpointed with STARTED status.
	found := false
	for _, op := range result.Operations {
		if op.Name == "pause" && op.Type == "WAIT" && op.Status == "STARTED" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a STARTED WAIT operation named 'pause'")
	}
}

func TestHandlerFails(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "fail-step", func(_ durable.StepContext) (string, error) {
			return "", errors.New("something broke")
		}, durable.WithRetry(durable.NoRetry()))
		if err != nil {
			return "", err
		}
		return "unreachable", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.Run(t, "input")

	if result.Status != durabletest.Failed {
		t.Fatalf("expected FAILED, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error details, got nil")
	}
	if result.Error.Type != "StepError" {
		t.Errorf("error type = %q, want %q", result.Error.Type, "StepError")
	}
}

func TestReplayDoesNotReExecute(t *testing.T) {
	// Counter incremented by the step body. On replay, the body must NOT
	// re-execute, so the counter should stay at 1 after two Run calls.
	var counter atomic.Int32

	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Step[string](ctx, "counted", func(_ durable.StepContext) (string, error) {
			counter.Add(1)
			return "result", nil
		})
		if err != nil {
			return "", err
		}
		// Second step always suspends, forcing a re-invocation.
		if err := durable.Wait(ctx, "hold", 10*time.Second); err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// First invocation: step executes, wait suspends.
	r1 := runner.Run(t, "go")
	if r1.Status != durabletest.Pending {
		t.Fatalf("first Run: expected PENDING, got %s", r1.Status)
	}
	if counter.Load() != 1 {
		t.Fatalf("first Run: counter = %d, want 1", counter.Load())
	}

	// Second invocation: step replays (does NOT re-execute), wait
	// suspends again.
	r2 := runner.Run(t, "go")
	if r2.Status != durabletest.Pending {
		t.Fatalf("second Run: expected PENDING, got %s", r2.Status)
	}
	if counter.Load() != 1 {
		t.Fatalf("second Run: counter = %d, want 1 (replay should not re-execute)", counter.Load())
	}
}

func TestTokenRotation(t *testing.T) {
	// Each Run call triggers at least one checkpoint. Verify that
	// subsequent Runs observe a rotated token (they can successfully
	// invoke without errors, which would happen if the token is stale).
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "s1", func(_ durable.StepContext) (string, error) {
			return "one", nil
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "hold", 5*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// First invocation checkpoints the step, gets PENDING on wait.
	r1 := runner.Run(t, "go")
	if r1.Status != durabletest.Pending {
		t.Fatalf("first Run: expected PENDING, got %s", r1.Status)
	}

	// Second invocation replays step, suspends on wait again.
	// If token rotation is broken, checkpoint would fail.
	r2 := runner.Run(t, "go")
	if r2.Status != durabletest.Pending {
		t.Fatalf("second Run: expected PENDING, got %s", r2.Status)
	}
}

func TestGoroutineSafety(t *testing.T) {
	// This test runs with -race to detect data races. The handler itself
	// is sequential (no async ops in R1 scope), but the underlying
	// checkpoint client is mutex-guarded. Running multiple sequential
	// test scenarios in rapid succession exercises the locking paths.
	handler := func(ctx durable.Context, event int) (int, error) {
		sum := 0
		for i := range event {
			v, err := durable.Step[int](ctx, "", func(_ durable.StepContext) (int, error) {
				return i * 2, nil
			})
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	}

	// Run 10 independent runners concurrently. Each runner is
	// single-goroutine, but we exercise the race detector on the
	// test infrastructure itself.
	const workers = 10
	done := make(chan struct{}, workers)
	for range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			runner := durabletest.NewLocalRunner(handler)
			result := runner.Run(t, 3)
			if result.Status != durabletest.Succeeded {
				t.Errorf("expected SUCCEEDED, got %s", result.Status)
			}
		}()
	}
	for range workers {
		<-done
	}
}

func TestResultAsOnNonSucceeded(t *testing.T) {
	result := &durabletest.TestResult{Status: durabletest.Pending}
	_, err := durabletest.ResultAs[string](result)
	if err == nil {
		t.Error("ResultAs on PENDING should return error")
	}
}

func TestEmptyResult(t *testing.T) {
	type void struct{}
	handler := func(ctx durable.Context, event string) (void, error) {
		return void{}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.Run(t, "input")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[void](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	_ = output // void result
}

// --- R2 Tests: RunUntilComplete, CompletePendingTimers, Callbacks, ChainedInvoke, Accessors ---

func TestRunUntilCompleteStepWithRetry(t *testing.T) {
	// A step that fails twice then succeeds. RunUntilComplete should
	// auto-advance the retry timer (PENDING→READY) between invocations
	// and complete with the final result.
	var attempts int32
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Step[string](ctx, "flaky", func(_ durable.StepContext) (string, error) {
			attempts++
			if attempts < 3 {
				return "", fmt.Errorf("transient failure %d", attempts)
			}
			return "success", nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if output != "success" {
		t.Errorf("result = %q, want %q", output, "success")
	}
	// Default retry is 3 max attempts (ExponentialBackoff), so we expect
	// 3 total attempts.
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}

	// Verify step details.
	op := result.Operation("flaky")
	if op == nil {
		t.Fatal("operation 'flaky' not found")
	}
	if op.Status != "SUCCEEDED" {
		t.Errorf("op status = %q, want SUCCEEDED", op.Status)
	}
}

func TestRunUntilCompleteRetryableErrorsStopOnNonMatch(t *testing.T) {
	// A step whose first two attempts fail with a retryable error and whose
	// third fails with a non-matching one. Retries stop at the third
	// attempt although the strategy allows five, and the failure reports
	// the three attempts made.
	var attempts int32
	handler := func(ctx durable.Context, event string) (string, error) {
		return durable.Step[string](ctx, "guarded", func(_ durable.StepContext) (string, error) {
			attempts++
			if attempts < 3 {
				return "", fmt.Errorf("transient failure %d", attempts)
			}
			return "", errors.New("permanent failure")
		}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:     5,
			RetryableErrors: []durable.ErrorMatcher{durable.ErrorContains("transient")},
		})))
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Failed {
		t.Fatalf("expected FAILED, got %s", result.Status)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if result.Error == nil {
		t.Fatal("expected error details, got nil")
	}
	if result.Error.Type != "StepError" {
		t.Errorf("error type = %q, want StepError", result.Error.Type)
	}
	if want := "failed after 3 attempts"; !strings.Contains(result.Error.Message, want) {
		t.Errorf("error message %q does not contain %q", result.Error.Message, want)
	}
	if !strings.Contains(result.Error.Message, "permanent failure") {
		t.Errorf("error message %q does not carry the final attempt's error", result.Error.Message)
	}
	op := result.Operation("guarded")
	if op == nil {
		t.Fatal("operation 'guarded' not found")
	}
	if op.Status != "FAILED" {
		t.Errorf("op status = %q, want FAILED", op.Status)
	}
}

func TestRunUntilCompleteWaitAdvances(t *testing.T) {
	// A handler with a wait that should be auto-advanced by
	// RunUntilComplete.
	handler := func(ctx durable.Context, event string) (string, error) {
		if err := durable.Wait(ctx, "pause", 60*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if output != "done" {
		t.Errorf("result = %q, want %q", output, "done")
	}
}

func TestRunUntilCompleteBlocksOnCallback(t *testing.T) {
	// A handler that creates a callback. RunUntilComplete should return
	// PENDING since the callback requires external resolution.
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		result, err := cb.Result()
		if err != nil {
			return "", err
		}
		return "approved:" + result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "request")

	// Should be PENDING because callback needs external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}
}

func TestCallbackSuccessFlow(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		result, err := cb.Result()
		if err != nil {
			return "", err
		}
		return "got:" + result, nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// Run until blocked on callback.
	result := runner.RunUntilComplete(t, "input")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Enumerate open callbacks.
	cbs := runner.OpenCallbacks()
	if len(cbs) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(cbs))
	}
	if cbs[0].Name != "approval" {
		t.Errorf("callback name = %q, want %q", cbs[0].Name, "approval")
	}

	// Send callback success.
	if err := runner.SendCallbackSuccess(cbs[0].CallbackID, "yes"); err != nil {
		t.Fatalf("SendCallbackSuccess error: %v", err)
	}

	// Run again to complete.
	result = runner.RunUntilComplete(t, "input")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (error: %+v)", result.Status, result.Error)
	}

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	// SendCallbackSuccess JSON-serializes the payload. The SDK's
	// CreateCallback[string] deserializes it back, so the string
	// roundtrips correctly.
	if output != "got:yes" {
		t.Errorf("result = %q, want %q", output, "got:yes")
	}
}

func TestCallbackFailureSurfacesCallbackError(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		_, err = cb.Result()
		if err != nil {
			return "", err
		}
		return "unreachable", nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// Run until blocked on callback.
	result := runner.RunUntilComplete(t, "input")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Send callback failure.
	cbs := runner.OpenCallbacks()
	if len(cbs) == 0 {
		t.Fatal("no open callbacks")
	}
	if err := runner.SendCallbackFailure(cbs[0].CallbackID, "ValidationError", "bad input"); err != nil {
		t.Fatalf("SendCallbackFailure error: %v", err)
	}

	// Run again — the external failure surfaces as CallbackExternalError,
	// and the execution fails with that wire type and the external message.
	result = runner.RunUntilComplete(t, "input")
	if result.Status != durabletest.Failed {
		t.Fatalf("expected FAILED, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error details, got nil")
	}
	if result.Error.Type != "CallbackExternalError" {
		t.Errorf("error type = %q, want %q", result.Error.Type, "CallbackExternalError")
	}
	if result.Error.Message != "bad input" {
		t.Errorf("error message = %q, want %q", result.Error.Message, "bad input")
	}
}

func TestCallbackHeartbeat(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "long-running")
		if err != nil {
			return "", err
		}
		result, err := cb.Result()
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// Run until blocked on callback.
	result := runner.RunUntilComplete(t, "input")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	cbs := runner.OpenCallbacks()
	if len(cbs) == 0 {
		t.Fatal("no open callbacks")
	}

	// Heartbeat should succeed without error.
	if err := runner.SendCallbackHeartbeat(cbs[0].CallbackID); err != nil {
		t.Fatalf("SendCallbackHeartbeat error: %v", err)
	}

	// Heartbeat on non-existent callback should error.
	if err := runner.SendCallbackHeartbeat("nonexistent"); err == nil {
		t.Error("expected error for nonexistent callback, got nil")
	}
}

func TestChainedInvokeSuccess(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Invoke[string](ctx, "call-child", "arn:aws:lambda:us-east-1:123:function:child", event)
		if err != nil {
			return "", err
		}
		return "parent-got:" + result, nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// First run: invoke is started, suspends.
	result := runner.RunUntilComplete(t, "hello")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Complete the chained invoke.
	if err := runner.CompleteChainedInvoke("call-child", "child-result"); err != nil {
		t.Fatalf("CompleteChainedInvoke error: %v", err)
	}

	// Run again — should succeed.
	result = runner.RunUntilComplete(t, "hello")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (error: %+v)", result.Status, result.Error)
	}

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	// The payload is JSON-serialized by CompleteChainedInvoke, and the
	// SDK deserializes it with the result serdes. json.Marshal("child-result")
	// produces `"child-result"` (with quotes), so the SDK reads back
	// "child-result" as a string.
	if output != "parent-got:child-result" {
		t.Errorf("result = %q, want %q", output, "parent-got:child-result")
	}
}

func TestChainedInvokeFailure(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Invoke[string](ctx, "call-child", "arn:aws:lambda:us-east-1:123:function:child", event)
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// First run: invoke is started, suspends.
	result := runner.RunUntilComplete(t, "hello")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Fail the chained invoke.
	if err := runner.FailChainedInvoke("call-child", "ChildError", "child crashed"); err != nil {
		t.Fatalf("FailChainedInvoke error: %v", err)
	}

	// Run again — should fail with InvokeError.
	result = runner.RunUntilComplete(t, "hello")
	if result.Status != durabletest.Failed {
		t.Fatalf("expected FAILED, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error details")
	}
	if result.Error.Type != "InvokeError" {
		t.Errorf("error type = %q, want %q", result.Error.Type, "InvokeError")
	}
}

func TestInvocationCapReached(t *testing.T) {
	// A handler that retries forever (never succeeds). With a cap of 3
	// invocations, RunUntilComplete should stop and report CapReached.
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "infinite", func(_ durable.StepContext) (string, error) {
			return "", fmt.Errorf("always fails")
		})
		if err != nil {
			return "", err
		}
		return "unreachable", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go", durabletest.WithMaxInvocations(3))

	// Should return with CapReached since the step retries forever.
	if !result.CapReached {
		t.Error("expected CapReached to be true")
	}
	// The status should still be PENDING (RETRY → PENDING → advance → READY → re-invoke).
	// Actually: the step fails, checkpoints RETRY (status=PENDING), suspends.
	// completePendingTimers flips PENDING→READY, runner re-invokes. Step re-executes, fails, RETRY again.
	// After 3 iterations, returns.
	if result.Status != durabletest.Pending && result.Status != durabletest.Failed {
		t.Errorf("expected PENDING or FAILED, got %s", result.Status)
	}
}

func TestCompletePendingTimersManual(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		if err := durable.Wait(ctx, "timer", 30*time.Second); err != nil {
			return "", err
		}
		return "timed", nil
	}

	runner := durabletest.NewLocalRunner(handler)

	// Single invocation: wait suspends.
	result := runner.Run(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Manually complete the pending wait timer.
	advanced := runner.CompletePendingTimers()
	if !advanced {
		t.Error("expected CompletePendingTimers to return true")
	}

	// Second call should return false (nothing left to complete).
	advanced = runner.CompletePendingTimers()
	if advanced {
		t.Error("expected CompletePendingTimers to return false")
	}

	// Re-invoke: wait is SUCCEEDED, handler completes.
	result = runner.Run(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
}

func TestOperationAccessorByName(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		a, _ := durable.Step[string](ctx, "step-a", func(_ durable.StepContext) (string, error) {
			return "A", nil
		})
		b, _ := durable.Step[string](ctx, "step-b", func(_ durable.StepContext) (string, error) {
			return "B", nil
		})
		return a + b, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	op := result.Operation("step-a")
	if op == nil {
		t.Fatal("operation 'step-a' not found")
	}
	if op.Type != "STEP" {
		t.Errorf("type = %q, want STEP", op.Type)
	}
	if op.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want SUCCEEDED", op.Status)
	}
	if op.StepDetails == nil {
		t.Fatal("StepDetails is nil")
	}

	// Typed accessor.
	val, err := durabletest.OperationResultAs[string](op)
	if err != nil {
		t.Fatalf("OperationResultAs error: %v", err)
	}
	if val != "A" {
		t.Errorf("step result = %q, want %q", val, "A")
	}

	// Not found.
	if result.Operation("nonexistent") != nil {
		t.Error("expected nil for nonexistent operation")
	}
}

func TestOperationAccessorByIndex(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "first", func(_ durable.StepContext) (string, error) { return "1", nil })
		if err != nil {
			return "", err
		}
		_, err = durable.Step[string](ctx, "second", func(_ durable.StepContext) (string, error) { return "2", nil })
		if err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	op := result.OperationByIndex(0)
	if op == nil {
		t.Fatal("index 0 returned nil")
	}
	if op.Name != "first" {
		t.Errorf("name = %q, want %q", op.Name, "first")
	}

	op = result.OperationByIndex(1)
	if op == nil {
		t.Fatal("index 1 returned nil")
	}
	if op.Name != "second" {
		t.Errorf("name = %q, want %q", op.Name, "second")
	}

	// Out of bounds.
	if result.OperationByIndex(-1) != nil {
		t.Error("expected nil for negative index")
	}
	if result.OperationByIndex(100) != nil {
		t.Error("expected nil for out-of-bounds index")
	}
}

func TestOperationAccessorByID(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "named", func(_ durable.StepContext) (string, error) { return "ok", nil })
		if err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	// Get the first operation's ID.
	if len(result.Operations) == 0 {
		t.Fatal("no operations")
	}
	id := result.Operations[0].ID
	if id == "" {
		t.Fatal("operation ID is empty")
	}

	op := result.OperationByID(id)
	if op == nil {
		t.Fatalf("OperationByID(%q) returned nil", id)
	}
	if op.Name != "named" {
		t.Errorf("name = %q, want %q", op.Name, "named")
	}

	// Not found.
	if result.OperationByID("nonexistent-id") != nil {
		t.Error("expected nil for nonexistent ID")
	}
}

func TestOperationsByType(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		_, err := durable.Step[string](ctx, "s1", func(_ durable.StepContext) (string, error) { return "a", nil })
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w1", time.Second); err != nil {
			return "", err
		}
		_, err = durable.Step[string](ctx, "s2", func(_ durable.StepContext) (string, error) { return "b", nil })
		if err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	steps := result.OperationsByType("STEP")
	if len(steps) != 2 {
		t.Fatalf("expected 2 STEP operations, got %d", len(steps))
	}

	waits := result.OperationsByType("WAIT")
	if len(waits) != 1 {
		t.Fatalf("expected 1 WAIT operation, got %d", len(waits))
	}
}

func TestIsTerminal(t *testing.T) {
	tests := []struct {
		status   string
		terminal bool
	}{
		{"SUCCEEDED", true},
		{"FAILED", true},
		{"CANCELLED", true},
		{"TIMED_OUT", true},
		{"STOPPED", true},
		{"STARTED", false},
		{"PENDING", false},
		{"READY", false},
	}
	for _, tc := range tests {
		op := durabletest.TestOperation{Status: tc.status}
		if got := op.IsTerminal(); got != tc.terminal {
			t.Errorf("IsTerminal(%q) = %v, want %v", tc.status, got, tc.terminal)
		}
	}
}

// TestWaitForConditionStateRoundTripUnderLocalRunner asserts that the
// intermediate state a polling step checkpoints on RETRY is read back by
// the next attempt under the LocalRunner, so a wait strategy that depends
// on accumulated state converges.
func TestWaitForConditionStateRoundTripUnderLocalRunner(t *testing.T) {
	var seenStates []int
	h := func(ctx durable.Context, _ any) (int, error) {
		return durable.WaitForCondition(ctx, "c", func(_ durable.StepContext, s int) (int, error) {
			seenStates = append(seenStates, s)
			return s + 1, nil
		}, durable.ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) durable.WaitDecision {
				if state >= 3 {
					return durable.WaitDecision{}
				}
				return durable.WaitDecision{Continue: true, Delay: time.Second}
			},
		})
	}
	res := durabletest.NewLocalRunner(h).RunUntilComplete(t, nil, durabletest.WithMaxInvocations(6))
	if res.Status != durabletest.Succeeded {
		t.Fatalf("WaitForCondition never converged: status=%v capReached=%v statesSeen=%v", res.Status, res.CapReached, seenStates)
	}
	if res.CapReached {
		t.Errorf("CapReached = true, want false")
	}
	wantSeen := []int{0, 1, 2}
	if fmt.Sprint(seenStates) != fmt.Sprint(wantSeen) {
		t.Errorf("statesSeen = %v, want %v", seenStates, wantSeen)
	}
	out, err := durabletest.ResultAs[int](res)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if out != 3 {
		t.Errorf("result = %d, want 3", out)
	}
}

// TestWaitForConditionAttemptNumber asserts that a condition check observes
// the 1-based poll attempt number through StepContext.Attempt, the same
// value the wait strategy receives.
func TestWaitForConditionAttemptNumber(t *testing.T) {
	var seen []int
	var strategySeen []int
	h := func(ctx durable.Context, _ any) (int, error) {
		return durable.WaitForCondition(ctx, "c", func(sc durable.StepContext, s int) (int, error) {
			seen = append(seen, sc.Attempt())
			return s + 1, nil
		}, durable.ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, attempt int) durable.WaitDecision {
				strategySeen = append(strategySeen, attempt)
				if state >= 3 {
					return durable.WaitDecision{}
				}
				return durable.WaitDecision{Continue: true, Delay: time.Second}
			},
		})
	}
	res := durabletest.NewLocalRunner(h).RunUntilComplete(t, nil, durabletest.WithMaxInvocations(6))
	if res.Status != durabletest.Succeeded {
		t.Fatalf("WaitForCondition never converged: status=%v capReached=%v attemptsSeen=%v", res.Status, res.CapReached, seen)
	}
	if len(seen) != 3 {
		t.Fatalf("check ran %d times, want 3 (attempts seen %v)", len(seen), seen)
	}
	for i, a := range seen {
		if a != i+1 {
			t.Errorf("check %d saw StepContext.Attempt()=%d, want %d", i+1, a, i+1)
		}
	}
	if fmt.Sprint(strategySeen) != fmt.Sprint(seen) {
		t.Errorf("wait strategy saw attempts %v, check saw %v; want equal", strategySeen, seen)
	}
}

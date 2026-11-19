// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestPauseEveryPausesAndResumes runs a step that fails its first four
// attempts with PauseEvery 2. The execution pauses after attempts 2 and 4,
// and each resume runs the next attempt. Attempt 5 succeeds.
func TestPauseEveryPausesAndResumes(t *testing.T) {
	var attempts []int
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.Step(ctx, "flaky", func(sc durable.StepContext) (string, error) {
			attempts = append(attempts, sc.Attempt())
			if sc.Attempt() < 5 {
				return "", fmt.Errorf("attempt %d failed", sc.Attempt())
			}
			return "done", nil
		}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts: 10, InitialDelay: time.Second, Jitter: durable.JitterNone, PauseEvery: 2,
		})))
	}
	runner := durabletest.NewLocalRunner(handler)

	for _, pausedAfter := range []int{2, 4} {
		result, err := runner.RunUntilComplete("in")
		if err != nil {
			t.Fatalf("RunUntilComplete() error = %v", err)
		}
		if result.Status != durabletest.Pending {
			t.Fatalf("status = %s, want PENDING after attempt %d", result.Status, pausedAfter)
		}
		paused, reason := runner.Paused()
		if !paused {
			t.Fatalf("Paused() = false after attempt %d, want true", pausedAfter)
		}
		if want := fmt.Sprintf("Step %q failed attempt %d", "flaky", pausedAfter); reason != want {
			t.Errorf("reason = %q, want %q", reason, want)
		}
		if got := attempts[len(attempts)-1]; got != pausedAfter {
			t.Errorf("last attempt = %d, want %d", got, pausedAfter)
		}
		if _, err := runner.Run("in"); !errors.Is(err, durabletest.ErrExecutionPaused) {
			t.Errorf("Run() while paused error = %v, want ErrExecutionPaused", err)
		}
		if err := runner.ResumeExecution(); err != nil {
			t.Fatalf("ResumeExecution() error = %v", err)
		}
	}

	result, err := runner.RunUntilComplete("in")
	if err != nil {
		t.Fatalf("RunUntilComplete() error = %v", err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got, want := fmt.Sprint(attempts), "[1 2 3 4 5]"; got != want {
		t.Errorf("attempts = %s, want %s", got, want)
	}
	if paused, _ := runner.Paused(); paused {
		t.Error("Paused() = true after the resume, want false")
	}
}

// TestWaitForCallbackSubmitterHonorsPause checks that the submitter of
// WaitForCallback, which runs as a step, pauses the execution.
func TestWaitForCallbackSubmitterHonorsPause(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.WaitForCallback[string](ctx, "approval", func(sc durable.StepContext, _ string) error {
			if sc.Attempt() == 1 {
				return errors.New("submit failed")
			}
			return nil
		}, durable.WithSubmitterRetry(func(durable.RetryAttempt) durable.RetryDecision {
			return durable.RetryDecision{Retry: true, Delay: time.Second, Pause: true}
		}))
	}
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete("in")
	if err != nil {
		t.Fatalf("RunUntilComplete() error = %v", err)
	}
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if paused, _ := runner.Paused(); !paused {
		t.Fatal("Paused() = false, want true")
	}
}

func TestResumeExecutionWhenNotPaused(t *testing.T) {
	runner := durabletest.NewLocalRunner(func(durable.Context, string) (string, error) { return "x", nil })
	err := runner.ResumeExecution()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ResourceConflictException" {
		t.Errorf("ResumeExecution() error = %v, want ResourceConflictException", err)
	}
}

// pauseFirstAttempt returns a handler with one step that fails attempt 1
// and pauses the execution, then succeeds on attempt 2.
func pauseFirstAttempt() func(durable.Context, string) (string, error) {
	return func(ctx durable.Context, _ string) (string, error) {
		return durable.Step(ctx, "s", func(sc durable.StepContext) (string, error) {
			if sc.Attempt() == 1 {
				return "", errors.New("transient")
			}
			return "ok", nil
		}, durable.WithRetry(func(durable.RetryAttempt) durable.RetryDecision {
			return durable.RetryDecision{Retry: true, Delay: time.Second, Pause: true}
		}))
	}
}

func TestRunUntilCompleteWhilePaused(t *testing.T) {
	runner := durabletest.NewLocalRunner(pauseFirstAttempt())
	if _, err := runner.RunUntilComplete("in"); err != nil {
		t.Fatalf("RunUntilComplete() error = %v", err)
	}
	result, err := runner.RunUntilComplete("in")
	if !errors.Is(err, durabletest.ErrExecutionPaused) {
		t.Errorf("RunUntilComplete() while paused error = %v, want ErrExecutionPaused", err)
	}
	if result != nil {
		t.Errorf("RunUntilComplete() while paused result = %+v, want nil", result)
	}
}

func TestResetClearsPause(t *testing.T) {
	runner := durabletest.NewLocalRunner(pauseFirstAttempt())
	if _, err := runner.RunUntilComplete("in"); err != nil {
		t.Fatalf("RunUntilComplete() error = %v", err)
	}
	if paused, _ := runner.Paused(); !paused {
		t.Fatal("Paused() = false, want true")
	}
	runner.Reset()
	if paused, reason := runner.Paused(); paused || reason != "" {
		t.Errorf("Paused() after Reset = %v, %q, want false and no reason", paused, reason)
	}
	result, err := runner.Run("in")
	if err != nil {
		t.Fatalf("Run() after Reset error = %v", err)
	}
	if result.Status != durabletest.Pending {
		t.Errorf("status = %s, want PENDING from attempt 1 of a new execution", result.Status)
	}
}

// A registered durable target that pauses stays PENDING. The caller's
// invoke stays STARTED, and the runner reports the caller PENDING.
func TestRegisteredDurableTargetPauses(t *testing.T) {
	var targetAttempts int
	pricing := func(ctx durable.Context, req priceRequest) (priceQuote, error) {
		return durable.Step(ctx, "compute", func(sc durable.StepContext) (priceQuote, error) {
			targetAttempts = sc.Attempt()
			return priceQuote{}, errors.New("transient")
		}, durable.WithRetry(func(durable.RetryAttempt) durable.RetryDecision {
			return durable.RetryDecision{Retry: true, Delay: time.Second, Pause: true}
		}))
	}
	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	for i := range 2 {
		result, err := runner.RunUntilComplete(priceRequest{SKU: "a", Quantity: 1})
		if err != nil {
			t.Fatalf("RunUntilComplete() #%d error = %v", i+1, err)
		}
		if result.Status != durabletest.Pending {
			t.Fatalf("RunUntilComplete() #%d status = %s, want PENDING", i+1, result.Status)
		}
		if op := result.Operation("quote"); op == nil || op.Status != "STARTED" {
			t.Fatalf("quote operation = %+v, want STARTED", op)
		}
	}
	if targetAttempts != 1 {
		t.Errorf("target attempts = %d, want 1: a paused target is not invoked again", targetAttempts)
	}
	if paused, _ := runner.Paused(); paused {
		t.Error("Paused() = true, want false: only the target paused")
	}
}

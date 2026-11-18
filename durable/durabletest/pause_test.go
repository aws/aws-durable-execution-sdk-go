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

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// The service's error messages for a checkpoint payload over its limit.
const (
	stepPayloadMessage = "STEP output payload size must be less than or equal to 262144 bytes."
	errorObjectMessage = "Error object size must be less than or equal to 262144 bytes."
	invokeInputMessage = "CHAINED_INVOKE input payload size must be less than or equal to 1048576 bytes."
	executionResultMsg = "1 validation error detected: Value at 'updates.1.member.payload' failed to satisfy constraint: Member must have length less than or equal to 6291456"
	zeroWaitMessage    = "1 validation error detected: Value '0' at 'updates.1.member.waitOptions.waitSeconds' failed to satisfy constraint: Member must have value greater than or equal to 1"
)

// errorRecorder keeps the error an operation returned inside the handler,
// so a test can inspect it after the run.
type errorRecorder struct {
	mu  sync.Mutex
	err error
}

func (r *errorRecorder) set(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func (r *errorRecorder) get() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// assertServiceRejection checks that err is a *durable.CheckpointError
// that wraps the service's API error with the given code and message.
func assertServiceRejection(t *testing.T, err error, code, message string) {
	t.Helper()
	var ce *durable.CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("operation error = %v (%T), want *durable.CheckpointError", err, err)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("operation error %v wraps no smithy.APIError", err)
	}
	if apiErr.ErrorCode() != code {
		t.Errorf("error code = %q, want %q", apiErr.ErrorCode(), code)
	}
	if apiErr.ErrorMessage() != message {
		t.Errorf("error message = %q, want %q", apiErr.ErrorMessage(), message)
	}
	if apiErr.ErrorFault() != smithy.FaultClient {
		t.Errorf("error fault = %v, want client fault", apiErr.ErrorFault())
	}
}

// assertFailedWith checks that the execution failed and that its recorded
// error carries message.
func assertFailedWith(t *testing.T, result *durabletest.TestResult, message string) {
	t.Helper()
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || !strings.Contains(result.Error.Message, message) {
		t.Fatalf("execution error = %+v, want a message containing %q", result.Error, message)
	}
}

// runToEnd runs h to completion and fails the test on a runner failure.
func runToEnd[O any](t *testing.T, h durable.Handler[struct{}, O]) *durabletest.TestResult {
	t.Helper()
	result, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// stepOfSize returns a handler whose step returns a string that serializes
// to exactly size bytes, and records the step's error in rec.
func stepOfSize(size int, rec *errorRecorder) durable.Handler[struct{}, int] {
	return func(ctx durable.Context, _ struct{}) (int, error) {
		// The serialized string carries two quote bytes.
		s, err := durable.Step(ctx, "big", func(_ durable.StepContext) (string, error) {
			return strings.Repeat("x", size-2), nil
		})
		if err != nil {
			rec.set(err)
			return 0, err
		}
		return len(s), nil
	}
}

func TestStepResultOverServiceLimitIsRejected(t *testing.T) {
	var rec errorRecorder
	result := runToEnd(t, stepOfSize(262145, &rec))

	assertServiceRejection(t, rec.get(), "InvalidParameterValueException", stepPayloadMessage)
	assertFailedWith(t, result, stepPayloadMessage)
}

func TestStepResultAtServiceLimitIsAccepted(t *testing.T) {
	var rec errorRecorder
	result := runToEnd(t, stepOfSize(262144, &rec))

	if err := rec.get(); err != nil {
		t.Fatalf("step error = %v, want nil", err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s (error %+v), want SUCCEEDED", result.Status, result.Error)
	}
}

func TestWaitForConditionStateOverServiceLimitIsRejected(t *testing.T) {
	var rec errorRecorder
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		state, err := durable.WaitForCondition(ctx, "grow",
			func(_ durable.StepContext, s string) (string, error) {
				return strings.Repeat("x", 300000), nil
			},
			durable.ConditionConfig[string]{WaitStrategy: func(string, int) durable.WaitDecision {
				return durable.WaitDecision{Continue: true, Delay: time.Second}
			}})
		if err != nil {
			rec.set(err)
			return 0, err
		}
		return len(state), nil
	}
	result := runToEnd(t, h)

	assertServiceRejection(t, rec.get(), "InvalidParameterValueException", stepPayloadMessage)
	assertFailedWith(t, result, stepPayloadMessage)
}

func TestErrorObjectOverServiceLimitIsRejected(t *testing.T) {
	var rec errorRecorder
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		_, err := durable.Step(ctx, "fails", func(_ durable.StepContext) (string, error) {
			return "", errors.New(strings.Repeat("e", 300000))
		}, durable.WithRetry(durable.NoRetry()))
		if err != nil {
			rec.set(err)
			return "", err
		}
		return "unreachable", nil
	}
	result := runToEnd(t, h)

	assertServiceRejection(t, rec.get(), "InvalidParameterValueException", errorObjectMessage)
	assertFailedWith(t, result, errorObjectMessage)
}

func TestInvokeInputOverServiceLimitIsRejected(t *testing.T) {
	var rec errorRecorder
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		out, err := durable.Invoke[string](ctx, "call", "target:$LATEST", strings.Repeat("x", 1048576))
		if err != nil {
			rec.set(err)
			return "", err
		}
		return out, nil
	}
	result := runToEnd(t, h)

	assertServiceRejection(t, rec.get(), "InvalidParameterValueException", invokeInputMessage)
	assertFailedWith(t, result, invokeInputMessage)
}

func TestExecutionResultOverServiceLimitIsRejected(t *testing.T) {
	h := func(_ durable.Context, _ struct{}) (string, error) {
		return strings.Repeat("x", 6291456), nil
	}
	result := runToEnd(t, h)

	assertFailedWith(t, result, executionResultMsg)
	if !strings.Contains(result.Error.Message, "ValidationException") {
		t.Errorf("execution error = %q, want the ValidationException code", result.Error.Message)
	}
}

// The SDK rounds a wait's duration up to whole seconds, so a zero duration
// is the one under a second that produces WaitSeconds 0.
func TestWaitUnderOneSecondIsRejected(t *testing.T) {
	var rec errorRecorder
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		if err := durable.Wait(ctx, "short", 0); err != nil {
			rec.set(err)
			return "", err
		}
		return "waited", nil
	}
	result := runToEnd(t, h)

	assertServiceRejection(t, rec.get(), "ValidationException", zeroWaitMessage)
	assertFailedWith(t, result, zeroWaitMessage)
	if op := result.Operation("short"); op != nil {
		t.Errorf("short = %+v, want no stored wait: the client stores nothing from a rejected request", op)
	}
}

func TestPendingWithNothingPendingFailsAfterFourInvocations(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		return durable.Race[int](ctx, "r", nil)
	}
	result := runToEnd(t, h)

	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "InvalidParameterValueException" ||
		result.Error.Message != "Cannot return PENDING status with no pending operations." {
		t.Fatalf("error = %+v, want InvalidParameterValueException: Cannot return PENDING status with no pending operations.", result.Error)
	}
	if n := len(result.Invocations); n != 4 {
		t.Errorf("invocations = %d, want 4", n)
	}
	if result.CapReached {
		t.Error("CapReached = true, want false")
	}
}

// A PENDING response with a pending operation is a legitimate suspension.
// It neither counts toward the rejection nor fails the execution.
func TestPendingWithPendingOperationIsAccepted(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.WaitForCallback[string](ctx, "approval", func(durable.StepContext, string) error { return nil })
	}
	runner := durabletest.NewLocalRunner(h)
	for i := range 5 {
		result, err := runner.Run(struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != durabletest.Pending {
			t.Fatalf("run %d: status = %s (error %+v), want PENDING", i+1, result.Status, result.Error)
		}
	}
}

// A wait that elapses while a concurrent step runs is reported SUCCEEDED in
// the response to a checkpoint request of that step, so one Run completes.
func TestWaitReportedDuringInvocation(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		pause := durable.WaitAsync(ctx, "pause", time.Second)
		time.Sleep(50 * time.Millisecond) // the wait START reaches the client first
		if _, err := durable.Step(ctx, "work", func(_ durable.StepContext) (string, error) {
			return "worked", nil
		}); err != nil {
			return "", err
		}
		if _, err := pause.Result(); err != nil {
			return "", err
		}
		return "done", nil
	}
	result, err := durabletest.NewLocalRunner(h).Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED in one Run", result.Status)
	}
	if n := len(result.Invocations); n != 1 {
		t.Errorf("invocations = %d, want 1", n)
	}
	if op := result.Operation("pause"); op == nil || op.Status != "SUCCEEDED" {
		t.Errorf("pause = %+v, want SUCCEEDED", op)
	}
}

// A step retry whose next attempt becomes due while a concurrent step runs
// is reported READY in the same invocation, so the retry runs there.
func TestStepRetryReportedReadyDuringInvocation(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		strategy, err := durable.NewRetryStrategy(durable.RetryConfig{MaxAttempts: 2, InitialDelay: time.Second, Jitter: durable.JitterNone})
		if err != nil {
			return "", err
		}
		flaky := durable.StepAsync(ctx, "flaky", func(sc durable.StepContext) (string, error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			if sc.Attempt() == 1 {
				return "", errors.New("transient")
			}
			return "recovered", nil
		}, durable.WithRetry(strategy))
		time.Sleep(50 * time.Millisecond) // attempt 1 and its RETRY reach the client first
		if _, err := durable.Step(ctx, "work", func(_ durable.StepContext) (string, error) {
			return "worked", nil
		}); err != nil {
			return "", err
		}
		return flaky.Result()
	}
	result, err := durabletest.NewLocalRunner(h).Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s (error %+v), want SUCCEEDED in one Run", result.Status, result.Error)
	}
	if n := len(result.Invocations); n != 1 {
		t.Errorf("invocations = %d, want 1", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Errorf("flaky attempts = %d, want 2", attempts)
	}
}

// A sequential wait still suspends the invocation: no other work sends a
// checkpoint request while it is pending.
func TestSequentialWaitStillSuspends(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		if err := durable.Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
	runner := durabletest.NewLocalRunner(h)
	first, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", first.Status)
	}
	op := first.Operation("pause")
	if op == nil || op.Status != "STARTED" {
		t.Fatalf("pause = %+v, want STARTED", op)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("CompletePendingTimers completed nothing")
	}
	second, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", second.Status)
	}
}

// awaitedFinishedCase is one of the ten cases of the SDK's test of an
// awaited operation that finishes during the invocation, driven through the
// local runner instead of a fake client.
type awaitedFinishedCase struct {
	name    string
	want    durabletest.ExecutionStatus
	handler func(runner func() *durabletest.LocalRunner[struct{}, string]) durable.Handler[struct{}, string]
	setup   func(*durabletest.LocalRunner[struct{}, string])
}

// longStep is a step named "work" that outlasts the operation the handler
// awaits.
func longStep(c durable.Context) error {
	_, err := durable.Step(c, "work", func(durable.StepContext) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "worked", nil
	})
	return err
}

// targetFailure is the error the failing invoke target returns.
type targetFailure struct{}

func (targetFailure) Error() string {
	return "You cannot invoke a durable function using an unqualified ARN."
}

func awaitedFinishedCases() []awaitedFinishedCase {
	noRunner := func(h durable.Handler[struct{}, string]) func(func() *durabletest.LocalRunner[struct{}, string]) durable.Handler[struct{}, string] {
		return func(func() *durabletest.LocalRunner[struct{}, string]) durable.Handler[struct{}, string] { return h }
	}
	registerTargets := func(r *durabletest.LocalRunner[struct{}, string]) {
		r.RegisterFunction("target:$LATEST", durabletest.PlainFunction(func(context.Context, string) (string, error) {
			return "invoked", nil
		}))
		r.RegisterFunction("target", durabletest.PlainFunction(func(context.Context, string) (string, error) {
			return "", targetFailure{}
		}))
	}
	return []awaitedFinishedCase{
		{name: "01 WaitAsync, then a step, then Result", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				pause := durable.WaitAsync(ctx, "pause", time.Second)
				if err := longStep(ctx); err != nil {
					return "", err
				}
				_, err := pause.Result()
				return "done", err
			})},
		{name: "02 WaitForCallback whose submitter completes the callback", want: durabletest.Succeeded,
			handler: func(runner func() *durabletest.LocalRunner[struct{}, string]) durable.Handler[struct{}, string] {
				return func(ctx durable.Context, _ struct{}) (string, error) {
					return durable.WaitForCallback[string](ctx, "approval", func(_ durable.StepContext, id string) error {
						return runner().SendCallbackSuccess(id, "approved")
					})
				}
			}},
		{name: "03 InvokeAsync, then a step, then Result", want: durabletest.Succeeded, setup: registerTargets,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				inv := durable.InvokeAsync[string](ctx, "inv", "target:$LATEST", "x")
				time.Sleep(50 * time.Millisecond) // the invoke START reaches the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return inv.Result()
			})},
		{name: "04 Invoke whose START response already reports it failed", want: durabletest.Failed, setup: registerTargets,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				return durable.Invoke[string](ctx, "inv", "target", "x")
			})},
		{name: "05 Wait in a Parallel branch while another branch runs a step", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				_, err := durable.Parallel(ctx, "p", []durable.Branch[string]{
					{Name: "short", Func: func(c durable.Context) (string, error) {
						return "waited", durable.Wait(c, "pause", time.Second)
					}},
					{Name: "long", Func: func(c durable.Context) (string, error) { return "worked", longStep(c) }},
				})
				return "done", err
			})},
		{name: "06 Wait in a Map item while another item runs a step", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				_, err := durable.Map(ctx, "m", []string{"short", "long"},
					func(c durable.Context, item string, _ int) (string, error) {
						if item == "short" {
							return "waited", durable.Wait(c, "pause", time.Second)
						}
						return "worked", longStep(c)
					}, durable.WithMaxConcurrency(2))
				return "done", err
			})},
		{name: "07 Wait in a Go branch while the handler runs a step", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				bg := durable.Go(ctx, "bg", func(c durable.Context) (string, error) {
					return "waited", durable.Wait(c, "pause", time.Second)
				})
				time.Sleep(50 * time.Millisecond) // the wait START reaches the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return bg.Result()
			})},
		{name: "08 Join over a WaitAsync and a StepAsync", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				pause := durable.WaitAsync(ctx, "pause", time.Second)
				work := durable.StepAsync(ctx, "work", func(durable.StepContext) (string, error) {
					time.Sleep(200 * time.Millisecond)
					return "worked", nil
				})
				if err := durable.Join(ctx, "both", []durable.Awaitable{pause, work}); err != nil {
					return "", err
				}
				return work.Result()
			})},
		{name: "09 StepAsync retry whose next attempt becomes due while another step runs", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				strategy, err := durable.NewRetryStrategy(durable.RetryConfig{MaxAttempts: 2, InitialDelay: time.Second, Jitter: durable.JitterNone})
				if err != nil {
					return "", err
				}
				flaky := durable.StepAsync(ctx, "flaky", func(sc durable.StepContext) (string, error) {
					if sc.Attempt() == 1 {
						return "", errors.New("transient")
					}
					return "recovered", nil
				}, durable.WithRetry(strategy))
				time.Sleep(50 * time.Millisecond) // attempt 1 and its RETRY reach the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return flaky.Result()
			})},
		{name: "10 WaitForCondition whose next check becomes due while a step runs", want: durabletest.Succeeded,
			handler: noRunner(func(ctx durable.Context, _ struct{}) (string, error) {
				poll := durable.Go(ctx, "poll", func(c durable.Context) (int, error) {
					return durable.WaitForCondition(c, "until-ready",
						func(_ durable.StepContext, n int) (int, error) { return n + 1, nil },
						durable.ConditionConfig[int]{InitialState: 0, WaitStrategy: func(n int, _ int) durable.WaitDecision {
							return durable.WaitDecision{Continue: n < 2, Delay: time.Second}
						}})
				})
				time.Sleep(50 * time.Millisecond) // the first check and its RETRY reach the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				n, err := poll.Result()
				if err != nil {
					return "", err
				}
				return strings.Repeat("+", n), nil
			})},
	}
}

func TestAwaitedOperationFinishedDuringLocalInvocation(t *testing.T) {
	for _, tc := range awaitedFinishedCases() {
		t.Run(tc.name, func(t *testing.T) {
			var runner *durabletest.LocalRunner[struct{}, string]
			runner = durabletest.NewLocalRunner(tc.handler(func() *durabletest.LocalRunner[struct{}, string] { return runner }))
			if tc.setup != nil {
				tc.setup(runner)
			}
			result, err := runner.Run(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != tc.want {
				t.Errorf("status = %s (error %+v), want %s", result.Status, result.Error, tc.want)
			}
			if n := len(result.Invocations); n != 1 {
				t.Errorf("invocations = %d, want 1", n)
			}
		})
	}
}

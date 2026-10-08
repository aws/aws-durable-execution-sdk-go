// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func fieldNames(v any) []string {
	t := reflect.TypeOf(v)
	names := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		names = append(names, t.Field(i).Name)
	}
	sort.Strings(names)
	return names
}

func has(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

func detailFieldsHandler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "pause", time.Second); err != nil {
		return "", err
	}
	return durable.Step(ctx, "flaky", func(_ durable.StepContext) (string, error) {
		return "", durable.WithErrorData(errors.New("downstream unavailable"), `{"retryable":false}`)
	}, durable.WithRetry(durable.NoRetry()))
}

// TestResultTypesCarryRecordedFields checks that the result types declare
// the wait, retry, error data, and stack trace fields, and that a run
// populates them.
func TestResultTypesCarryRecordedFields(t *testing.T) {
	runner := durabletest.NewLocalRunner(detailFieldsHandler)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}

	want := map[string][]string{
		"TestWaitDetails":     {"WaitSeconds", "ScheduledEndTimestamp"},
		"TestStepDetails":     {"NextAttemptTimestamp", "ErrorData", "StackTrace"},
		"TestCallbackDetails": {"ErrorData", "StackTrace"},
		"TestInvokeDetails":   {"ErrorData", "StackTrace"},
		"TestContextDetails":  {"ErrorData", "StackTrace"},
		"TestError":           {"ErrorData", "StackTrace"},
	}
	types := map[string]any{
		"TestWaitDetails":     durabletest.TestWaitDetails{},
		"TestStepDetails":     durabletest.TestStepDetails{},
		"TestCallbackDetails": durabletest.TestCallbackDetails{},
		"TestInvokeDetails":   durabletest.TestInvokeDetails{},
		"TestContextDetails":  durabletest.TestContextDetails{},
		"TestError":           durabletest.TestError{},
	}
	for name, fields := range want {
		got := fieldNames(types[name])
		for _, f := range fields {
			if !has(got, f) {
				t.Errorf("%s fields %v lack %s", name, got, f)
			}
		}
	}

	wait := result.Operation("pause")
	if wait == nil || wait.WaitDetails == nil {
		t.Fatalf("pause op or its WaitDetails missing: %+v", wait)
	}
	if wait.WaitDetails.WaitSeconds != 1 {
		t.Errorf("pause WaitSeconds = %d, want 1", wait.WaitDetails.WaitSeconds)
	}
	if wait.WaitDetails.ScheduledEndTimestamp.IsZero() {
		t.Error("pause ScheduledEndTimestamp is zero")
	}

	step := result.Operation("flaky")
	if step == nil || step.StepDetails == nil {
		t.Fatalf("flaky op or its StepDetails missing: %+v", step)
	}
	if step.StepDetails.ErrorData != `{"retryable":false}` {
		t.Errorf("flaky ErrorData = %q, want the attached data", step.StepDetails.ErrorData)
	}
	if len(step.StepDetails.StackTrace) == 0 {
		t.Error("flaky StackTrace is empty")
	}

	if result.Error == nil {
		t.Fatal("result.Error = nil")
	}
	if result.Error.ErrorData != `{"retryable":false}` {
		t.Errorf("result.Error.ErrorData = %q, want the attached data", result.Error.ErrorData)
	}
	if len(result.Error.StackTrace) == 0 {
		t.Error("result.Error.StackTrace is empty")
	}
}

func TestWaitDetailsLocal(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
	result, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	wait := result.Operation("pause")
	if wait == nil || wait.WaitDetails == nil {
		t.Fatalf("pause = %+v, want wait details", wait)
	}
	if wait.WaitDetails.WaitSeconds != 5 {
		t.Errorf("WaitSeconds = %d, want 5", wait.WaitDetails.WaitSeconds)
	}
	if want := wait.StartTime.Add(5 * time.Second); !wait.WaitDetails.ScheduledEndTimestamp.Equal(want) {
		t.Errorf("ScheduledEndTimestamp = %v, want StartTime + 5s = %v", wait.WaitDetails.ScheduledEndTimestamp, want)
	}
}

func TestStepNextAttemptTimestampLocal(t *testing.T) {
	strategy, err := durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Hour,
		BackoffRate:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := func(ctx durable.Context, _ any) (string, error) {
		return durable.Step(ctx, "flaky", func(durable.StepContext) (string, error) {
			return "", errors.New("transient")
		}, durable.WithRetry(strategy))
	}
	result, err := durabletest.NewLocalRunner(handler).Run(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	step := result.Operation("flaky")
	if step == nil || step.StepDetails == nil {
		t.Fatalf("flaky = %+v, want step details", step)
	}
	if step.StepDetails.NextAttemptTimestamp.IsZero() {
		t.Errorf("NextAttemptTimestamp is zero, want the scheduled retry: %+v", *step.StepDetails)
	}
}

func TestInvokeErrorDetailsLocal(t *testing.T) {
	target := func(ctx durable.Context, _ string) (string, error) {
		return "", durable.WithErrorData(errors.New("rejected"), `{"reason":"limit"}`)
	}
	handler := func(ctx durable.Context, _ any) (string, error) {
		return durable.Invoke[string](ctx, "call", "target-function:$LATEST", "x")
	}
	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("target-function:$LATEST", durabletest.DurableFunction(target))
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	inv := result.Operation("call")
	if inv == nil || inv.InvokeDetails == nil {
		t.Fatalf("call = %+v, want invoke details", inv)
	}
	if inv.InvokeDetails.ErrorData != `{"reason":"limit"}` {
		t.Errorf("ErrorData = %q, want the target's data", inv.InvokeDetails.ErrorData)
	}
	if len(inv.InvokeDetails.StackTrace) == 0 {
		t.Error("StackTrace is empty, want the target's trace")
	}
}

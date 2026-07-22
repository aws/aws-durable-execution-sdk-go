// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func goldenPath(name string) string {
	return filepath.Join("testdata", name+".golden")
}

func TestSignatureStepSuccess(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Step[string](ctx, "process", func(_ durable.StepContext) (string, error) {
			return "done-" + event, nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "input")

	durabletest.AssertGoldenSignature(t, result, goldenPath("step_success"))
}

func TestSignatureStepRetry(t *testing.T) {
	var attempts int
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Step[string](ctx, "flaky", func(_ durable.StepContext) (string, error) {
			attempts++
			if attempts < 3 {
				return "", &retryableErr{msg: "transient"}
			}
			return "recovered", nil
		}, durable.WithRetry(durable.ExponentialBackoff()))
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "input")

	durabletest.AssertGoldenSignature(t, result, goldenPath("step_retry"))
}

type retryableErr struct {
	msg string
}

func (e *retryableErr) Error() string { return e.msg }

func TestSignatureWait(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		result, err := durable.Step[string](ctx, "after-wait", func(_ durable.StepContext) (string, error) {
			return "completed", nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "input")

	durabletest.AssertGoldenSignature(t, result, goldenPath("wait"))
}

func TestSignatureCallback(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
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
	result := runner.RunUntilComplete(t, "input")

	// Callback leaves execution PENDING.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Resolve callback.
	cbs := runner.OpenCallbacks()
	if len(cbs) == 0 {
		t.Fatal("expected open callbacks")
	}
	if err := runner.SendCallbackSuccess(cbs[0].CallbackID, "approved"); err != nil {
		t.Fatalf("send callback: %v", err)
	}

	result = runner.RunUntilComplete(t, "input")
	durabletest.AssertGoldenSignature(t, result, goldenPath("callback"))
}

func TestSignatureChainedInvoke(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		result, err := durable.Invoke[string, string](ctx, "call-service", "target-fn", event)
		if err != nil {
			return "", err
		}
		return "got: " + result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "hello")

	// Chained invoke leaves execution PENDING.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected PENDING, got %s", result.Status)
	}

	// Complete the chained invoke.
	if err := runner.CompleteChainedInvoke("call-service", "world"); err != nil {
		t.Fatalf("complete invoke: %v", err)
	}

	result = runner.RunUntilComplete(t, "hello")
	durabletest.AssertGoldenSignature(t, result, goldenPath("chained_invoke"))
}

func TestSignatureParallel(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		branches := []durable.Branch[string]{
			{Name: "branch-a", Func: func(ctx durable.Context) (string, error) {
				return durable.Step[string](ctx, "step-a", func(_ durable.StepContext) (string, error) {
					return "a-done", nil
				})
			}},
			{Name: "branch-b", Func: func(ctx durable.Context) (string, error) {
				return durable.Step[string](ctx, "step-b", func(_ durable.StepContext) (string, error) {
					return "b-done", nil
				})
			}},
		}
		br, err := durable.Parallel(ctx, "parallel-work", branches, durable.WithMaxConcurrency(1))
		if err != nil {
			return "", err
		}
		return br.Items[0].Result + "+" + br.Items[1].Result, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "input")

	durabletest.AssertGoldenSignature(t, result, goldenPath("parallel"))
}

func TestSignatureMap(t *testing.T) {
	handler := func(ctx durable.Context, event []int) (string, error) {
		br, err := durable.Map(ctx, "process-items", event, func(ctx durable.Context, item int, _ int) (int, error) {
			return durable.Step[int](ctx, "", func(_ durable.StepContext) (int, error) {
				return item * 2, nil
			})
		}, durable.WithMaxConcurrency(1))
		if err != nil {
			return "", err
		}
		_ = br
		return "mapped", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, []int{1, 2, 3})

	durabletest.AssertGoldenSignature(t, result, goldenPath("map"))
}

func TestEventSignatureUnit(t *testing.T) {
	// Unit test EventSignature directly, without golden files.
	result := &durabletest.TestResult{
		Status: durabletest.Succeeded,
		Operations: []durabletest.TestOperation{
			{Type: "STEP", SubType: "Step", Name: "first", Status: "SUCCEEDED"},
			{Type: "WAIT", Name: "delay", Status: "SUCCEEDED"},
			{Type: "CALLBACK", SubType: "CreateCallback", Name: "cb", Status: "SUCCEEDED"},
		},
	}

	sigs := durabletest.EventSignature(result)
	if len(sigs) != 3 {
		t.Fatalf("expected 3 signatures, got %d", len(sigs))
	}

	expected := []durabletest.OperationSignature{
		{Type: "STEP", SubType: "Step", Name: "first", Status: "SUCCEEDED"},
		{Type: "WAIT", Name: "delay", Status: "SUCCEEDED"},
		{Type: "CALLBACK", SubType: "CreateCallback", Name: "cb", Status: "SUCCEEDED"},
	}
	for i, sig := range sigs {
		if sig != expected[i] {
			t.Errorf("signature[%d] = %+v, want %+v", i, sig, expected[i])
		}
	}
}

func TestEventSignatureNil(t *testing.T) {
	sigs := durabletest.EventSignature(nil)
	if sigs != nil {
		t.Errorf("expected nil for nil result, got %v", sigs)
	}
}

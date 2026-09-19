// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// A wrap hook that panics after calling fn must not cause the SDK to run the
// wrapped body a second time. These tests run a handler through the local
// runner with a panicking hook on each wrap point and count how often the
// wrapped body executes.

// TestWrapPanicAfterCallDoubleExecutesStepBody is the regression test for
// a WrapOperationAttemptFn hook that panics after calling fn.
func TestWrapPanicAfterCallDoubleExecutesStepBody(t *testing.T) {
	var stepRuns int32
	p := durable.Plugin{
		WrapOperationAttemptFn: func(_ context.Context, _ durable.AttemptHookInfo, fn func() (any, error)) (any, error) {
			_, _ = fn()
			panic("plugin panics after calling fn")
		},
	}
	h := func(ctx durable.Context, _ any) (int32, error) {
		if _, err := durable.Step(ctx, "s", func(durable.StepContext) (int, error) {
			return int(atomic.AddInt32(&stepRuns, 1)), nil
		}); err != nil {
			return 0, err
		}
		return atomic.LoadInt32(&stepRuns), nil
	}
	res := durabletest.NewLocalRunner(h, durable.WithPlugins(p)).RunUntilComplete(t, nil)
	t.Logf("status=%v result=%s stepRuns=%d", res.Status, res.RawResult, stepRuns)
	if stepRuns != 1 {
		t.Errorf("step body executed %d times in one attempt; want 1", stepRuns)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
}

// TestWrapPanicBeforeCallRunsStepBodyOnce covers a WrapOperationAttemptFn
// hook that panics before calling fn: the body runs exactly once and its
// result is used.
func TestWrapPanicBeforeCallRunsStepBodyOnce(t *testing.T) {
	var stepRuns int32
	p := durable.Plugin{
		WrapOperationAttemptFn: func(_ context.Context, _ durable.AttemptHookInfo, _ func() (any, error)) (any, error) {
			panic("plugin panics before calling fn")
		},
	}
	h := func(ctx durable.Context, _ any) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			atomic.AddInt32(&stepRuns, 1)
			return "step-result", nil
		})
	}
	res := durabletest.NewLocalRunner(h, durable.WithPlugins(p)).RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	if stepRuns != 1 {
		t.Errorf("step body executed %d times; want 1", stepRuns)
	}
	out, err := durabletest.ResultAs[string](res)
	if err != nil {
		t.Fatal(err)
	}
	if out != "step-result" {
		t.Errorf("result = %q, want %q", out, "step-result")
	}
}

// TestWrapChildContextPanicAfterCallRunsBodyOnce covers WrapChildContextFn.
func TestWrapChildContextPanicAfterCallRunsBodyOnce(t *testing.T) {
	var childRuns int32
	p := durable.Plugin{
		WrapChildContextFn: func(_ context.Context, _ durable.OperationHookInfo, fn func() (any, error)) (any, error) {
			_, _ = fn()
			panic("plugin panics after calling fn")
		},
	}
	h := func(ctx durable.Context, _ any) (string, error) {
		return durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) {
			atomic.AddInt32(&childRuns, 1)
			return "child-result", nil
		})
	}
	res := durabletest.NewLocalRunner(h, durable.WithPlugins(p)).RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	if childRuns != 1 {
		t.Errorf("child body executed %d times; want 1", childRuns)
	}
	out, err := durabletest.ResultAs[string](res)
	if err != nil {
		t.Fatal(err)
	}
	if out != "child-result" {
		t.Errorf("result = %q, want %q", out, "child-result")
	}
}

// TestWrapInvocationPanicAfterCallRunsHandlerOnce covers WrapInvocation.
func TestWrapInvocationPanicAfterCallRunsHandlerOnce(t *testing.T) {
	var handlerRuns int32
	p := durable.Plugin{
		WrapInvocation: func(_ context.Context, _ durable.InvocationHookInfo, fn func() (any, error)) (any, error) {
			_, _ = fn()
			panic("plugin panics after calling fn")
		},
	}
	h := func(ctx durable.Context, _ any) (string, error) {
		atomic.AddInt32(&handlerRuns, 1)
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "handler-result", nil
		})
	}
	res := durabletest.NewLocalRunner(h, durable.WithPlugins(p)).RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	if handlerRuns != 1 {
		t.Errorf("handler executed %d times in one invocation; want 1", handlerRuns)
	}
	out, err := durabletest.ResultAs[string](res)
	if err != nil {
		t.Fatal(err)
	}
	if out != "handler-result" {
		t.Errorf("result = %q, want %q", out, "handler-result")
	}
}

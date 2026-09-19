// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// wrapInvocationChain runs fn through wrapChain with the given plugins'
// WrapInvocation hooks.
func wrapInvocationChain(plugins []Plugin, fn func() (any, error)) (any, error) {
	pd := newPluginDispatcher(plugins)
	return wrapChain(pd, context.Background(),
		func(p *Plugin) wrapHook {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(ctx context.Context, inner wrapBody) (any, error) {
				return p.WrapInvocation(ctx, InvocationHookInfo{}, inner)
			}
		},
		func(context.Context) (any, error) { return fn() },
	)
}

// TestWrapPanicAfterCallReturnsRecordedResult verifies that a hook that
// panics after calling fn does not run fn again and the chain returns the
// result fn produced.
func TestWrapPanicAfterCallReturnsRecordedResult(t *testing.T) {
	var runs int32
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			_, _ = fn(ctx)
			panic("after fn")
		},
	}
	result, err := wrapInvocationChain([]Plugin{hook}, func() (any, error) {
		atomic.AddInt32(&runs, 1)
		return "body-result", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "body-result" {
		t.Fatalf("result = %v, want body-result", result)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapPanicAfterCallPreservesBodyError verifies that the body's error
// is returned when the hook panics after calling fn.
func TestWrapPanicAfterCallPreservesBodyError(t *testing.T) {
	bodyErr := errors.New("body error")
	var runs int32
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			_, _ = fn(ctx)
			panic("after fn")
		},
	}
	_, err := wrapInvocationChain([]Plugin{hook}, func() (any, error) {
		atomic.AddInt32(&runs, 1)
		return nil, bodyErr
	})
	if !errors.Is(err, bodyErr) {
		t.Fatalf("err = %v, want %v", err, bodyErr)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapPanicBeforeCallRunsBodyOnce verifies that a hook that panics
// before calling fn causes fn to run exactly once.
func TestWrapPanicBeforeCallRunsBodyOnce(t *testing.T) {
	var runs int32
	hook := Plugin{
		WrapInvocation: func(_ context.Context, _ InvocationHookInfo, _ func(context.Context) (any, error)) (any, error) {
			panic("before fn")
		},
	}
	result, err := wrapInvocationChain([]Plugin{hook}, func() (any, error) {
		atomic.AddInt32(&runs, 1)
		return "body-result", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "body-result" {
		t.Fatalf("result = %v, want body-result", result)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapHookCallsFnTwiceRunsBodyOnce verifies that a hook that calls fn
// twice runs the body once and receives the recorded result on the second
// call.
func TestWrapHookCallsFnTwiceRunsBodyOnce(t *testing.T) {
	var runs int32
	var second any
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			_, _ = fn(ctx)
			r, e := fn(ctx)
			second = r
			return r, e
		},
	}
	result, err := wrapInvocationChain([]Plugin{hook}, func() (any, error) {
		atomic.AddInt32(&runs, 1)
		return "body-result", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "body-result" || second != "body-result" {
		t.Fatalf("result = %v, second call = %v, want body-result for both", result, second)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapBodyPanicPropagatesWithoutRerun verifies that a panic raised by
// the body itself reaches the caller of wrapChain with its original value
// and the body is not run again, even though a wrap hook is in place.
func TestWrapBodyPanicPropagatesWithoutRerun(t *testing.T) {
	var runs int32
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(ctx)
		},
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = wrapInvocationChain([]Plugin{hook}, func() (any, error) {
			atomic.AddInt32(&runs, 1)
			panic("body panic")
		})
	}()
	if recovered != "body panic" {
		t.Fatalf("recovered = %v, want body panic", recovered)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapNestedPanicsRunBodyOnce verifies that with two hooks that both
// panic after calling fn, the body runs once and its result is returned.
func TestWrapNestedPanicsRunBodyOnce(t *testing.T) {
	var runs int32
	panicAfter := func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
		_, _ = fn(ctx)
		panic("after fn")
	}
	plugins := []Plugin{{WrapInvocation: panicAfter}, {WrapInvocation: panicAfter}}
	result, err := wrapInvocationChain(plugins, func() (any, error) {
		atomic.AddInt32(&runs, 1)
		return "body-result", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "body-result" {
		t.Fatalf("result = %v, want body-result", result)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapHookRecoversBodyPanicAndReturnsNormally verifies that a hook
// which recovers the body's panic and returns a normal result cannot turn
// the panic into success: the body's panic still reaches the caller and
// the body runs once.
func TestWrapHookRecoversBodyPanicAndReturnsNormally(t *testing.T) {
	var runs int32
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (result any, err error) {
			defer func() {
				if r := recover(); r != nil {
					result, err = "hook-result", nil
				}
			}()
			return fn(ctx)
		},
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = wrapInvocationChain([]Plugin{hook}, func() (any, error) {
			atomic.AddInt32(&runs, 1)
			panic("body panic")
		})
	}()
	if recovered != "body panic" {
		t.Fatalf("recovered = %v, want body panic", recovered)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapHookRecoversBodyPanicAndCallsFnAgain verifies that a hook which
// recovers the body's panic and calls fn a second time sees the same panic
// again, does not run the body again, and cannot report success.
func TestWrapHookRecoversBodyPanicAndCallsFnAgain(t *testing.T) {
	var runs int32
	var secondCallPanic any
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			func() {
				defer func() { _ = recover() }()
				_, _ = fn(ctx)
			}()
			func() {
				defer func() { secondCallPanic = recover() }()
				_, _ = fn(ctx)
			}()
			return "hook-result", nil
		},
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = wrapInvocationChain([]Plugin{hook}, func() (any, error) {
			atomic.AddInt32(&runs, 1)
			panic("body panic")
		})
	}()
	if secondCallPanic != "body panic" {
		t.Fatalf("second fn call recovered = %v, want body panic", secondCallPanic)
	}
	if recovered != "body panic" {
		t.Fatalf("recovered = %v, want body panic", recovered)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

// TestWrapHookRecoversBodyPanicThenPanics verifies that when the body
// panics and the hook recovers it and then panics itself, the body's panic
// reaches the caller instead of the hook's, and the body runs once.
func TestWrapHookRecoversBodyPanicThenPanics(t *testing.T) {
	var runs int32
	hook := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			func() {
				defer func() { _ = recover() }()
				_, _ = fn(ctx)
			}()
			panic("hook panic")
		},
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = wrapInvocationChain([]Plugin{hook}, func() (any, error) {
			atomic.AddInt32(&runs, 1)
			panic("body panic")
		})
	}()
	if recovered != "body panic" {
		t.Fatalf("recovered = %v, want body panic", recovered)
	}
	if runs != 1 {
		t.Fatalf("body ran %d times, want 1", runs)
	}
}

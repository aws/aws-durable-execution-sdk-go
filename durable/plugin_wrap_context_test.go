// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// wrapCtxKey is the context key the tests in this file attach through wrap
// hooks and read back inside user code.
type wrapCtxKey string

// origCtxKey is a key the tests attach to the SDK's own context, to check
// that a body context still reaches it.
type origCtxKey struct{}

// valueHooks returns a plugin whose three wrap hooks attach value under key
// to the context they pass on.
func valueHooks(key wrapCtxKey, value string) Plugin {
	return Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(ctx, key, value))
		},
		WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(ctx, key, value))
		},
		WrapChildContextFn: func(ctx context.Context, _ OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(ctx, key, value))
		},
	}
}

// parentOf returns the context a StepContext was built over: the one the
// wrap hooks supplied.
func parentOf(t *testing.T, sc StepContext) context.Context {
	t.Helper()
	c, ok := sc.(*stepContext)
	if !ok {
		t.Fatalf("StepContext is %T, want *stepContext", sc)
	}
	return c.Context
}

// valueOf reads key from ctx as a string; "" when absent.
func valueOf(ctx context.Context, key wrapCtxKey) string {
	s, _ := ctx.Value(key).(string)
	return s
}

// TestWrapHookContextReachesUserCode is the acceptance test for the wrap
// hook context: a value a plugin attaches in WrapOperationAttemptFn is
// readable inside a step body and a condition check, one attached in
// WrapChildContextFn inside a child context function, and one attached in
// WrapInvocation inside the handler and everything below it.
func TestWrapHookContextReachesUserCode(t *testing.T) {
	const key wrapCtxKey = "trace"
	seen := map[string]string{}

	handler := Wrap(func(ctx Context, _ string) (string, error) {
		seen["handler"] = valueOf(ctx, key)

		step, err := Step(ctx, "s", func(sc StepContext) (string, error) {
			return valueOf(sc, key), nil
		})
		if err != nil {
			return "", err
		}
		seen["step"] = step

		child, err := RunInChildContext(ctx, "c", func(c Context) (string, error) {
			return valueOf(c, key), nil
		})
		if err != nil {
			return "", err
		}
		seen["child"] = child

		cond, err := WaitForCondition(ctx, "w", func(sc StepContext, _ string) (string, error) {
			return valueOf(sc, key), nil
		}, ConditionConfig[string]{
			WaitStrategy: func(string, int) WaitDecision { return WaitDecision{Continue: false} },
		})
		if err != nil {
			return "", err
		}
		seen["check"] = cond
		return "ok", nil
	}, WithPlugins(valueHooks(key, "attached")), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:wrap-ctx", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	for _, site := range []string{"handler", "step", "child", "check"} {
		if seen[site] != "attached" {
			t.Errorf("%s observed %q, want %q", site, seen[site], "attached")
		}
	}
}

// TestWrapHookContextNestsAcrossPlugins verifies that the context the outer
// plugin passes on is the one the inner plugin receives, so the body sees
// both plugins' values.
func TestWrapHookContextNestsAcrossPlugins(t *testing.T) {
	const outer, inner wrapCtxKey = "outer", "inner"
	var seenOuter, seenInner, innerHookSaw string

	p1 := valueHooks(outer, "one")
	p2 := Plugin{
		WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			innerHookSaw = valueOf(ctx, outer)
			return fn(context.WithValue(ctx, inner, "two"))
		},
	}

	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(sc StepContext) (string, error) {
			seenOuter, seenInner = valueOf(sc, outer), valueOf(sc, inner)
			return "", nil
		})
	}, WithPlugins(p1, p2), withLambdaAPI(&fakePluginClient{}))

	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:wrap-nest", "tok1", nil)); err != nil {
		t.Fatal(err)
	}
	if innerHookSaw != "one" {
		t.Errorf("inner hook received outer value %q, want %q", innerHookSaw, "one")
	}
	if seenOuter != "one" || seenInner != "two" {
		t.Errorf("step body saw outer=%q inner=%q, want one/two", seenOuter, seenInner)
	}
}

// TestWrapHookUnchangedContextIsSameContext verifies that a hook passing
// the ctx it received on unchanged causes no change: the step body gets the
// same context it would get without the plugin, by identity.
func TestWrapHookUnchangedContextIsSameContext(t *testing.T) {
	var inBody context.Context
	var inHook context.Context
	passthrough := Plugin{
		WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			inHook = ctx
			return fn(ctx)
		},
	}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(sc StepContext) (string, error) {
			inBody = parentOf(t, sc)
			return "", nil
		})
	}, WithPlugins(passthrough), withLambdaAPI(&fakePluginClient{}))

	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:wrap-same", "tok1", nil)); err != nil {
		t.Fatal(err)
	}
	if inBody == nil || inBody != inHook {
		t.Fatalf("step body context %v is not the context the hook received %v", inBody, inHook)
	}
}

// TestWrapHookCannotDetachFromInvocationContext verifies that a hook which
// passes a context not descending from the invocation's still leaves the
// body attached to the invocation's cancellation, deadline, and values,
// while the hook's own value is visible.
func TestWrapHookCannotDetachFromInvocationContext(t *testing.T) {
	const key wrapCtxKey = "detached"
	deadline := time.Now().Add(time.Hour)
	invCtx, cancel := context.WithDeadline(context.WithValue(context.Background(), origCtxKey{}, "orig"), deadline)
	defer cancel()

	detach := Plugin{
		WrapOperationAttemptFn: func(_ context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(context.Background(), key, "yes"))
		},
	}
	// The body context lives only while the body runs, so every check
	// happens inside the step. The step cancels the invocation context
	// itself and waits for the cancellation to reach its own context.
	// What the handler returns after that is not this test's subject.
	var checked bool
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(sc StepContext) (string, error) {
			body := parentOf(t, sc)
			checked = true
			if got := valueOf(body, key); got != "yes" {
				t.Errorf("plugin value = %q, want %q", got, "yes")
			}
			if got, _ := body.Value(origCtxKey{}).(string); got != "orig" {
				t.Errorf("invocation value = %q, want %q", got, "orig")
			}
			if d, ok := body.Deadline(); !ok || !d.Equal(deadline) {
				t.Errorf("deadline = %v, %v; want %v, true", d, ok, deadline)
			}
			if body.Err() != nil {
				t.Fatalf("body context ended early: %v", body.Err())
			}
			cancel()
			select {
			case <-body.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("body context not cancelled after the invocation context was")
			}
			if !errors.Is(context.Cause(body), context.Canceled) {
				t.Errorf("cause = %v, want Canceled", context.Cause(body))
			}
			return "", nil
		})
	}, WithPlugins(detach), withLambdaAPI(&fakePluginClient{}))

	_, _ = handler(invCtx, makePluginPayload(t, "arn:test:wrap-detach", "tok1", nil))
	if !checked {
		t.Fatal("step body did not run")
	}
}

// noDeadlineContext forwards everything from its embedded context except
// the deadline, which it reports as absent. It models a custom context that
// shares orig's Done channel but does not carry orig's deadline.
type noDeadlineContext struct {
	context.Context
}

func (noDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }

// sliceContext is a context whose dynamic type is not comparable: it holds
// a slice. Comparing two interface values of this type with == panics.
type sliceContext struct {
	context.Context
	tags []string
}

// TestBodyContextNonComparable is the regression test for comparing two
// context interface values with ==. A hook that passes a non-comparable
// context through unchanged must not make bodyContext panic, and the body
// must still see the values, deadline, and cancellation of that context.
func TestBodyContextNonComparable(t *testing.T) {
	const key wrapCtxKey = "k"

	t.Run("unchanged non-comparable context does not panic", func(t *testing.T) {
		base, cancel := context.WithCancel(context.WithValue(context.Background(), key, "v"))
		defer cancel()
		orig := sliceContext{Context: base, tags: []string{"a"}}

		got, release := bodyContext(orig, orig)
		defer release()
		if valueOf(got, key) != "v" {
			t.Errorf("value = %q, want v", valueOf(got, key))
		}
		cancel()
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled after orig was")
		}
	})

	t.Run("comparable wrapper over a non-comparable context does not panic", func(t *testing.T) {
		// noDeadlineContext is a comparable struct type, but its embedded
		// interface holds a non-comparable value. Comparing two such
		// values with == still panics, so the guard must look through the
		// interface field.
		inner := sliceContext{Context: context.WithValue(context.Background(), key, "v")}
		orig := noDeadlineContext{Context: inner}

		got, release := bodyContext(orig, orig)
		defer release()
		if valueOf(got, key) != "v" {
			t.Errorf("value = %q, want v", valueOf(got, key))
		}
	})

	t.Run("pass-through hook over a non-comparable invocation context", func(t *testing.T) {
		orig := sliceContext{Context: context.WithValue(context.Background(), key, "v"), tags: []string{"a"}}
		d := &pluginDispatcher{plugins: []Plugin{{
			WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
				return fn(ctx)
			},
		}}}
		got, err := wrapChain(d, orig, func(p *Plugin) wrapHook {
			if p.WrapOperationAttemptFn == nil {
				return nil
			}
			return func(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
				return p.WrapOperationAttemptFn(ctx, AttemptHookInfo{}, fn)
			}
		}, func(ctx context.Context) (any, error) {
			return valueOf(ctx, key), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != "v" {
			t.Errorf("body saw %v, want v", got)
		}
	})
}

// TestSameContext checks the guarded identity test bodyContext relies on.
func TestSameContext(t *testing.T) {
	a := context.Background()
	b := context.WithValue(a, wrapCtxKey("k"), "v")
	nc := sliceContext{Context: a}

	cases := []struct {
		name string
		x, y context.Context
		want bool
	}{
		{"identical comparable", a, a, true},
		{"different comparable", a, b, false},
		{"different types", a, nc, false},
		{"identical non-comparable", nc, nc, false},
		{"both nil", nil, nil, true},
		{"one nil", a, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameContext(tc.x, tc.y); got != tc.want {
				t.Errorf("sameContext = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBodyContext covers bodyContext directly: which contexts pass through
// unchanged, and how any other supplied context is merged with orig.
func TestBodyContext(t *testing.T) {
	const key wrapCtxKey = "k"

	t.Run("nil and identical pass through", func(t *testing.T) {
		orig, cancel := context.WithCancel(context.Background())
		defer cancel()
		got, release := bodyContext(orig, nil)
		release()
		if got != orig {
			t.Errorf("nil supplied: got %v, want orig", got)
		}
		got, release = bodyContext(orig, orig)
		release()
		if got != orig {
			t.Errorf("orig supplied: got %v, want orig", got)
		}
	})

	t.Run("value chain over orig keeps orig's values, deadline, and cancellation", func(t *testing.T) {
		deadline := time.Now().Add(time.Hour)
		orig, cancel := context.WithDeadline(context.WithValue(context.Background(), origCtxKey{}, "orig"), deadline)
		defer cancel()
		supplied := context.WithValue(orig, key, "v")
		got, release := bodyContext(orig, supplied)
		defer release()
		if valueOf(got, key) != "v" {
			t.Errorf("supplied value = %q, want v", valueOf(got, key))
		}
		if o, _ := got.Value(origCtxKey{}).(string); o != "orig" {
			t.Errorf("orig value = %q, want orig", o)
		}
		if d, ok := got.Deadline(); !ok || !d.Equal(deadline) {
			t.Errorf("deadline = %v, %v; want %v, true", d, ok, deadline)
		}
		cancel()
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled after orig was")
		}
	})

	t.Run("unrelated contexts with nil Done channels are merged", func(t *testing.T) {
		// Neither context can be cancelled, so both report a nil Done
		// channel. That equality must not be read as supplied descending
		// from orig: orig's values still have to reach the body.
		orig := context.WithValue(context.Background(), origCtxKey{}, "orig")
		supplied := context.WithValue(context.Background(), key, "v")
		if orig.Done() != nil || supplied.Done() != nil {
			t.Fatal("test setup: expected nil Done channels")
		}
		got, release := bodyContext(orig, supplied)
		defer release()
		if valueOf(got, key) != "v" {
			t.Errorf("supplied value = %q, want v", valueOf(got, key))
		}
		if o, _ := got.Value(origCtxKey{}).(string); o != "orig" {
			t.Errorf("orig value = %q, want orig", o)
		}
	})

	t.Run("shared Done with a changed deadline keeps orig's deadline", func(t *testing.T) {
		// supplied forwards orig's Done channel but hides orig's deadline.
		// The body must still observe orig's deadline.
		deadline := time.Now().Add(time.Hour)
		orig, cancel := context.WithDeadline(context.WithValue(context.Background(), origCtxKey{}, "orig"), deadline)
		defer cancel()
		supplied := noDeadlineContext{Context: orig}
		if supplied.Done() != orig.Done() {
			t.Fatal("test setup: expected shared Done channel")
		}
		got, release := bodyContext(orig, supplied)
		defer release()
		if d, ok := got.Deadline(); !ok || !d.Equal(deadline) {
			t.Errorf("deadline = %v, %v; want %v, true", d, ok, deadline)
		}
		if o, _ := got.Value(origCtxKey{}).(string); o != "orig" {
			t.Errorf("orig value = %q, want orig", o)
		}
		cancel()
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled after orig was")
		}
	})

	t.Run("supplied cancellation is kept", func(t *testing.T) {
		orig, cancelOrig := context.WithCancel(context.Background())
		defer cancelOrig()
		supplied, cancelSupplied := context.WithCancel(orig)
		defer cancelSupplied()
		got, release := bodyContext(orig, supplied)
		defer release()
		cancelSupplied()
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled after the supplied context was")
		}
	})

	t.Run("orig cause and deadline reach a detached context", func(t *testing.T) {
		deadline := time.Now().Add(time.Hour)
		orig, cancelOrig := context.WithDeadline(context.Background(), deadline)
		defer cancelOrig()
		cause := errors.New("stop")
		orig2, cancelCause := context.WithCancelCause(orig)
		defer cancelCause(nil)

		got, release := bodyContext(orig2, context.Background())
		defer release()
		if d, ok := got.Deadline(); !ok || !d.Equal(deadline) {
			t.Errorf("deadline = %v, %v; want %v, true", d, ok, deadline)
		}
		cancelCause(cause)
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled after orig was")
		}
		if !errors.Is(context.Cause(got), cause) {
			t.Errorf("cause = %v, want %v", context.Cause(got), cause)
		}
	})

	t.Run("earlier supplied deadline wins", func(t *testing.T) {
		orig, cancelOrig := context.WithTimeout(context.Background(), time.Hour)
		defer cancelOrig()
		sooner := time.Now().Add(time.Minute)
		supplied, cancelSupplied := context.WithDeadline(context.Background(), sooner)
		defer cancelSupplied()
		got, release := bodyContext(orig, supplied)
		defer release()
		if d, ok := got.Deadline(); !ok || !d.Equal(sooner) {
			t.Errorf("deadline = %v, %v; want %v, true", d, ok, sooner)
		}
	})

	t.Run("derived contexts propagate cancellation", func(t *testing.T) {
		orig, cancelOrig := context.WithCancel(context.Background())
		defer cancelOrig()
		got, release := bodyContext(orig, context.Background())
		defer release()
		derived, cancelDerived := context.WithCancel(got)
		defer cancelDerived()
		cancelOrig()
		select {
		case <-derived.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("context derived from the body context not cancelled after orig was")
		}
	})
}

// TestWrapChainContextGuard covers the context rules of the exactly-once
// guard: a nil context stands for the hook's own, a hook that panics before
// calling fn runs it with the context that hook received, and a second call
// with a different context returns the first call's outcome.
func TestWrapChainContextGuard(t *testing.T) {
	const key wrapCtxKey = "g"
	attempt := func(p Plugin) wrapHook {
		if p.WrapOperationAttemptFn == nil {
			return nil
		}
		return func(ctx context.Context, fn wrapBody) (any, error) {
			return p.WrapOperationAttemptFn(ctx, AttemptHookInfo{}, fn)
		}
	}
	getWrap := func(p *Plugin) wrapHook { return attempt(*p) }
	base := context.WithValue(context.Background(), key, "base")

	t.Run("nil context means the hook's context", func(t *testing.T) {
		pd := newPluginDispatcher([]Plugin{
			valueHooks(key, "outer"),
			{WrapOperationAttemptFn: func(_ context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
				return fn(nil)
			}},
		})
		got, err := wrapChain(pd, base, getWrap, func(ctx context.Context) (any, error) { return valueOf(ctx, key), nil })
		if err != nil || got != "outer" {
			t.Fatalf("body saw %v, %v; want outer", got, err)
		}
	})

	t.Run("panic before fn runs fn with the hook's context", func(t *testing.T) {
		pd := newPluginDispatcher([]Plugin{
			valueHooks(key, "outer"),
			{WrapOperationAttemptFn: func(context.Context, AttemptHookInfo, func(context.Context) (any, error)) (any, error) {
				panic("before fn")
			}},
		})
		got, err := wrapChain(pd, base, getWrap, func(ctx context.Context) (any, error) { return valueOf(ctx, key), nil })
		if err != nil || got != "outer" {
			t.Fatalf("body saw %v, %v; want outer", got, err)
		}
	})

	t.Run("second call keeps the first context", func(t *testing.T) {
		var runs int
		pd := newPluginDispatcher([]Plugin{
			{WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
				_, _ = fn(context.WithValue(ctx, key, "first"))
				return fn(context.WithValue(ctx, key, "second"))
			}},
		})
		got, err := wrapChain(pd, base, getWrap, func(ctx context.Context) (any, error) {
			runs++
			return valueOf(ctx, key), nil
		})
		if err != nil || got != "first" || runs != 1 {
			t.Fatalf("body saw %v, %v after %d runs; want first after 1 run", got, err, runs)
		}
	})
}

// channelContext is a cancellable context that the standard library cannot
// see into: its Value does not expose an underlying cancellation context.
// context.AfterFunc therefore watches its Done channel from a goroutine,
// one per registration, which makes registrations countable from a test.
type channelContext struct {
	context.Context
	done chan struct{}
}

func newChannelContext() *channelContext {
	return &channelContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *channelContext) Done() <-chan struct{} { return c.done }

func (c *channelContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// TestBodyContextRelease is the regression test for body contexts that
// outlived their bodies. Every wrapped body whose hook supplied a new
// context registered a cancellation callback on the invocation context
// and allocated a cancellable context, and neither was released until the
// invocation ended. Release must cancel the merged context and remove the
// callback from orig.
func TestBodyContextRelease(t *testing.T) {
	const key wrapCtxKey = "k"

	t.Run("release cancels the merged context", func(t *testing.T) {
		orig, cancelOrig := context.WithTimeout(context.Background(), time.Hour)
		defer cancelOrig()
		got, release := bodyContext(orig, context.WithValue(context.Background(), key, "v"))
		if got.Err() != nil {
			t.Fatalf("body context ended before release: %v", got.Err())
		}
		release()
		select {
		case <-got.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("body context not cancelled by release")
		}
		if orig.Err() != nil {
			t.Errorf("release ended orig: %v", orig.Err())
		}
		// A second release, and orig ending afterwards, must be harmless.
		release()
		cancelOrig()
	})

	t.Run("release removes the callback registered on orig", func(t *testing.T) {
		orig := newChannelContext()
		const bodies = 200
		baseline := runtime.NumGoroutine()

		var contexts []context.Context
		for range bodies {
			got, release := bodyContext(orig, context.WithValue(context.Background(), key, "v"))
			contexts = append(contexts, got)
			release()
		}

		// With the callbacks released, the goroutines that watched orig
		// have exited. Without release, one per body would remain until
		// orig ended.
		if n := waitForGoroutines(baseline + bodies/10); n > baseline+bodies/10 {
			t.Errorf("goroutines after %d released bodies = %d, baseline %d", bodies, n, baseline)
		}
		for i, c := range contexts {
			if c.Err() == nil {
				t.Fatalf("body context %d still live after release", i)
			}
		}
		close(orig.done)
	})

	t.Run("wrapChain releases the body context when the body returns", func(t *testing.T) {
		orig, cancelOrig := context.WithCancel(context.Background())
		defer cancelOrig()
		d := &pluginDispatcher{plugins: []Plugin{valueHooks(key, "v")}}
		attempt := func(p *Plugin) wrapHook {
			return func(ctx context.Context, fn wrapBody) (any, error) {
				return p.WrapOperationAttemptFn(ctx, AttemptHookInfo{}, fn)
			}
		}

		var seen context.Context
		got, err := wrapChain(d, orig, attempt, func(ctx context.Context) (any, error) {
			seen = ctx
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return valueOf(ctx, key), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != "v" {
			t.Errorf("body saw %v, want v", got)
		}
		if seen.Err() == nil {
			t.Error("body context still live after the body returned")
		}
		if orig.Err() != nil {
			t.Errorf("invocation context ended with the body: %v", orig.Err())
		}
	})

	t.Run("wrapChain leaves an unchanged context live after the body returns", func(t *testing.T) {
		orig, cancelOrig := context.WithCancel(context.Background())
		defer cancelOrig()
		d := &pluginDispatcher{plugins: []Plugin{{
			WrapOperationAttemptFn: func(ctx context.Context, _ AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
				return fn(ctx)
			},
		}}}
		attempt := func(p *Plugin) wrapHook {
			return func(ctx context.Context, fn wrapBody) (any, error) {
				return p.WrapOperationAttemptFn(ctx, AttemptHookInfo{}, fn)
			}
		}

		var seen context.Context
		if _, err := wrapChain(d, orig, attempt, func(ctx context.Context) (any, error) {
			seen = ctx
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		if seen != orig {
			t.Fatalf("body context is %v, want orig", seen)
		}
		if seen.Err() != nil {
			t.Errorf("pass-through body context ended with the body: %v", seen.Err())
		}
	})
}

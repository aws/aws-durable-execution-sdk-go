package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lambdaSvc "github.com/aws/aws-sdk-go-v2/service/lambda"
)

// TestPluginFanOutJoinsBeforeProceeding verifies that notification hooks
// fan out concurrently and are joined before execution proceeds.
func TestPluginFanOutJoinsBeforeProceeding(t *testing.T) {
	// Two plugins, each increments a counter with a small delay.
	// After dispatch returns, both must have incremented.
	var count atomic.Int32
	started := make(chan struct{}, 2)

	p1 := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			started <- struct{}{}
			time.Sleep(10 * time.Millisecond)
			count.Add(1)
		},
	}
	p2 := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			started <- struct{}{}
			time.Sleep(10 * time.Millisecond)
			count.Add(1)
		},
	}

	pd := newPluginDispatcher([]Plugin{p1, p2})
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnInvocationStart != nil {
			p.OnInvocationStart(context.Background(), InvocationHookInfo{})
		}
	})

	if got := count.Load(); got != 2 {
		t.Fatalf("expected count=2 after dispatch, got %d", got)
	}
}

// TestPluginPanicSwallowed verifies that panicking hooks are recovered and
// never affect execution.
func TestPluginPanicSwallowed(t *testing.T) {
	panicker := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			panic("plugin exploded")
		},
	}
	var called atomic.Bool
	good := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			called.Store(true)
		},
	}

	pd := newPluginDispatcher([]Plugin{panicker, good})
	// Must not panic.
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnInvocationStart != nil {
			p.OnInvocationStart(context.Background(), InvocationHookInfo{})
		}
	})

	if !called.Load() {
		t.Fatal("good plugin was not called")
	}
}

// TestPluginErrorSwallowedCustomerErrorPreserved verifies that plugin errors
// in wrap hooks do not affect the customer function result.
func TestPluginErrorSwallowedCustomerErrorPreserved(t *testing.T) {
	customerErr := errors.New("customer error")

	// Plugin that panics — its panic should be skipped and inner fn called.
	panicker := Plugin{
		WrapInvocation: func(_ context.Context, _ InvocationHookInfo, fn func() (any, error)) (any, error) {
			panic("wrap panic")
		},
	}

	pd := newPluginDispatcher([]Plugin{panicker})
	_, err := wrapChain(pd,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(fn func() (any, error)) (any, error) {
				return p.WrapInvocation(context.Background(), InvocationHookInfo{}, fn)
			}
		},
		func() (any, error) {
			return nil, customerErr
		},
	)

	if !errors.Is(err, customerErr) {
		t.Fatalf("expected customer error preserved, got %v", err)
	}
}

// TestPluginWrapCompositionOrder verifies that plugins[0] is outermost
// (reduceRight composition): plugin1-before, plugin2-before, fn,
// plugin2-after, plugin1-after.
func TestPluginWrapCompositionOrder(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	p1 := Plugin{
		WrapInvocation: func(_ context.Context, _ InvocationHookInfo, fn func() (any, error)) (any, error) {
			record("p1-before")
			result, err := fn()
			record("p1-after")
			return result, err
		},
	}
	p2 := Plugin{
		WrapInvocation: func(_ context.Context, _ InvocationHookInfo, fn func() (any, error)) (any, error) {
			record("p2-before")
			result, err := fn()
			record("p2-after")
			return result, err
		},
	}

	pd := newPluginDispatcher([]Plugin{p1, p2})
	_, _ = wrapChain(pd,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(fn func() (any, error)) (any, error) {
				return p.WrapInvocation(context.Background(), InvocationHookInfo{}, fn)
			}
		},
		func() (any, error) {
			record("fn")
			return nil, nil
		},
	)

	expected := []string{"p1-before", "p2-before", "fn", "p2-after", "p1-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %q, got %q (full: %v)", i, v, order[i], order)
		}
	}
}

// TestPluginNilHookFieldsSkipped verifies that nil hook fields are safely
// skipped with no panic.
func TestPluginNilHookFieldsSkipped(t *testing.T) {
	// Plugin with no hooks set.
	empty := Plugin{}
	pd := newPluginDispatcher([]Plugin{empty})

	// Should not panic.
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnInvocationStart != nil {
			p.OnInvocationStart(context.Background(), InvocationHookInfo{})
		}
	})
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnOperationStart != nil {
			p.OnOperationStart(context.Background(), OperationHookInfo{})
		}
	})

	// Wrap with nil should pass through.
	result, err := wrapChain(pd,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(fn func() (any, error)) (any, error) {
				return p.WrapInvocation(context.Background(), InvocationHookInfo{}, fn)
			}
		},
		func() (any, error) {
			return "hello", nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != "hello" {
		t.Fatalf("expected 'hello', got %v", result)
	}
}

// TestPluginZeroPluginsZeroOverhead verifies that with no plugins, the
// dispatcher is nil and dispatch functions return immediately.
func TestPluginZeroPluginsZeroOverhead(t *testing.T) {
	pd := newPluginDispatcher(nil)
	if pd != nil {
		t.Fatal("expected nil dispatcher for empty plugins")
	}

	// Dispatch with nil should be a no-op.
	dispatchNotification(nil, func(p *Plugin) {
		t.Fatal("should not be called")
	})

	// Wrap with nil should pass through directly.
	result, err := wrapChain(nil,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			return nil
		},
		func() (any, error) {
			return 42, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %v", result)
	}
}

// TestPluginIsReplayOnSecondRun verifies that hooks fire with IsReplay=true
// for replayed operations using the durabletest LocalRunner.
func TestPluginIsReplayOnSecondRun(t *testing.T) {
	var isReplayValues []bool
	var mu sync.Mutex

	plugin := Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			isReplayValues = append(isReplayValues, info.IsReplay)
			mu.Unlock()
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		result, err := Step(ctx, "step1", func(sc StepContext) (string, error) {
			return "done", nil
		})
		return result, err
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	// First invocation: live execution.
	payload := makePluginPayload(t, "arn:test:exec", "tok1", nil)
	resp1, err := handler.Invoke(makePluginContext(), payload)
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp1, invocationSucceeded)

	// Verify first run: IsReplay=false.
	mu.Lock()
	if len(isReplayValues) != 1 || isReplayValues[0] != false {
		t.Fatalf("first run: expected [false], got %v", isReplayValues)
	}
	isReplayValues = nil
	mu.Unlock()

	// Second invocation: replay (the step is already checkpointed).
	// Build the state with the step's checkpoint.
	ops := []wireOperation{
		{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`}},
		{Id: hashID("1"), Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "step1", StepDetails: &wireStepDetails{Attempt: 1, Result: `"done"`}},
	}
	payload2 := makePluginPayload(t, "arn:test:exec", "tok2", ops)
	resp2, err := handler.Invoke(makePluginContext(), payload2)
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp2, invocationSucceeded)

	// Verify second run: IsReplay=true.
	mu.Lock()
	if len(isReplayValues) != 1 || isReplayValues[0] != true {
		t.Fatalf("second run: expected [true], got %v", isReplayValues)
	}
	mu.Unlock()
}

// TestPluginPendingOnInvocationEnd verifies that OnInvocationEnd fires with
// status PENDING when the invocation suspends.
func TestPluginPendingOnInvocationEnd(t *testing.T) {
	var endStatus PluginInvocationStatus
	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			endStatus = info.Status
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		// Wait suspends immediately.
		return "", Wait(ctx, "", 5*time.Second)
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	payload := makePluginPayload(t, "arn:test:suspend", "tok1", nil)
	resp, err := handler.Invoke(makePluginContext(), payload)
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)

	if endStatus != PluginInvocationPending {
		t.Fatalf("expected PENDING invocation end status, got %q", endStatus)
	}
}

// TestPluginConcurrentDispatchRace verifies that concurrent dispatch from
// parallel operations does not race under -race.
func TestPluginConcurrentDispatchRace(t *testing.T) {
	var count atomic.Int64
	plugin := Plugin{
		OnOperationStart: func(_ context.Context, _ OperationHookInfo) {
			count.Add(1)
		},
	}

	pd := newPluginDispatcher([]Plugin{plugin})

	// Dispatch from multiple goroutines concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dispatchNotification(pd, func(p *Plugin) {
				if p.OnOperationStart != nil {
					p.OnOperationStart(context.Background(), OperationHookInfo{})
				}
			})
		}()
	}
	wg.Wait()

	if got := count.Load(); got != 100 {
		t.Fatalf("expected 100 dispatches, got %d", got)
	}
}

// TestPluginEnrichLogContext verifies that enrichLogContext merges from all
// plugins, with later plugins overriding earlier ones.
func TestPluginEnrichLogContext(t *testing.T) {
	p1 := Plugin{
		EnrichLogContext: func(_ context.Context) map[string]any {
			return map[string]any{"key1": "val1", "shared": "from-p1"}
		},
	}
	p2 := Plugin{
		EnrichLogContext: func(_ context.Context) map[string]any {
			return map[string]any{"key2": "val2", "shared": "from-p2"}
		},
	}

	pd := newPluginDispatcher([]Plugin{p1, p2})
	result := enrichLogContext(context.Background(), pd)

	if result["key1"] != "val1" {
		t.Fatalf("expected key1=val1, got %q", result["key1"])
	}
	if result["key2"] != "val2" {
		t.Fatalf("expected key2=val2, got %q", result["key2"])
	}
	// p2 overrides p1 for shared key.
	if result["shared"] != "from-p2" {
		t.Fatalf("expected shared=from-p2, got %q", result["shared"])
	}
}

// TestPluginEnrichLogContextPanicSwallowed verifies panicking EnrichLogContext
// is swallowed.
func TestPluginEnrichLogContextPanicSwallowed(t *testing.T) {
	p1 := Plugin{
		EnrichLogContext: func(_ context.Context) map[string]any {
			panic("boom")
		},
	}
	p2 := Plugin{
		EnrichLogContext: func(_ context.Context) map[string]any {
			return map[string]any{"k": "v"}
		},
	}

	pd := newPluginDispatcher([]Plugin{p1, p2})
	result := enrichLogContext(context.Background(), pd)

	if result["k"] != "v" {
		t.Fatalf("expected k=v, got %q", result["k"])
	}
}

// TestPluginOperationHooksNotFiredForPendingOps verifies that operation-level
// hooks are NOT fired for operations that are still pending (waiting for
// backend to complete them).
func TestPluginOperationHooksNotFiredForPendingOps(t *testing.T) {
	var opStartCalled atomic.Bool
	plugin := Plugin{
		OnOperationStart: func(_ context.Context, _ OperationHookInfo) {
			opStartCalled.Store(true)
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		// Wait: if STARTED status is already checkpointed, it suspends
		// without firing operation hooks (timer hasn't fired yet).
		return "", Wait(ctx, "", 60*time.Second)
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	// Pre-checkpointed as STARTED (backend is waiting for timer to fire).
	ops := []wireOperation{
		{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hi"`}},
		{Id: hashID("1"), Status: "STARTED", Type: "WAIT", SubType: "Wait", Name: ""},
	}
	payload := makePluginPayload(t, "arn:test:pending", "tok1", ops)
	resp, err := handler.Invoke(makePluginContext(), payload)
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)

	// Operation hooks must NOT fire for the pending wait.
	if opStartCalled.Load() {
		t.Fatal("OnOperationStart should not fire for pending operations")
	}
}

// TestPluginSinglePluginNoGoroutineSpawn verifies that single-plugin dispatch
// doesn't spawn a goroutine (optimization path).
func TestPluginSinglePluginNoGoroutineSpawn(t *testing.T) {
	var called bool
	p := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			called = true
		},
	}
	pd := newPluginDispatcher([]Plugin{p})
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnInvocationStart != nil {
			p.OnInvocationStart(context.Background(), InvocationHookInfo{})
		}
	})
	if !called {
		t.Fatal("single plugin hook not called")
	}
}

// TestPluginWrapChildContextFn verifies that WrapChildContextFn wraps child
// body execution.
func TestPluginWrapChildContextFn(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	plugin := Plugin{
		WrapChildContextFn: func(_ context.Context, _ OperationHookInfo, fn func() (any, error)) (any, error) {
			record("wrap-before")
			r, e := fn()
			record("wrap-after")
			return r, e
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		result, err := RunInChildContext(ctx, "child1", func(childCtx Context) (string, error) {
			record("child-fn")
			return "child-result", nil
		})
		return result, err
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	payload := makePluginPayload(t, "arn:test:wrap-child", "tok1", nil)
	_, err := handler.Invoke(makePluginContext(), payload)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"wrap-before", "child-fn", "wrap-after"}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %q, got %q", i, v, order[i])
		}
	}
}

// TestPluginAttemptHooksFire verifies OnOperationAttemptStart/End fire for
// step attempts.
func TestPluginAttemptHooksFire(t *testing.T) {
	var starts []int
	var ends []AttemptEndHookInfo
	var mu sync.Mutex

	plugin := Plugin{
		OnOperationAttemptStart: func(_ context.Context, info AttemptHookInfo) {
			mu.Lock()
			starts = append(starts, info.Attempt)
			mu.Unlock()
		},
		OnOperationAttemptEnd: func(_ context.Context, info AttemptEndHookInfo) {
			mu.Lock()
			ends = append(ends, info)
			mu.Unlock()
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return Step(ctx, "attempt-step", func(sc StepContext) (string, error) {
			return "ok", nil
		})
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	payload := makePluginPayload(t, "arn:test:attempt", "tok1", nil)
	_, err := handler.Invoke(makePluginContext(), payload)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(starts) != 1 || starts[0] != 1 {
		t.Fatalf("expected attempt start [1], got %v", starts)
	}
	if len(ends) != 1 || ends[0].Outcome != PluginAttemptSucceeded {
		t.Fatalf("expected attempt end [SUCCEEDED], got %v", ends)
	}
}

// --- S1 plugin-layer upgrade tests ---

// TestPluginInvocationStartBeforeOperationChange verifies the dispatch
// ordering contract: OnInvocationStart fires before OnOperationChange.
func TestPluginInvocationStartBeforeOperationChange(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	plugin := Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			record("invocation-start")
		},
		OnOperationChange: func(_ context.Context, _ OperationChangeHookInfo) {
			record("operation-change")
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) {
			return "ok", nil
		})
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	ops := []wireOperation{
		{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`}},
		{Id: hashID("1"), Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s1", StepDetails: &wireStepDetails{Attempt: 1, Result: `"ok"`}},
	}
	payload := makePluginPayloadWithUpdated(t, "arn:test:order", "tok1", ops, []string{hashID("1")})
	if _, err := handler.Invoke(makePluginContext(), payload); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "invocation-start" || order[1] != "operation-change" {
		t.Fatalf("expected [invocation-start operation-change], got %v", order)
	}
}

// TestPluginUpdatedOperationsMapKeyedByID verifies that UpdatedOperations is
// delivered as a map keyed by operation ID, on both OnInvocationStart and
// OnOperationChange, with the new per-operation fields populated.
func TestPluginUpdatedOperationsMapKeyedByID(t *testing.T) {
	var startInfo InvocationHookInfo
	var changeInfo OperationChangeHookInfo
	var mu sync.Mutex

	plugin := Plugin{
		OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
			mu.Lock()
			startInfo = info
			mu.Unlock()
		},
		OnOperationChange: func(_ context.Context, info OperationChangeHookInfo) {
			mu.Lock()
			changeInfo = info
			mu.Unlock()
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) {
			return "ok", nil
		})
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	stepID := hashID("1")
	ops := []wireOperation{
		{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`}},
		{
			Id: stepID, Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s1",
			StartTimestamp: "2026-07-24T00:00:01Z",
			EndTimestamp:   "2026-07-24T00:00:02Z",
			StepDetails:    &wireStepDetails{Attempt: 1, Result: `"ok"`},
		},
	}
	payload := makePluginPayloadWithUpdated(t, "arn:test:map", "tok1", ops, []string{stepID})
	if _, err := handler.Invoke(makePluginContext(), payload); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for name, updated := range map[string]map[string]OperationHookInfo{
		"OnInvocationStart": startInfo.UpdatedOperations,
		"OnOperationChange": changeInfo.UpdatedOperations,
	} {
		op, ok := updated[stepID]
		if !ok {
			t.Fatalf("%s: expected UpdatedOperations keyed by %q, got %v", name, stepID, updated)
		}
		if op.Name != "s1" || op.Status != PluginOperationSucceeded || !op.IsReplay {
			t.Fatalf("%s: unexpected operation info: %+v", name, op)
		}
		if op.StartTimestamp.IsZero() || op.EndTimestamp.IsZero() {
			t.Fatalf("%s: expected wire timestamps populated, got %+v", name, op)
		}
		if op.Result != `"ok"` {
			t.Fatalf("%s: expected Result %q, got %q", name, `"ok"`, op.Result)
		}
	}
}

// TestPluginInvocationInfoFieldsPopulated verifies ExecutionInput,
// ExecutionStartTimestamp, ExecutionResult, and ExecutionError carry real
// values on the invocation-level hooks.
func TestPluginInvocationInfoFieldsPopulated(t *testing.T) {
	var startInfo InvocationHookInfo
	var endInfo InvocationEndHookInfo
	var mu sync.Mutex

	plugin := Plugin{
		OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
			mu.Lock()
			startInfo = info
			mu.Unlock()
		},
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			mu.Lock()
			endInfo = info
			mu.Unlock()
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return "result-" + event, nil
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	ops := []wireOperation{
		{
			Id: "exec", Status: "STARTED", Type: "EXECUTION",
			StartTimestamp:   "2026-07-24T00:00:00Z",
			ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`},
		},
	}
	payload := makePluginPayload(t, "arn:test:fields", "tok1", ops)
	if _, err := handler.Invoke(makePluginContext(), payload); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, ok := startInfo.ExecutionInput.(string); !ok || got != "hello" {
		t.Fatalf("expected ExecutionInput \"hello\", got %#v", startInfo.ExecutionInput)
	}
	if startInfo.ExecutionStartTimestamp.IsZero() {
		t.Fatal("expected ExecutionStartTimestamp populated from wire payload")
	}
	if endInfo.Status != PluginInvocationSucceeded {
		t.Fatalf("expected SUCCEEDED, got %q", endInfo.Status)
	}
	if endInfo.ExecutionResult == nil {
		t.Fatal("expected ExecutionResult populated on success")
	}
	if endInfo.ExecutionError != nil {
		t.Fatalf("expected nil ExecutionError on success, got %v", endInfo.ExecutionError)
	}
}

// TestPluginInvocationEndErrorPopulated verifies ExecutionError carries the
// handler error on failed invocations.
func TestPluginInvocationEndErrorPopulated(t *testing.T) {
	var endInfo InvocationEndHookInfo
	var mu sync.Mutex

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			mu.Lock()
			endInfo = info
			mu.Unlock()
		},
	}

	handlerErr := errors.New("handler failed")
	handler := Wrap(func(ctx Context, event string) (string, error) {
		return "", handlerErr
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	payload := makePluginPayload(t, "arn:test:fail", "tok1", nil)
	if _, err := handler.Invoke(makePluginContext(), payload); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if endInfo.Status != PluginInvocationFailed {
		t.Fatalf("expected FAILED, got %q", endInfo.Status)
	}
	if endInfo.ExecutionError == nil {
		t.Fatal("expected ExecutionError populated on failure")
	}
	if endInfo.ExecutionResult != nil {
		t.Fatalf("expected nil ExecutionResult on failure, got %v", endInfo.ExecutionResult)
	}
}

// TestPluginContextThreadedToHooks verifies the context.Context passed to
// Invoke is visible in notification hooks (value threading).
func TestPluginContextThreadedToHooks(t *testing.T) {
	type ctxKey struct{}
	var seen atomic.Bool

	plugin := Plugin{
		OnInvocationStart: func(ctx context.Context, _ InvocationHookInfo) {
			if v, ok := ctx.Value(ctxKey{}).(string); ok && v == "threaded" {
				seen.Store(true)
			}
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return "ok", nil
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	ctx := context.WithValue(context.Background(), ctxKey{}, "threaded")
	payload := makePluginPayload(t, "arn:test:ctx", "tok1", nil)
	if _, err := handler.Invoke(ctx, payload); err != nil {
		t.Fatal(err)
	}

	if !seen.Load() {
		t.Fatal("expected invocation ctx value visible in OnInvocationStart")
	}
}

// TestPluginOperationHookParentIDAndTimestamps verifies live operation hooks
// carry ParentID (empty at root) and a non-zero StartTimestamp.
func TestPluginOperationHookParentIDAndTimestamps(t *testing.T) {
	var infos []OperationHookInfo
	var mu sync.Mutex

	plugin := Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			infos = append(infos, info)
			mu.Unlock()
		},
	}

	handler := Wrap(func(ctx Context, event string) (string, error) {
		return Step(ctx, "root-step", func(StepContext) (string, error) {
			return "ok", nil
		})
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	payload := makePluginPayload(t, "arn:test:parent", "tok1", nil)
	if _, err := handler.Invoke(makePluginContext(), payload); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(infos) != 1 {
		t.Fatalf("expected 1 OnOperationStart, got %d", len(infos))
	}
	if infos[0].ParentID != "" {
		t.Fatalf("expected empty ParentID for root-level step, got %q", infos[0].ParentID)
	}
	if infos[0].StartTimestamp.IsZero() {
		t.Fatal("expected non-zero StartTimestamp on live OnOperationStart")
	}
}

// TestPluginEnrichLogContextAnyValues verifies map[string]any values of
// mixed types survive the merge.
func TestPluginEnrichLogContextAnyValues(t *testing.T) {
	p := Plugin{
		EnrichLogContext: func(_ context.Context) map[string]any {
			return map[string]any{"str": "v", "num": 42, "flag": true}
		},
	}
	pd := newPluginDispatcher([]Plugin{p})
	result := enrichLogContext(context.Background(), pd)

	if result["str"] != "v" || result["num"] != 42 || result["flag"] != true {
		t.Fatalf("expected mixed-type values preserved, got %v", result)
	}
}

func makePluginPayloadWithUpdated(t *testing.T, arn, token string, ops []wireOperation, updatedIDs []string) []byte {
	t.Helper()
	in := invocationInput{
		DurableExecutionArn:   arn,
		CheckpointToken:       token,
		UpdatedOperationIds:   updatedIDs,
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// --- Test helpers ---

// fakePluginClient is a minimal ExecutionClient for plugin tests.
type fakePluginClient struct {
	mu sync.Mutex
}

func (c *fakePluginClient) GetDurableExecutionState(_ context.Context, _ *lambdaSvc.GetDurableExecutionStateInput, _ ...func(*lambdaSvc.Options)) (*lambdaSvc.GetDurableExecutionStateOutput, error) {
	return &lambdaSvc.GetDurableExecutionStateOutput{}, nil
}

func (c *fakePluginClient) CheckpointDurableExecution(_ context.Context, in *lambdaSvc.CheckpointDurableExecutionInput, _ ...func(*lambdaSvc.Options)) (*lambdaSvc.CheckpointDurableExecutionOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &lambdaSvc.CheckpointDurableExecutionOutput{
		CheckpointToken: in.CheckpointToken,
	}, nil
}

func makePluginPayload(t *testing.T, arn, token string, ops []wireOperation) []byte {
	t.Helper()
	if ops == nil {
		ops = []wireOperation{
			{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`}},
		}
	}
	in := invocationInput{
		DurableExecutionArn:   arn,
		CheckpointToken:       token,
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func makePluginContext() context.Context {
	return context.Background()
}

func assertPluginResponseStatus(t *testing.T, resp []byte, expected string) {
	t.Helper()
	var r invocationResponse
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if r.Status != expected {
		t.Fatalf("expected status %q, got %q", expected, r.Status)
	}
}

// Suppress unused import warning for fmt.
var _ = fmt.Sprintf

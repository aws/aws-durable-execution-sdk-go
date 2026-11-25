package durable

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestInvocationHookInfoOperationsFullSet asserts InvocationHookInfo lists
// every operation known at invocation start, keyed by wire ID: an operation
// checkpointed by an earlier invocation that did not change in this one,
// the operation that did change, and the root execution operation.
// UpdatedOperations holds only the changed operation and is a subset of
// Operations. Both OnInvocationStart and WrapInvocation receive the map.
func TestInvocationHookInfoOperationsFullSet(t *testing.T) {
	var startInfo, wrapInfo InvocationHookInfo
	var mu sync.Mutex
	plugin := Plugin{
		OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
			mu.Lock()
			startInfo = info
			mu.Unlock()
		},
		WrapInvocation: func(ctx context.Context, info InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			mu.Lock()
			wrapInfo = info
			mu.Unlock()
			return fn(ctx)
		},
	}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil }); err != nil {
			return "", err
		}
		return Step(ctx, "s2", func(StepContext) (string, error) { return "two", nil })
	}, withTestPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	// s1 was checkpointed by an earlier invocation and did not change; s2
	// changed since the previous invocation.
	s1, s2 := hashID("1"), hashID("2")
	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: s1, Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s1",
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
			StepDetails:    &wireStepDetails{Attempt: 1, Result: `"one"`}},
		{Id: s2, Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s2",
			StepDetails: &wireStepDetails{Attempt: 1, Result: `"two"`}},
	}
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:ops", "tok1", ops, []string{s2}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	mu.Lock()
	defer mu.Unlock()
	for name, info := range map[string]InvocationHookInfo{"OnInvocationStart": startInfo, "WrapInvocation": wrapInfo} {
		if len(info.Operations) != 3 {
			t.Fatalf("%s: Operations has %d entries, want 3: %v", name, len(info.Operations), info.Operations)
		}
		unchanged, ok := info.Operations[s1]
		if !ok {
			t.Fatalf("%s: Operations lacks the unchanged operation %q", name, s1)
		}
		if unchanged.ID != s1 || unchanged.Name != "s1" || unchanged.Status != PluginOperationSucceeded || !unchanged.IsReplay ||
			unchanged.Result != `"one"` || unchanged.Type != "STEP" || unchanged.SubType != "Step" {
			t.Errorf("%s: unchanged operation info = %+v", name, unchanged)
		}
		if !unchanged.StartTimestamp.Equal(lifecycleStart) || !unchanged.EndTimestamp.Equal(lifecycleEnd) {
			t.Errorf("%s: unchanged operation timestamps = %v %v, want the checkpointed ones", name, unchanged.StartTimestamp, unchanged.EndTimestamp)
		}
		if _, ok := info.Operations["exec"]; !ok {
			t.Errorf("%s: Operations lacks the execution operation", name)
		}
		if len(info.UpdatedOperations) != 1 {
			t.Fatalf("%s: UpdatedOperations = %v, want only %q", name, info.UpdatedOperations, s2)
		}
		for id, updated := range info.UpdatedOperations {
			full, ok := info.Operations[id]
			if !ok {
				t.Errorf("%s: updated operation %q missing from Operations", name, id)
			}
			if full.Status != updated.Status || full.Name != updated.Name || full.Result != updated.Result {
				t.Errorf("%s: Operations[%q] = %+v, UpdatedOperations[%q] = %+v, want the same record", name, id, full, id, updated)
			}
		}
	}
}

// TestInvocationHookInfoOperationsFirstInvocation asserts that on the first
// invocation Operations holds the execution operation alone and
// UpdatedOperations is empty.
func TestInvocationHookInfoOperationsFirstInvocation(t *testing.T) {
	var got InvocationHookInfo
	var mu sync.Mutex
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil })
	}, withTestPlugins(Plugin{OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
		mu.Lock()
		got = info
		mu.Unlock()
	}}), withLambdaAPI(&fakePluginClient{}))
	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:ops", "tok1", nil)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !got.IsFirstInvocation {
		t.Error("IsFirstInvocation = false, want true")
	}
	if len(got.Operations) != 1 || got.Operations["exec"].Type != "EXECUTION" {
		t.Errorf("Operations = %v, want the execution operation alone", got.Operations)
	}
	if len(got.UpdatedOperations) != 0 {
		t.Errorf("UpdatedOperations = %v, want empty", got.UpdatedOperations)
	}
}

// TestInvocationHookInfoOperationsNotMutated asserts the map a hook
// received does not change while the handler checkpoints new operations:
// it is a snapshot of the state at invocation start.
func TestInvocationHookInfoOperationsNotMutated(t *testing.T) {
	var got InvocationHookInfo
	var mu sync.Mutex
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s2", func(StepContext) (string, error) { return "two", nil })
	}, withTestPlugins(Plugin{OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
		mu.Lock()
		got = info
		mu.Unlock()
	}}), withLambdaAPI(&fakePluginClient{newState: []Operation{{
		Id: aws.String(hashID("1")), Status: OperationStatusSucceeded, Type: OperationTypeStep,
		StepDetails: &StepDetails{Attempt: 1, Result: aws.String(`"two"`)},
	}}}))
	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:ops", "tok1", nil)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got.Operations) != 1 {
		t.Errorf("Operations = %v, want the execution operation alone: the step checkpointed later must not appear", got.Operations)
	}
}

// TestInvocationHookInfoOperationsBuiltForFactory asserts the full
// operation set is built whenever a factory is registered, even when the
// plugin the factory returns has no hook that receives InvocationHookInfo.
// The factory itself is the consumer: it reads Operations before it
// decides what to return. This is the per-invocation cost the factory
// contract makes explicit.
func TestInvocationHookInfoOperationsBuiltForFactory(t *testing.T) {
	var seen InvocationHookInfo
	var changed OperationChangeHookInfo
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(_ context.Context, info InvocationHookInfo) (*Plugin, error) {
		mu.Lock()
		seen = info
		mu.Unlock()
		// Operation-level hooks only: no OnInvocationStart, no WrapInvocation.
		return &Plugin{OnOperationChange: func(_ context.Context, info OperationChangeHookInfo) {
			mu.Lock()
			changed = info
			mu.Unlock()
		}}, nil
	})
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil })
	}, WithPlugins(factory), withLambdaAPI(&fakePluginClient{}))
	s1 := hashID("1")
	ops := []wireOperation{lifecycleExecOp(), {Id: s1, Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s1",
		StartTimestamp: flexTimestamp{Time: time.Now(), Valid: true},
		StepDetails:    &wireStepDetails{Attempt: 1, Result: `"one"`}}}
	if _, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:ops", "tok1", ops, []string{s1})); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen.Operations) != 2 {
		t.Errorf("factory saw Operations = %v, want the execution operation and the step", seen.Operations)
	}
	if _, ok := seen.Operations[s1]; !ok {
		t.Errorf("factory saw Operations without the checkpointed step %q: %v", s1, seen.Operations)
	}
	if len(seen.UpdatedOperations) != 1 {
		t.Errorf("factory saw UpdatedOperations = %v, want the changed step", seen.UpdatedOperations)
	}
	if len(changed.UpdatedOperations) != 1 {
		t.Errorf("OnOperationChange UpdatedOperations = %v, want the changed step", changed.UpdatedOperations)
	}
}

package durable

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// endInfoRecorder is a plugin that records the info of OnInvocationStart
// and of OnInvocationEnd. It implements no OnOperationChange. So the tests
// show that OnInvocationEnd alone gets the complete Operations map.
type endInfoRecorder struct {
	mu    sync.Mutex
	start InvocationHookInfo
	end   InvocationEndHookInfo
}

func (r *endInfoRecorder) plugin() Plugin {
	return Plugin{
		OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
			r.mu.Lock()
			r.start = info
			r.mu.Unlock()
		},
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			r.mu.Lock()
			r.end = info
			r.mu.Unlock()
		},
	}
}

// TestInvocationEndOperationsHoldsOperationCheckpointedInInvocation runs
// one new step in an invocation that also replays an older step.
// InvocationEndHookInfo.Operations holds the new step with its checkpointed
// status, result, timestamps, and attempt, and IsReplay false. The
// Operations map of OnInvocationStart of the same invocation lacks it. The
// older step is in both maps, and its end entry has IsReplay true.
func TestInvocationEndOperationsHoldsOperationCheckpointedInInvocation(t *testing.T) {
	rec := &endInfoRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "old", func(StepContext) (string, error) { return "one", nil }); err != nil {
			return "", err
		}
		return Step(ctx, "new", func(StepContext) (string, error) { return "two", nil })
	}, withTestPlugins(rec.plugin()), withLambdaAPI(stepEchoClient{}))

	oldID, newID := hashID("1"), hashID("2")
	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: oldID, Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "old",
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
			StepDetails:    &wireStepDetails{Attempt: 1, Result: `"one"`}},
	}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:end-ops", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, ok := rec.start.Operations[newID]; ok {
		t.Errorf("OnInvocationStart Operations holds the step checkpointed later: %v", rec.start.Operations)
	}
	if rec.end.Status != PluginInvocationSucceeded {
		t.Errorf("end Status = %v, want succeeded", rec.end.Status)
	}
	if len(rec.end.Operations) != 3 {
		t.Fatalf("end Operations has %d entries, want the execution and two steps: %v", len(rec.end.Operations), rec.end.Operations)
	}
	if rec.end.Operations["exec"].Type != "EXECUTION" {
		t.Errorf("end Operations lacks the execution operation: %v", rec.end.Operations)
	}
	got, ok := rec.end.Operations[newID]
	if !ok {
		t.Fatalf("end Operations lacks the step checkpointed during the invocation: %v", rec.end.Operations)
	}
	if got.ID != newID || got.Name != "new" || got.Type != "STEP" || got.Status != PluginOperationSucceeded ||
		got.Result != `"two"` || got.Attempt != 1 || got.IsReplay {
		t.Errorf("new step entry = %+v, want SUCCEEDED, result \"two\", attempt 1, IsReplay false", got)
	}
	if !got.StartTimestamp.Equal(lifecycleStart) || !got.EndTimestamp.Equal(lifecycleEnd) {
		t.Errorf("new step timestamps = %v %v, want the checkpointed ones", got.StartTimestamp, got.EndTimestamp)
	}
	old, ok := rec.end.Operations[oldID]
	if !ok || old.Status != PluginOperationSucceeded || !old.IsReplay {
		t.Errorf("old step entry = %+v (present %v), want SUCCEEDED with IsReplay true", old, ok)
	}
}

// TestInvocationEndOperationsHoldsCallInFlightWhenInvocationPends asserts
// that an invocation waits for a checkpoint call in flight before it
// dispatches OnInvocationEnd, when OnInvocationEnd is the only hook that
// reads the operations. One branch blocks on a wait, so the invocation is
// pending. A second branch starts a step, and the client holds that call.
// The handler returns while the call is held. The Operations map still
// holds the step as STARTED, with IsReplay false.
func TestInvocationEndOperationsHoldsCallInFlightWhenInvocationPends(t *testing.T) {
	rec := &endInfoRecorder{}
	client := &slowStepStartClient{
		// hold must exceed the handler's sleep below by a wide margin.
		hold:     time.Second,
		waitSeen: make(chan struct{}),
		stepSent: make(chan struct{}),
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		Go(ctx, "waiter", func(c Context) (string, error) {
			return "", Wait(c, "pause", time.Hour)
		})
		<-client.waitSeen
		Go(ctx, "stepper", func(c Context) (string, error) {
			return Step(c, "held", func(StepContext) (string, error) {
				<-release
				return "", nil
			})
		})
		<-client.stepSent
		// The waiter branch must be blocked on its wait before the
		// handler returns, or the invocation does not end as pending.
		// The package exposes no signal for that, so the handler sleeps.
		time.Sleep(200 * time.Millisecond)
		return "ignored", nil
	}, withTestPlugins(rec.plugin()), withLambdaAPI(client))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:end-ops-in-flight", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	var held *OperationHookInfo
	for _, op := range rec.end.Operations {
		if op.Name == "held" {
			held = &op
		}
	}
	if held == nil {
		t.Fatalf("end Operations lacks the step whose checkpoint call was in flight when the handler returned: %v", rec.end.Operations)
	}
	if held.Status != PluginOperationStarted || held.IsReplay {
		t.Errorf("held step = %+v, want STARTED with IsReplay false", *held)
	}
}

// TestInvocationEndOperationsOnFailedInvocation asserts the map is set when
// the handler returns an error: the dispatch carries it whatever the
// invocation status.
func TestInvocationEndOperationsOnFailedInvocation(t *testing.T) {
	rec := &endInfoRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil }); err != nil {
			return "", err
		}
		return "", errors.New("handler failed")
	}, withTestPlugins(rec.plugin()), withLambdaAPI(stepEchoClient{}))
	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:end-ops", "tok1", nil)); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.end.Status != PluginInvocationFailed || rec.end.ExecutionError == nil {
		t.Fatalf("end info = %+v, want a failed invocation with an error", rec.end)
	}
	if got := rec.end.Operations[hashID("1")]; got.Status != PluginOperationSucceeded {
		t.Errorf("end Operations = %v, want the step SUCCEEDED", rec.end.Operations)
	}
}

// TestSnapshotOperationErrorIsOperationError asserts the Error of a
// snapshot entry for each operation type that checkpoints an error. The
// error is an *OperationError with the operation's name and the four
// checkpointed fields. Its Err is the stand-in that was the whole value
// before, so its text ends with "<ErrorType>: <Message>". A failed
// operation whose checkpoint holds no error has a nil Error.
func TestSnapshotOperationErrorIsOperationError(t *testing.T) {
	checkpointed := func() *wireError {
		return &wireError{
			ErrorType: "PaymentDeclinedError", ErrorMessage: "card expired",
			ErrorData: `{"code":7}`, StackTrace: []string{"frame-1", "frame-2"},
		}
	}
	cases := []struct {
		label string
		op    wireOperation
		// wantNil is true when the checkpoint holds no error.
		wantNil bool
	}{
		{label: "step", op: wireOperation{Type: "STEP", SubType: "Step", Name: "charge",
			StepDetails: &wireStepDetails{Attempt: 3, Error: checkpointed()}}},
		{label: "wait for condition", op: wireOperation{Type: "STEP", SubType: "WaitForCondition", Name: "poll",
			StepDetails: &wireStepDetails{Attempt: 2, Error: checkpointed()}}},
		{label: "callback", op: wireOperation{Type: "CALLBACK", SubType: "Callback", Name: "approve",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1", Error: checkpointed()}}},
		{label: "invoke", op: wireOperation{Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", Name: "call",
			ChainedInvokeDetails: &wireChainedInvokeDetails{Error: checkpointed()}}},
		{label: "child context", op: wireOperation{Type: "CONTEXT", SubType: "RunInChildContext", Name: "group",
			ContextDetails: &wireContextDetails{Error: checkpointed()}}},
		{label: "no name", op: wireOperation{Type: "STEP", SubType: "Step",
			StepDetails: &wireStepDetails{Attempt: 1, Error: checkpointed()}}},
		{label: "callback with no checkpointed error", wantNil: true, op: wireOperation{Type: "CALLBACK", SubType: "Callback", Name: "approve",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-2"}}},
	}

	// The handler runs no operation. The failed operations come from the
	// invocation payload, so each entry is built from the checkpoint alone.
	ops := []wireOperation{lifecycleExecOp()}
	for i, tc := range cases {
		op := tc.op
		op.Id = hashID(string(rune('1' + i)))
		op.Status = "FAILED"
		op.StartTimestamp = flexTimestamp{Time: lifecycleStart, Valid: true}
		op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}
		ops = append(ops, op)
	}
	rec := &endInfoRecorder{}
	handler := Wrap(func(Context, string) (string, error) { return "done", nil },
		withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
	if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:end-ops", "tok1", ops)); err != nil {
		t.Fatal(err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	maps := map[string]map[string]OperationHookInfo{
		"OnInvocationStart": rec.start.Operations,
		"OnInvocationEnd":   rec.end.Operations,
	}
	for hook, snapshot := range maps {
		for i, tc := range cases {
			id := ops[i+1].Id
			entry, ok := snapshot[id]
			if !ok {
				t.Errorf("%s, %s: Operations lacks the operation", hook, tc.label)
				continue
			}
			if entry.Status != PluginOperationFailed {
				t.Errorf("%s, %s: Status = %v, want FAILED", hook, tc.label, entry.Status)
			}
			if tc.wantNil {
				if entry.Error != nil {
					t.Errorf("%s, %s: Error = %v, want nil", hook, tc.label, entry.Error)
				}
				continue
			}
			var opErr *OperationError
			if !errors.As(entry.Error, &opErr) {
				t.Errorf("%s, %s: Error = %T %v, want an *OperationError", hook, tc.label, entry.Error, entry.Error)
				continue
			}
			if opErr.Name != tc.op.Name || opErr.ErrorType != "PaymentDeclinedError" || opErr.Message != "card expired" ||
				opErr.ErrorData != `{"code":7}` || !slices.Equal(opErr.StackTrace, []string{"frame-1", "frame-2"}) {
				t.Errorf("%s, %s: OperationError = %+v, want the name and the four checkpointed fields", hook, tc.label, *opErr)
			}
			if opErr.Err == nil || opErr.Err.Error() != "PaymentDeclinedError: card expired" {
				t.Errorf("%s, %s: Err = %v, want the stand-in \"PaymentDeclinedError: card expired\"", hook, tc.label, opErr.Err)
			}
			want := `durable: operation "` + tc.op.Name + `" failed: PaymentDeclinedError: card expired`
			if entry.Error.Error() != want {
				t.Errorf("%s, %s: Error text = %q, want %q", hook, tc.label, entry.Error.Error(), want)
			}
		}
	}
}

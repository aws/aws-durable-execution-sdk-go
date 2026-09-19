package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// opEvent is one recorded operation lifecycle hook call.
type opEvent struct {
	hook string // "start" or "end"
	info OperationHookInfo
}

// opRecorder is a plugin that records every OnOperationStart and
// OnOperationEnd call in dispatch order.
type opRecorder struct {
	mu     sync.Mutex
	events []opEvent
}

func (r *opRecorder) plugin() Plugin {
	return Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			r.mu.Lock()
			r.events = append(r.events, opEvent{hook: "start", info: info})
			r.mu.Unlock()
		},
		OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			r.mu.Lock()
			r.events = append(r.events, opEvent{hook: "end", info: info})
			r.mu.Unlock()
		},
	}
}

// take returns the recorded events and clears the recorder.
func (r *opRecorder) take() []opEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := r.events
	r.events = nil
	return evs
}

// forName returns the events for the operation with the given name.
func eventsForName(evs []opEvent, name string) []opEvent {
	var out []opEvent
	for _, ev := range evs {
		if ev.info.Name == name {
			out = append(out, ev)
		}
	}
	return out
}

// summarize renders events as "hook:name:status:replay" strings for
// sequence assertions.
func summarize(evs []opEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, fmt.Sprintf("%s:%s:%s:%v", ev.hook, ev.info.Name, ev.info.Status, ev.info.IsReplay))
	}
	return out
}

func assertSequence(t *testing.T, got []opEvent, want ...string) {
	t.Helper()
	g := summarize(got)
	if len(g) != len(want) {
		t.Fatalf("event sequence = %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("event sequence = %v, want %v", g, want)
		}
	}
}

var (
	lifecycleStart = time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	lifecycleEnd   = time.Date(2026, 8, 1, 10, 0, 5, 0, time.UTC)
)

func lifecycleExecOp() wireOperation {
	return wireOperation{Id: "exec", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `"hello"`}}
}

// assertIdentity checks the identity fields the helper fills for every
// hook of one operation.
func assertIdentity(t *testing.T, ev opEvent, id, name, opType, subType, parentID string) {
	t.Helper()
	if ev.info.ID != id {
		t.Errorf("%s ID = %q, want %q", ev.hook, ev.info.ID, id)
	}
	if ev.info.Name != name {
		t.Errorf("%s Name = %q, want %q", ev.hook, ev.info.Name, name)
	}
	if ev.info.Type != opType {
		t.Errorf("%s Type = %q, want %q", ev.hook, ev.info.Type, opType)
	}
	if ev.info.SubType != subType {
		t.Errorf("%s SubType = %q, want %q", ev.hook, ev.info.SubType, subType)
	}
	if ev.info.ParentID != parentID {
		t.Errorf("%s ParentID = %q, want %q", ev.hook, ev.info.ParentID, parentID)
	}
	if ev.info.ExecutionArn == "" {
		t.Errorf("%s ExecutionArn is empty", ev.hook)
	}
}

// runSuspendResume drives handler through a first invocation with an empty
// state, asserts it suspends, then a second invocation with resumeOps in
// the state, asserts it finishes with wantFinal, and returns the events of
// each invocation.
func runSuspendResume(t *testing.T, handler func(context.Context, []byte) ([]byte, error), rec *opRecorder, resumeOps []wireOperation, wantFinal string) (first, second []opEvent) {
	t.Helper()
	return runSuspendResumeUpdated(t, handler, rec, resumeOps, nil, wantFinal)
}

// runSuspendResumeUpdated is runSuspendResume whose second invocation
// payload lists updatedIDs (wire IDs) as the operations updated since the
// first invocation.
func runSuspendResumeUpdated(t *testing.T, handler func(context.Context, []byte) ([]byte, error), rec *opRecorder, resumeOps []wireOperation, updatedIDs []string, wantFinal string) (first, second []opEvent) {
	t.Helper()
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	first = rec.take()

	resp, err = handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok2", append([]wireOperation{lifecycleExecOp()}, resumeOps...), updatedIDs))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, wantFinal)
	second = rec.take()
	return first, second
}

// waitLifecycleHandlers returns the blocking and async wait handlers under
// test, each followed by a step so the second invocation also emits live
// events after the replayed wait.
func waitLifecycleHandlers(rec *opRecorder) map[string]func(context.Context, []byte) ([]byte, error) {
	after := func(ctx Context) (string, error) {
		return Step(ctx, "after", func(StepContext) (string, error) { return "ok", nil })
	}
	return map[string]func(context.Context, []byte) ([]byte, error){
		"Wait": Wrap(func(ctx Context, _ string) (string, error) {
			if err := Wait(ctx, "pause", 5*time.Second); err != nil {
				return "", err
			}
			return after(ctx)
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		"WaitAsync": Wrap(func(ctx Context, _ string) (string, error) {
			fut := WaitAsync(ctx, "pause", 5*time.Second)
			if _, err := fut.Result(); err != nil {
				return "", err
			}
			return after(ctx)
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
	}
}

// TestOperationLifecycleWaitSuspendResume asserts the event sequence for a
// wait across a suspend-and-resume cycle, for Wait and WaitAsync.
//
// Invocation 1 runs the wait live: one STARTED start, no end, then the
// invocation suspends. Invocation 2 replays the wait as SUCCEEDED: no start
// and one replayed end carrying the checkpointed timestamps, then the
// following step runs live.
func TestOperationLifecycleWaitSuspendResume(t *testing.T) {
	for variant := range waitLifecycleHandlers(&opRecorder{}) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			handler := waitLifecycleHandlers(rec)[variant]
			resumeOps := []wireOperation{{
				Id: hashID("1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause",
				StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
				EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
			}}
			first, second := runSuspendResume(t, handler, rec, resumeOps, invocationSucceeded)

			assertSequence(t, first, "start:pause:STARTED:false")
			live := first[0]
			assertIdentity(t, live, "1", "pause", string(OperationTypeWait), OperationSubTypeWait, "")
			if live.info.StartTimestamp.IsZero() {
				t.Error("live start: StartTimestamp is zero")
			}
			if !live.info.EndTimestamp.IsZero() {
				t.Error("live start: EndTimestamp must be zero")
			}

			assertSequence(t, second,
				"end:pause:SUCCEEDED:true",
				"start:after:STARTED:false",
				"end:after:SUCCEEDED:false",
			)
			replayedEnd := second[0]
			assertIdentity(t, replayedEnd, "1", "pause", string(OperationTypeWait), OperationSubTypeWait, "")
			if !replayedEnd.info.StartTimestamp.Equal(lifecycleStart) {
				t.Errorf("replayed end: StartTimestamp = %v, want %v", replayedEnd.info.StartTimestamp, lifecycleStart)
			}
			if !replayedEnd.info.EndTimestamp.Equal(lifecycleEnd) {
				t.Errorf("replayed end: EndTimestamp = %v, want %v", replayedEnd.info.EndTimestamp, lifecycleEnd)
			}
		})
	}
}

// TestOperationLifecycleWaitReplayedPendingSuspends asserts that a wait
// replayed while still STARTED dispatches one replayed start and no end,
// for Wait and WaitAsync.
func TestOperationLifecycleWaitReplayedPendingSuspends(t *testing.T) {
	for variant := range waitLifecycleHandlers(&opRecorder{}) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			handler := waitLifecycleHandlers(rec)[variant]
			ops := []wireOperation{lifecycleExecOp(), {
				Id: hashID("1"), Status: "STARTED", Type: "WAIT", SubType: "Wait", Name: "pause",
				StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			}}
			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationPending)
			evs := rec.take()
			assertSequence(t, evs, "start:pause:STARTED:true")
			if !evs[0].info.StartTimestamp.Equal(lifecycleStart) {
				t.Errorf("StartTimestamp = %v, want %v", evs[0].info.StartTimestamp, lifecycleStart)
			}
		})
	}
}

// invokeLifecycleHandlers returns the blocking and async invoke handlers
// under test.
func invokeLifecycleHandlers(rec *opRecorder) map[string]func(context.Context, []byte) ([]byte, error) {
	return map[string]func(context.Context, []byte) ([]byte, error){
		"Invoke": Wrap(func(ctx Context, _ string) (string, error) {
			return Invoke[string](ctx, "call", "target-fn", "in")
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		"InvokeAsync": Wrap(func(ctx Context, _ string) (string, error) {
			return InvokeAsync[string](ctx, "call", "target-fn", "in").Result()
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
	}
}

// TestOperationLifecycleInvokeSuspendResume asserts the event sequence for
// an invoke across a suspend-and-resume cycle, for Invoke and InvokeAsync.
//
// Invocation 1 starts the invoke live: one STARTED start, no end, then the
// invocation suspends. Invocation 2 replays the settled invoke: no start
// and one replayed end with the checkpointed timestamps and outcome. A
// succeeded, a failed, and a timed-out outcome are covered.
func TestOperationLifecycleInvokeSuspendResume(t *testing.T) {
	outcomes := []struct {
		name       string
		op         wireOperation
		wantStatus PluginOperationStatus
		wantFinal  string
	}{
		{
			name: "succeeded",
			op: wireOperation{
				Status:               "SUCCEEDED",
				ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"out"`},
			},
			wantStatus: PluginOperationSucceeded,
			wantFinal:  invocationSucceeded,
		},
		{
			name: "failed",
			op: wireOperation{
				Status: "FAILED",
				ChainedInvokeDetails: &wireChainedInvokeDetails{
					Error: &wireFullError{ErrorType: "Error", ErrorMessage: "target blew up"},
				},
			},
			wantStatus: PluginOperationFailed,
			wantFinal:  invocationFailed,
		},
		{
			name: "timed out",
			op: wireOperation{
				Status: "TIMED_OUT",
				ChainedInvokeDetails: &wireChainedInvokeDetails{
					Error: &wireFullError{ErrorType: "Error", ErrorMessage: "too slow"},
				},
			},
			wantStatus: PluginOperationTimedOut,
			wantFinal:  invocationFailed,
		},
	}
	for variant := range invokeLifecycleHandlers(&opRecorder{}) {
		for _, tc := range outcomes {
			t.Run(variant+"/"+tc.name, func(t *testing.T) {
				rec := &opRecorder{}
				handler := invokeLifecycleHandlers(rec)[variant]
				op := tc.op
				op.Id = hashID("1")
				op.Type = "CHAINED_INVOKE"
				op.SubType = "ChainedInvoke"
				op.Name = "call"
				op.StartTimestamp = flexTimestamp{Time: lifecycleStart, Valid: true}
				op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}
				first, second := runSuspendResume(t, handler, rec, []wireOperation{op}, tc.wantFinal)

				assertSequence(t, first, "start:call:STARTED:false")
				live := first[0]
				assertIdentity(t, live, "1", "call", string(OperationTypeChainedInvoke), OperationSubTypeChainedInvoke, "")
				if live.info.StartTimestamp.IsZero() {
					t.Error("live start: StartTimestamp is zero")
				}

				want := string(tc.wantStatus)
				assertSequence(t, second, "end:call:"+want+":true")
				ev := second[0]
				assertIdentity(t, ev, "1", "call", string(OperationTypeChainedInvoke), OperationSubTypeChainedInvoke, "")
				if !ev.info.StartTimestamp.Equal(lifecycleStart) {
					t.Errorf("replayed end StartTimestamp = %v, want %v", ev.info.StartTimestamp, lifecycleStart)
				}
				if !ev.info.EndTimestamp.Equal(lifecycleEnd) {
					t.Errorf("replayed end EndTimestamp = %v, want %v", ev.info.EndTimestamp, lifecycleEnd)
				}
				if tc.wantStatus == PluginOperationSucceeded {
					if ev.info.Result != `"out"` {
						t.Errorf("replayed end Result = %q, want %q", ev.info.Result, `"out"`)
					}
					if ev.info.Error != nil {
						t.Errorf("replayed end Error = %v, want nil", ev.info.Error)
					}
				} else {
					if ev.info.Error == nil {
						t.Error("replayed end Error is nil, want the recorded failure")
					}
					if ev.info.Result != "" {
						t.Errorf("replayed end Result = %q, want empty", ev.info.Result)
					}
				}
			})
		}
	}
}

// TestOperationLifecycleInvokeReplayedSucceededSerdesFailure asserts that a
// SUCCEEDED invoke whose result cannot be deserialized still dispatches one
// replayed end event, for Invoke and InvokeAsync. The invoke reached its
// terminal state when the checkpoint recorded it; the result Serdes failure
// belongs to the caller, so it surfaces as a SerdesError and does not
// suppress the lifecycle end event.
func TestOperationLifecycleInvokeReplayedSucceededSerdesFailure(t *testing.T) {
	cause := errors.New("unmarshal exploded")
	handlers := func(rec *opRecorder, got *error) map[string]func(context.Context, []byte) ([]byte, error) {
		serdes := WithInvokeResultSerdes(failingSerdes{failUnmarshal: true, cause: cause})
		return map[string]func(context.Context, []byte) ([]byte, error){
			"Invoke": Wrap(func(ctx Context, _ string) (string, error) {
				_, err := Invoke[string](ctx, "call", "target-fn", "in", serdes)
				*got = err
				return "", nil
			}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
			"InvokeAsync": Wrap(func(ctx Context, _ string) (string, error) {
				_, err := InvokeAsync[string](ctx, "call", "target-fn", "in", serdes).Result()
				*got = err
				return "", nil
			}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		}
	}
	for variant := range handlers(&opRecorder{}, new(error)) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			var got error
			handler := handlers(rec, &got)[variant]
			ops := []wireOperation{lifecycleExecOp(), {
				Id: hashID("1"), Status: "SUCCEEDED", Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", Name: "call",
				StartTimestamp:       flexTimestamp{Time: lifecycleStart, Valid: true},
				EndTimestamp:         flexTimestamp{Time: lifecycleEnd, Valid: true},
				ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"out"`},
			}}
			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			assertSerdesError(t, got, "call", "unmarshal", cause)

			evs := rec.take()
			assertSequence(t, evs, "end:call:SUCCEEDED:true")
			ev := evs[0]
			assertIdentity(t, ev, "1", "call", string(OperationTypeChainedInvoke), OperationSubTypeChainedInvoke, "")
			if !ev.info.StartTimestamp.Equal(lifecycleStart) {
				t.Errorf("replayed end StartTimestamp = %v, want %v", ev.info.StartTimestamp, lifecycleStart)
			}
			if !ev.info.EndTimestamp.Equal(lifecycleEnd) {
				t.Errorf("replayed end EndTimestamp = %v, want %v", ev.info.EndTimestamp, lifecycleEnd)
			}
			if ev.info.Result != `"out"` {
				t.Errorf("replayed end Result = %q, want %q", ev.info.Result, `"out"`)
			}
			if ev.info.Error != nil {
				t.Errorf("replayed end Error = %v, want nil", ev.info.Error)
			}
		})
	}
}

// TestOperationLifecycleInvokeReplayedUnsettledSuspends asserts that an
// invoke replayed while the invoked execution is unsettled dispatches one
// replayed start with the checkpointed status and no end.
func TestOperationLifecycleInvokeReplayedUnsettledSuspends(t *testing.T) {
	for variant := range invokeLifecycleHandlers(&opRecorder{}) {
		for _, status := range []string{"STARTED", "PENDING"} {
			t.Run(variant+"/"+status, func(t *testing.T) {
				rec := &opRecorder{}
				handler := invokeLifecycleHandlers(rec)[variant]
				ops := []wireOperation{lifecycleExecOp(), {
					Id: hashID("1"), Status: status, Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", Name: "call",
					StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
				}}
				resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
				if err != nil {
					t.Fatal(err)
				}
				assertPluginResponseStatus(t, resp, invocationPending)
				assertSequence(t, rec.take(), "start:call:"+status+":true")
			})
		}
	}
}

// TestOperationLifecycleParentIDInsideChildContext asserts that a wait and
// an invoke claimed inside a child context report the child context's wire
// ID as ParentID on both the live and the replayed hooks.
func TestOperationLifecycleParentIDInsideChildContext(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "child", func(c Context) (string, error) {
			if err := Wait(c, "pause", 5*time.Second); err != nil {
				return "", err
			}
			return Invoke[string](c, "call", "target-fn", "in")
		})
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	// The child context claims "1"; its operations claim "1-1", "1-2".
	wantParent := hashID("1")

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	first := eventsForName(rec.take(), "pause")
	assertSequence(t, first, "start:pause:STARTED:false")
	assertIdentity(t, first[0], "1-1", "pause", string(OperationTypeWait), OperationSubTypeWait, wantParent)

	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: hashID("1"), Status: "STARTED", Type: "CONTEXT", SubType: "RunInChildContext", Name: "child"},
		{Id: hashID("1-1"), ParentId: hashID("1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
	}
	resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok2", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	second := rec.take()
	pause := eventsForName(second, "pause")
	assertSequence(t, pause, "end:pause:SUCCEEDED:true")
	assertIdentity(t, pause[0], "1-1", "pause", string(OperationTypeWait), OperationSubTypeWait, wantParent)
	call := eventsForName(second, "call")
	assertSequence(t, call, "start:call:STARTED:false")
	assertIdentity(t, call[0], "1-2", "call", string(OperationTypeChainedInvoke), OperationSubTypeChainedInvoke, wantParent)
}

// TestOperationLifecycleStepEventsPinned pins the events runStep emits so
// the shared helper does not change them: a live step emits a STARTED
// start and a SUCCEEDED or FAILED end with IsReplay false; a replayed
// terminal step emits a start and an end at its checkpointed status with
// IsReplay true, the attempt count, and the checkpointed timestamps.
func TestOperationLifecycleStepEventsPinned(t *testing.T) {
	stepErr := errors.New("boom")
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "ok", func(StepContext) (string, error) { return "v", nil }); err != nil {
			return "", err
		}
		_, err := Step(ctx, "bad", func(StepContext) (string, error) { return "", stepErr }, WithRetry(NoRetry()))
		return "", err
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	// Live.
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationFailed)
	live := rec.take()
	assertSequence(t, live,
		"start:ok:STARTED:false",
		"end:ok:SUCCEEDED:false",
		"start:bad:STARTED:false",
		"end:bad:FAILED:false",
	)
	for _, ev := range live {
		assertIdentity(t, ev, ev.info.ID, ev.info.Name, string(OperationTypeStep), OperationSubTypeStep, "")
		if ev.info.Attempt != 1 {
			t.Errorf("live %s %s Attempt = %d, want 1", ev.hook, ev.info.Name, ev.info.Attempt)
		}
		if ev.info.StartTimestamp.IsZero() {
			t.Errorf("live %s %s StartTimestamp is zero", ev.hook, ev.info.Name)
		}
		if ev.hook == "end" && ev.info.EndTimestamp.IsZero() {
			t.Errorf("live end %s EndTimestamp is zero", ev.info.Name)
		}
		if ev.hook == "start" && !ev.info.EndTimestamp.IsZero() {
			t.Errorf("live start %s EndTimestamp must be zero", ev.info.Name)
		}
	}
	if live[3].info.Error == nil {
		t.Error("live failed end: Error is nil")
	}
	if live[1].info.Error != nil {
		t.Errorf("live succeeded end: Error = %v, want nil", live[1].info.Error)
	}

	// Replay of both terminal steps.
	ops := []wireOperation{
		lifecycleExecOp(),
		{
			Id: hashID("1"), Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "ok",
			StepDetails:    &wireStepDetails{Attempt: 1, Result: `"v"`},
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
		},
		{
			Id: hashID("2"), Status: "FAILED", Type: "STEP", SubType: "Step", Name: "bad",
			StepDetails:    &wireStepDetails{Attempt: 2, Error: &wireStepError{ErrorType: "Error", ErrorMessage: "boom"}},
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
		},
	}
	resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok2", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationFailed)
	replay := rec.take()
	assertSequence(t, replay,
		"start:ok:SUCCEEDED:true",
		"end:ok:SUCCEEDED:true",
		"start:bad:FAILED:true",
		"end:bad:FAILED:true",
	)
	for _, ev := range replay {
		if !ev.info.StartTimestamp.Equal(lifecycleStart) {
			t.Errorf("replayed %s %s StartTimestamp = %v, want %v", ev.hook, ev.info.Name, ev.info.StartTimestamp, lifecycleStart)
		}
		if ev.hook == "end" && !ev.info.EndTimestamp.Equal(lifecycleEnd) {
			t.Errorf("replayed end %s EndTimestamp = %v, want %v", ev.info.Name, ev.info.EndTimestamp, lifecycleEnd)
		}
		if ev.hook == "start" && !ev.info.EndTimestamp.IsZero() {
			t.Errorf("replayed start %s EndTimestamp must be zero", ev.info.Name)
		}
	}
	for _, ev := range eventsForName(replay, "ok") {
		if ev.info.Attempt != 1 || ev.info.Result != `"v"` || ev.info.Error != nil {
			t.Errorf("replayed %s ok: Attempt=%d Result=%q Error=%v", ev.hook, ev.info.Attempt, ev.info.Result, ev.info.Error)
		}
	}
	for _, ev := range eventsForName(replay, "bad") {
		if ev.info.Attempt != 2 || ev.info.Result != "" || ev.info.Error == nil {
			t.Errorf("replayed %s bad: Attempt=%d Result=%q Error=%v", ev.hook, ev.info.Attempt, ev.info.Result, ev.info.Error)
		}
	}
}

// TestOperationLifecycleAtMostOnePerInvocation asserts that a handler
// running a wait, an invoke, and a step in one invocation dispatches at
// most one start and at most one end per operation, live and on replay.
func TestOperationLifecycleAtMostOnePerInvocation(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		w := WaitAsync(ctx, "w", 5*time.Second)
		i := InvokeAsync[string](ctx, "i", "target-fn", "in")
		s := StepAsync(ctx, "s", func(StepContext) (string, error) { return "v", nil })
		if _, err := s.Result(); err != nil {
			return "", err
		}
		if _, err := w.Result(); err != nil {
			return "", err
		}
		return i.Result()
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	count := func(evs []opEvent) map[string]int {
		m := map[string]int{}
		for _, ev := range evs {
			m[ev.hook+":"+ev.info.Name]++
		}
		return m
	}
	assertAtMostOne := func(t *testing.T, evs []opEvent) {
		t.Helper()
		for k, n := range count(evs) {
			if n > 1 {
				t.Errorf("%s dispatched %d times, want at most 1; events %v", k, n, summarize(evs))
			}
		}
	}

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	first := rec.take()
	assertAtMostOne(t, first)
	c := count(first)
	if c["start:w"] != 1 || c["start:i"] != 1 || c["start:s"] != 1 || c["end:s"] != 1 {
		t.Errorf("live counts = %v", c)
	}
	if c["end:w"] != 0 || c["end:i"] != 0 {
		t.Errorf("suspended operations must not end: %v", c)
	}

	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: hashID("1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "w"},
		{Id: hashID("2"), Status: "SUCCEEDED", Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", Name: "i", ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"out"`}},
		{Id: hashID("3"), Status: "SUCCEEDED", Type: "STEP", SubType: "Step", Name: "s", StepDetails: &wireStepDetails{Attempt: 1, Result: `"v"`}},
	}
	resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok2", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	second := rec.take()
	assertAtMostOne(t, second)
	c = count(second)
	for _, k := range []string{"end:w", "end:i", "start:s", "end:s"} {
		if c[k] != 1 {
			t.Errorf("replay: %s dispatched %d times, want 1", k, c[k])
		}
	}
	for _, k := range []string{"start:w", "start:i"} {
		if c[k] != 0 {
			t.Errorf("replay: %s dispatched %d times, want 0: a settled wait or invoke replays only its end", k, c[k])
		}
	}
	for _, ev := range second {
		if !ev.info.IsReplay {
			t.Errorf("replay: %s %s IsReplay = false", ev.hook, ev.info.Name)
		}
	}
}

// TestDispatchOperationHelpersRecoverPanics asserts that the shared
// dispatch helpers recover a panicking hook, skip nil hooks, and are no-ops
// on a context with no plugins.
func TestDispatchOperationHelpersRecoverPanics(t *testing.T) {
	var starts, ends atomic.Int32
	plugins := []Plugin{
		{
			OnOperationStart: func(context.Context, OperationHookInfo) { panic("start") },
			OnOperationEnd:   func(context.Context, OperationHookInfo) { panic("end") },
		},
		{
			OnOperationStart: func(context.Context, OperationHookInfo) { starts.Add(1) },
			OnOperationEnd:   func(context.Context, OperationHookInfo) { ends.Add(1) },
		},
		{}, // no hooks
	}
	ec := &execContext{Context: context.Background(), executionArn: "arn", pluginDispatcher: newPluginDispatcher(plugins)}
	info := ec.operationHookInfo("1", "n", string(OperationTypeWait), OperationSubTypeWait, false)
	dispatchOperationStart(ec, info, PluginOperationStarted)
	dispatchOperationEnd(ec, info, PluginOperationSucceeded)
	if starts.Load() != 1 || ends.Load() != 1 {
		t.Fatalf("starts=%d ends=%d, want 1 and 1", starts.Load(), ends.Load())
	}

	none := &execContext{Context: context.Background()}
	dispatchOperationStart(none, none.operationHookInfo("1", "n", "WAIT", "Wait", false), PluginOperationStarted)
	dispatchOperationEnd(none, none.operationHookInfo("1", "n", "WAIT", "Wait", false), PluginOperationSucceeded)
}

// TestDispatchOperationHelpersSetStatus asserts the helpers set Status from
// their argument and leave the other fields as the caller supplied them.
func TestDispatchOperationHelpersSetStatus(t *testing.T) {
	var got []OperationHookInfo
	var mu sync.Mutex
	p := Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			got = append(got, info)
			mu.Unlock()
		},
		OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			got = append(got, info)
			mu.Unlock()
		},
	}
	ec := &execContext{Context: context.Background(), executionArn: "arn:x", checkpointParent: "7", pluginDispatcher: newPluginDispatcher([]Plugin{p})}
	info := ec.operationHookInfo("7-1", "n", string(OperationTypeChainedInvoke), OperationSubTypeChainedInvoke, true)
	info.StartTimestamp = lifecycleStart
	dispatchOperationStart(ec, info, PluginOperationPending)
	info.EndTimestamp = lifecycleEnd
	info.Result = "r"
	dispatchOperationEnd(ec, info, PluginOperationSucceeded)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("got %d hooks, want 2", len(got))
	}
	s, e := got[0], got[1]
	if s.Status != PluginOperationPending || e.Status != PluginOperationSucceeded {
		t.Errorf("statuses = %q, %q", s.Status, e.Status)
	}
	for _, i := range got {
		if i.ExecutionArn != "arn:x" || i.ID != "7-1" || i.Name != "n" || i.Type != string(OperationTypeChainedInvoke) || i.SubType != OperationSubTypeChainedInvoke || !i.IsReplay || i.ParentID != hashID("7") {
			t.Errorf("identity fields not preserved: %+v", i)
		}
		if !i.StartTimestamp.Equal(lifecycleStart) {
			t.Errorf("StartTimestamp = %v", i.StartTimestamp)
		}
	}
	if !s.EndTimestamp.IsZero() || s.Result != "" {
		t.Errorf("start carried end fields: %+v", s)
	}
	if !e.EndTimestamp.Equal(lifecycleEnd) || e.Result != "r" {
		t.Errorf("end fields = %v %q", e.EndTimestamp, e.Result)
	}
}

// TestDispatchOperationHelpersConcurrent asserts the helpers are safe to
// call from many goroutines at once, as concurrent branches do.
func TestDispatchOperationHelpersConcurrent(t *testing.T) {
	var n atomic.Int32
	p := Plugin{
		OnOperationStart: func(context.Context, OperationHookInfo) { n.Add(1) },
		OnOperationEnd:   func(context.Context, OperationHookInfo) { n.Add(1) },
	}
	ec := &execContext{Context: context.Background(), pluginDispatcher: newPluginDispatcher([]Plugin{p, p})}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			info := ec.operationHookInfo(fmt.Sprint(i), "", "WAIT", "Wait", false)
			dispatchOperationStart(ec, info, PluginOperationStarted)
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)
		}(i)
	}
	wg.Wait()
	if n.Load() != 200 {
		t.Fatalf("hooks called %d times, want 200", n.Load())
	}
}

package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wfcRecorder records the operation-level and attempt-level hook calls of
// a wait-for-condition operation in dispatch order. Operation events are
// rendered as "start"/"end", attempt events as "astart"/"aend".
type wfcRecorder struct {
	mu     sync.Mutex
	events []string
	ops    []opEvent
	aends  []AttemptEndHookInfo
}

func (r *wfcRecorder) record(s string) {
	r.mu.Lock()
	r.events = append(r.events, s)
	r.mu.Unlock()
}

func (r *wfcRecorder) plugin() Plugin {
	return Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			r.mu.Lock()
			r.ops = append(r.ops, opEvent{hook: "start", info: info})
			r.mu.Unlock()
			r.record(fmt.Sprintf("start:%s:%d:%v", info.Status, info.Attempt, info.IsReplay))
		},
		OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			r.mu.Lock()
			r.ops = append(r.ops, opEvent{hook: "end", info: info})
			r.mu.Unlock()
			r.record(fmt.Sprintf("end:%s:%d:%v", info.Status, info.Attempt, info.IsReplay))
		},
		OnOperationAttemptStart: func(_ context.Context, info AttemptHookInfo) {
			r.record(fmt.Sprintf("astart:%d:%v", info.Attempt, info.IsReplay))
		},
		OnOperationAttemptEnd: func(_ context.Context, info AttemptEndHookInfo) {
			r.mu.Lock()
			r.aends = append(r.aends, info)
			r.mu.Unlock()
			r.record(fmt.Sprintf("aend:%d:%s", info.Attempt, info.Outcome))
		},
	}
}

// take returns the recorded event strings and operation events and clears
// the recorder.
func (r *wfcRecorder) take() (events []string, ops []opEvent, aends []AttemptEndHookInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	events, ops, aends = r.events, r.ops, r.aends
	r.events, r.ops, r.aends = nil, nil, nil
	return events, ops, aends
}

func assertStrings(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// pollingHandler returns a handler running a wait-for-condition named
// "poll" that increments its state each attempt and stops at 3, with the
// plugin p registered.
func pollingHandler(p Plugin) func(context.Context, []byte) ([]byte, error) {
	return Wrap(func(ctx Context, _ string) (int, error) {
		return WaitForCondition(ctx, "poll", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 3 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
	}, WithPlugins(p), withLambdaAPI(&fakePluginClient{}))
}

// wfcOp builds the checkpoint of the polling operation "poll" at the root.
func wfcOp(status string, details *wireStepDetails) wireOperation {
	op := wireOperation{
		Id: hashID("1"), Status: status, Type: "STEP", SubType: OperationSubTypeWaitForCondition, Name: "poll",
		StepDetails:    details,
		StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
	}
	if status == "SUCCEEDED" || status == "FAILED" {
		op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}
	}
	return op
}

// TestOperationLifecycleWaitForConditionMultiInvocation asserts the event
// sequence of a poll that spans three invocations, then is replayed.
//
// Invocation 1 runs attempt 1: the operation start is dispatched once,
// before the attempt hooks, then the invocation suspends with no end.
// Invocations 2 and 3 run attempts 2 and 3 with no operation start;
// attempt 3 satisfies the condition and the end reports SUCCEEDED with the
// final attempt count and state. Invocation 4 replays the terminal
// checkpoint: one replayed end with the checkpointed attempt count and
// timestamps, no attempt hooks. The attempt-level hooks fire once per
// attempt, in every invocation, as before: their IsReplay reports whether
// the context was replaying when the attempt ran, true from the second
// invocation on, unchanged by the operation-level events.
func TestOperationLifecycleWaitForConditionMultiInvocation(t *testing.T) {
	rec := &wfcRecorder{}
	handler := pollingHandler(rec.plugin())
	arn := "arn:test:lifecycle"

	invoke := func(token string, ops []wireOperation, wantStatus string) ([]string, []opEvent, []AttemptEndHookInfo) {
		t.Helper()
		resp, err := handler(makePluginContext(), makePluginPayload(t, arn, token, ops))
		if err != nil {
			t.Fatal(err)
		}
		assertPluginResponseStatus(t, resp, wantStatus)
		return rec.take()
	}

	events, ops, _ := invoke("tok1", nil, invocationPending)
	assertStrings(t, events, "start:STARTED:1:false", "astart:1:false", "aend:1:SUCCEEDED")
	assertIdentity(t, ops[0], "1", "poll", string(OperationTypeStep), OperationSubTypeWaitForCondition, "")
	assertLiveTimestamps(t, ops[0])

	state := func(attempt int, result string) []wireOperation {
		return []wireOperation{lifecycleExecOp(), wfcOp("STARTED", &wireStepDetails{Attempt: attempt, Result: result})}
	}
	events, ops, _ = invoke("tok2", state(1, "1"), invocationPending)
	assertStrings(t, events, "astart:2:true", "aend:2:SUCCEEDED")
	if len(ops) != 0 {
		t.Fatalf("invocation 2 dispatched operation events %v, want none", summarize(ops))
	}

	events, ops, aends := invoke("tok3", state(2, "2"), invocationSucceeded)
	assertStrings(t, events, "astart:3:true", "aend:3:SUCCEEDED", "end:SUCCEEDED:3:false")
	end := ops[0]
	assertIdentity(t, end, "1", "poll", string(OperationTypeStep), OperationSubTypeWaitForCondition, "")
	if end.info.Result != "3" || end.info.Error != nil {
		t.Errorf("end Result = %q Error = %v, want %q and nil", end.info.Result, end.info.Error, "3")
	}
	if !end.info.StartTimestamp.Equal(lifecycleStart) {
		t.Errorf("end StartTimestamp = %v, want the checkpointed %v", end.info.StartTimestamp, lifecycleStart)
	}
	if end.info.EndTimestamp.IsZero() {
		t.Error("end EndTimestamp is zero")
	}
	if len(aends) != 1 || aends[0].Attempt != 3 || aends[0].Outcome != PluginAttemptSucceeded || aends[0].Error != nil {
		t.Errorf("attempt end = %+v, want attempt 3 SUCCEEDED", aends)
	}

	events, ops, _ = invoke("tok4", []wireOperation{lifecycleExecOp(), wfcOp("SUCCEEDED", &wireStepDetails{Attempt: 3, Result: "3"})}, invocationSucceeded)
	assertStrings(t, events, "end:SUCCEEDED:3:true")
	assertIdentity(t, ops[0], "1", "poll", string(OperationTypeStep), OperationSubTypeWaitForCondition, "")
	assertReplayedTimestamps(t, ops[0], true)
	if ops[0].info.Result != "3" {
		t.Errorf("replayed end Result = %q, want %q", ops[0].info.Result, "3")
	}
}

// TestOperationLifecycleWaitForConditionReplayedPending asserts an
// invocation that finds the poll PENDING on its retry timer dispatches
// nothing and suspends: the operation began earlier and has no outcome.
func TestOperationLifecycleWaitForConditionReplayedPending(t *testing.T) {
	rec := &wfcRecorder{}
	handler := pollingHandler(rec.plugin())
	ops := []wireOperation{lifecycleExecOp(), wfcOp("PENDING", &wireStepDetails{Attempt: 1, Result: "1"})}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	events, _, _ := rec.take()
	if len(events) != 0 {
		t.Fatalf("events = %v, want none", events)
	}
}

// TestOperationLifecycleWaitForConditionReenteredStart asserts that an
// invocation which finds the START checkpointed with no completed attempt
// runs attempt 1 and dispatches a replayed start with the checkpointed
// status: the operation is still at its first attempt.
func TestOperationLifecycleWaitForConditionReenteredStart(t *testing.T) {
	rec := &wfcRecorder{}
	handler := pollingHandler(rec.plugin())
	ops := []wireOperation{lifecycleExecOp(), wfcOp("STARTED", nil)}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	events, opEvents, _ := rec.take()
	assertStrings(t, events, "start:STARTED:1:true", "astart:1:true", "aend:1:SUCCEEDED")
	assertReplayedTimestamps(t, opEvents[0], false)
}

// TestOperationLifecycleWaitForConditionFailed asserts the end of a poll
// that fails: live, when the check returns an error or the strategy stops
// with one, the end reports FAILED with the returned WaitForConditionError
// and the attempt count; replayed from the FAILED checkpoint, the end
// carries the checkpointed attempt count and a WaitForConditionError.
func TestOperationLifecycleWaitForConditionFailed(t *testing.T) {
	errCheck := errors.New("check failed")
	errStrategy := errors.New("gave up")
	for _, tc := range []struct {
		name  string
		check func(StepContext, int) (int, error)
		stop  func(int, int) WaitDecision
		want  error
		aend  PluginAttemptOutcome
	}{
		{
			name:  "check error",
			check: func(StepContext, int) (int, error) { return 0, errCheck },
			stop:  func(int, int) WaitDecision { return WaitDecision{Continue: true, Delay: time.Second} },
			want:  errCheck,
			aend:  PluginAttemptFailed,
		},
		{
			name:  "strategy error",
			check: func(_ StepContext, s int) (int, error) { return s + 1, nil },
			stop:  func(int, int) WaitDecision { return WaitDecision{Continue: false, Err: errStrategy} },
			want:  errStrategy,
			aend:  PluginAttemptSucceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &wfcRecorder{}
			handler := Wrap(func(ctx Context, _ string) (int, error) {
				return WaitForCondition(ctx, "poll", tc.check, ConditionConfig[int]{WaitStrategy: tc.stop})
			}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationFailed)
			events, ops, _ := rec.take()
			assertStrings(t, events, "start:STARTED:1:false", "astart:1:false", "aend:1:"+string(tc.aend), "end:FAILED:1:false")
			end := ops[1]
			var wfcErr *WaitForConditionError
			if !errors.As(end.info.Error, &wfcErr) || wfcErr.Attempts != 1 || !strings.Contains(end.info.Error.Error(), tc.want.Error()) {
				t.Errorf("end Error = %v, want a WaitForConditionError for attempt 1 wrapping %v", end.info.Error, tc.want)
			}
			if end.info.Result != "" {
				t.Errorf("end Result = %q, want empty", end.info.Result)
			}
			assertLiveTimestamps(t, end)
		})
	}

	t.Run("replayed", func(t *testing.T) {
		rec := &wfcRecorder{}
		handler := pollingHandler(rec.plugin())
		op := wfcOp("FAILED", &wireStepDetails{Attempt: 2, Error: &wireStepError{ErrorType: "boom", ErrorMessage: "check failed"}})
		resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", []wireOperation{lifecycleExecOp(), op}))
		if err != nil {
			t.Fatal(err)
		}
		assertPluginResponseStatus(t, resp, invocationFailed)
		events, ops, _ := rec.take()
		assertStrings(t, events, "end:FAILED:2:true")
		assertReplayedTimestamps(t, ops[0], true)
		var wfcErr *WaitForConditionError
		if !errors.As(ops[0].info.Error, &wfcErr) || wfcErr.Attempts != 2 || wfcErr.ErrorType != "boom" {
			t.Errorf("replayed end Error = %v, want a WaitForConditionError for attempt 2 of type boom", ops[0].info.Error)
		}
	})
}

// TestOperationLifecycleWaitForConditionCheckpointTerminatedNoEnd asserts
// that a checkpoint refused because the invocation has terminated produces
// no end event, at every checkpoint of a poll attempt: the START, the FAIL
// after a check error, the FAIL after a strategy error, the terminal
// SUCCEED, and the RETRY. The operation has no outcome; the invocation
// responds PENDING and the next one replays it. The hooks dispatched
// before the refused checkpoint are unchanged.
func TestOperationLifecycleWaitForConditionCheckpointTerminatedNoEnd(t *testing.T) {
	errCheck := errors.New("check failed")
	continueForever := func(int, int) WaitDecision { return WaitDecision{Continue: true, Delay: time.Second} }
	stopNow := func(int, int) WaitDecision { return WaitDecision{Continue: false} }
	increment := func(_ StepContext, s int) (int, error) { return s + 1, nil }
	for _, tc := range []struct {
		name string
		// okCalls is the number of checkpoint calls that succeed before
		// the service responds without a token.
		okCalls int32
		check   func(StepContext, int) (int, error)
		stop    func(int, int) WaitDecision
		want    []string
	}{
		{
			name: "start", okCalls: 0, check: increment, stop: continueForever,
			want: []string{"start:STARTED:1:false"},
		},
		{
			name: "fail after check error", okCalls: 1,
			check: func(StepContext, int) (int, error) { return 0, errCheck }, stop: continueForever,
			want: []string{"start:STARTED:1:false", "astart:1:false", "aend:1:FAILED"},
		},
		{
			name: "fail after strategy error", okCalls: 1, check: increment,
			stop: func(int, int) WaitDecision { return WaitDecision{Continue: false, Err: errors.New("gave up")} },
			want: []string{"start:STARTED:1:false", "astart:1:false", "aend:1:SUCCEEDED"},
		},
		{
			name: "succeed", okCalls: 1, check: increment, stop: stopNow,
			want: []string{"start:STARTED:1:false", "astart:1:false", "aend:1:SUCCEEDED"},
		},
		{
			name: "retry", okCalls: 1, check: increment, stop: continueForever,
			want: []string{"start:STARTED:1:false", "astart:1:false", "aend:1:SUCCEEDED"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen atomic.Int32
			fake, calls := countingClient(func(in CheckpointInput) (CheckpointOutput, error) {
				if seen.Add(1) <= tc.okCalls {
					return CheckpointOutput{CheckpointToken: in.CheckpointToken}, nil
				}
				return CheckpointOutput{}, nil
			})
			rec := &wfcRecorder{}
			handler := Wrap(func(ctx Context, _ string) (int, error) {
				return WaitForCondition(ctx, "poll", tc.check, ConditionConfig[int]{WaitStrategy: tc.stop})
			}, WithPlugins(rec.plugin()), withLambdaAPI(fake))

			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
			if err != nil {
				t.Fatalf("Invoke error = %v, want PENDING response", err)
			}
			assertPluginResponseStatus(t, resp, invocationPending)
			if got := calls.Load(); got != tc.okCalls+1 {
				t.Errorf("checkpoint calls = %d, want %d", got, tc.okCalls+1)
			}
			events, ops, _ := rec.take()
			assertStrings(t, events, tc.want...)
			for _, op := range ops {
				if op.hook == "end" {
					t.Errorf("end event dispatched with status %s, want none", op.info.Status)
				}
			}
		})
	}
}

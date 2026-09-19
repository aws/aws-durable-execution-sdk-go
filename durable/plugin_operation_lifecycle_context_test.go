package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// summarizeByID renders events as "hook:id:status:replay" strings, for
// sequences that include unnamed operations.
func summarizeByID(evs []opEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, fmt.Sprintf("%s:%s:%s:%v", ev.hook, ev.info.ID, ev.info.Status, ev.info.IsReplay))
	}
	return out
}

func assertSequenceByID(t *testing.T, got []opEvent, want ...string) {
	t.Helper()
	g := summarizeByID(got)
	if len(g) != len(want) {
		t.Fatalf("event sequence = %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("event sequence = %v, want %v", g, want)
		}
	}
}

// errBox holds the error a handler observed, written from the handler's
// goroutine and read by the test after the invocation returned. A suspended
// invocation may return before its handler goroutine finishes, so the
// access is guarded.
type errBox struct {
	mu  sync.Mutex
	err error
}

func (b *errBox) set(err error) {
	b.mu.Lock()
	b.err = err
	b.mu.Unlock()
}

func (b *errBox) get() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// contextOp builds a checkpointed context operation with the lifecycle
// timestamps.
func contextOp(id, parent, subType, name, status string, details *wireContextDetails) wireOperation {
	op := wireOperation{
		Id: hashID(id), Status: status, Type: "CONTEXT", SubType: subType, Name: name,
		ContextDetails: details,
		StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
	}
	if parent != "" {
		op.ParentId = hashID(parent)
	}
	if status == "SUCCEEDED" || status == "FAILED" {
		op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}
	}
	return op
}

// assertReplayedTimestamps checks an event replayed from a checkpoint
// carries the checkpointed timestamps.
func assertReplayedTimestamps(t *testing.T, ev opEvent, wantEnd bool) {
	t.Helper()
	if !ev.info.StartTimestamp.Equal(lifecycleStart) {
		t.Errorf("%s %s StartTimestamp = %v, want %v", ev.hook, ev.info.ID, ev.info.StartTimestamp, lifecycleStart)
	}
	if wantEnd && !ev.info.EndTimestamp.Equal(lifecycleEnd) {
		t.Errorf("%s %s EndTimestamp = %v, want %v", ev.hook, ev.info.ID, ev.info.EndTimestamp, lifecycleEnd)
	}
	if !wantEnd && !ev.info.EndTimestamp.IsZero() {
		t.Errorf("%s %s EndTimestamp = %v, want zero", ev.hook, ev.info.ID, ev.info.EndTimestamp)
	}
}

// assertLiveTimestamps checks a live event carries a start timestamp and,
// for an end, an end timestamp no earlier than the start.
func assertLiveTimestamps(t *testing.T, ev opEvent) {
	t.Helper()
	if ev.info.StartTimestamp.IsZero() {
		t.Errorf("live %s %s StartTimestamp is zero", ev.hook, ev.info.ID)
	}
	switch ev.hook {
	case "start":
		if !ev.info.EndTimestamp.IsZero() {
			t.Errorf("live start %s EndTimestamp must be zero", ev.info.ID)
		}
	case "end":
		if ev.info.EndTimestamp.Before(ev.info.StartTimestamp) {
			t.Errorf("live end %s EndTimestamp %v before StartTimestamp %v", ev.info.ID, ev.info.EndTimestamp, ev.info.StartTimestamp)
		}
	}
}

// nestedChildHandlers returns the blocking and async three-level nested
// child context handlers under test. Each level is a child context; the
// innermost runs one step.
func nestedChildHandlers(rec *opRecorder) map[string]func(context.Context, []byte) ([]byte, error) {
	leaf := func(c Context) (string, error) {
		return Step(c, "leaf", func(StepContext) (string, error) { return "v", nil })
	}
	return map[string]func(context.Context, []byte) ([]byte, error){
		"RunInChildContext": Wrap(func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
				return RunInChildContext(c, "mid", func(c Context) (string, error) {
					return RunInChildContext(c, "inner", leaf)
				})
			})
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		"RunInChildContextAsync": Wrap(func(ctx Context, _ string) (string, error) {
			return RunInChildContextAsync(ctx, "outer", func(c Context) (string, error) {
				return RunInChildContextAsync(c, "mid", func(c Context) (string, error) {
					return RunInChildContextAsync(c, "inner", leaf).Result()
				}).Result()
			}).Result()
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
	}
}

// TestOperationLifecycleChildContextThreeLevelTree asserts the event tree
// for a three-level nesting of child contexts, live and on replay, for
// RunInChildContext and RunInChildContextAsync.
//
// Live: each level dispatches a STARTED start before its body, then the
// leaf step runs, then each level dispatches a SUCCEEDED end after its
// checkpoint, innermost first. Every event's ParentID is the wire ID of the
// enclosing context, so the tree is reconstructable from the events alone.
//
// Replay of the settled tree: the outer context is SUCCEEDED, so only its
// replayed end is dispatched; the inner levels are not visited.
//
// Replay of a partially settled tree: the outer and middle contexts are
// STARTED and the inner context is SUCCEEDED. The outer and middle
// contexts re-enter with replayed starts, the inner context dispatches a
// replayed end, and the outer and middle contexts complete live.
func TestOperationLifecycleChildContextThreeLevelTree(t *testing.T) {
	for variant := range nestedChildHandlers(&opRecorder{}) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			handler := nestedChildHandlers(rec)[variant]

			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			live := rec.take()
			assertSequence(t, live,
				"start:outer:STARTED:false",
				"start:mid:STARTED:false",
				"start:inner:STARTED:false",
				"start:leaf:STARTED:false",
				"end:leaf:SUCCEEDED:false",
				"end:inner:SUCCEEDED:false",
				"end:mid:SUCCEEDED:false",
				"end:outer:SUCCEEDED:false",
			)
			ctxType, ctxSub := string(OperationTypeContext), OperationSubTypeRunInChildContext
			for _, ev := range live {
				assertLiveTimestamps(t, ev)
				switch ev.info.Name {
				case "outer":
					assertIdentity(t, ev, "1", "outer", ctxType, ctxSub, "")
				case "mid":
					assertIdentity(t, ev, "1-1", "mid", ctxType, ctxSub, hashID("1"))
				case "inner":
					assertIdentity(t, ev, "1-1-1", "inner", ctxType, ctxSub, hashID("1-1"))
				case "leaf":
					assertIdentity(t, ev, "1-1-1-1", "leaf", string(OperationTypeStep), OperationSubTypeStep, hashID("1-1-1"))
				}
				if ev.hook == "end" && ev.info.Type == ctxType {
					if ev.info.Result != `"v"` {
						t.Errorf("end %s Result = %q, want %q", ev.info.Name, ev.info.Result, `"v"`)
					}
					if ev.info.Error != nil {
						t.Errorf("end %s Error = %v, want nil", ev.info.Name, ev.info.Error)
					}
				}
			}

			// Replay of the settled tree.
			ops := []wireOperation{
				lifecycleExecOp(),
				contextOp("1", "", ctxSub, "outer", "SUCCEEDED", &wireContextDetails{Result: `"v"`}),
				contextOp("1-1", "1", ctxSub, "mid", "SUCCEEDED", &wireContextDetails{Result: `"v"`}),
				contextOp("1-1-1", "1-1", ctxSub, "inner", "SUCCEEDED", &wireContextDetails{Result: `"v"`}),
			}
			resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok2", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			settled := rec.take()
			assertSequence(t, settled, "end:outer:SUCCEEDED:true")
			assertIdentity(t, settled[0], "1", "outer", ctxType, ctxSub, "")
			assertReplayedTimestamps(t, settled[0], true)
			if settled[0].info.Result != `"v"` {
				t.Errorf("replayed end Result = %q, want %q", settled[0].info.Result, `"v"`)
			}

			// Replay of a partially settled tree.
			ops = []wireOperation{
				lifecycleExecOp(),
				contextOp("1", "", ctxSub, "outer", "STARTED", nil),
				contextOp("1-1", "1", ctxSub, "mid", "STARTED", nil),
				contextOp("1-1-1", "1-1", ctxSub, "inner", "SUCCEEDED", &wireContextDetails{Result: `"v"`}),
			}
			resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok3", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			partial := rec.take()
			assertSequence(t, partial,
				"start:outer:STARTED:true",
				"start:mid:STARTED:true",
				"end:inner:SUCCEEDED:true",
				"end:mid:SUCCEEDED:false",
				"end:outer:SUCCEEDED:false",
			)
			assertIdentity(t, partial[0], "1", "outer", ctxType, ctxSub, "")
			assertIdentity(t, partial[1], "1-1", "mid", ctxType, ctxSub, hashID("1"))
			assertIdentity(t, partial[2], "1-1-1", "inner", ctxType, ctxSub, hashID("1-1"))
			assertIdentity(t, partial[3], "1-1", "mid", ctxType, ctxSub, hashID("1"))
			assertIdentity(t, partial[4], "1", "outer", ctxType, ctxSub, "")
			assertReplayedTimestamps(t, partial[0], false)
			assertReplayedTimestamps(t, partial[1], false)
			assertReplayedTimestamps(t, partial[2], true)
			for _, ev := range partial[3:] {
				// A re-entered context keeps its checkpointed start
				// and completes now.
				if !ev.info.StartTimestamp.Equal(lifecycleStart) {
					t.Errorf("live end %s StartTimestamp = %v, want %v", ev.info.Name, ev.info.StartTimestamp, lifecycleStart)
				}
				if ev.info.EndTimestamp.IsZero() {
					t.Errorf("live end %s EndTimestamp is zero", ev.info.Name)
				}
			}
		})
	}
}

// errChildBoom is the error the child context bodies under test fail with.
var errChildBoom = errors.New("child boom")

// failingChildHandlers returns the blocking and async handlers whose child
// context body fails with errChildBoom.
func failingChildHandlers(rec *opRecorder, got *errBox) map[string]func(context.Context, []byte) ([]byte, error) {
	body := func(Context) (string, error) { return "", errChildBoom }
	return map[string]func(context.Context, []byte) ([]byte, error){
		"RunInChildContext": Wrap(func(ctx Context, _ string) (string, error) {
			_, err := RunInChildContext(ctx, "child", body)
			got.set(err)
			return "", err
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		"RunInChildContextAsync": Wrap(func(ctx Context, _ string) (string, error) {
			_, err := RunInChildContextAsync(ctx, "child", body).Result()
			got.set(err)
			return "", err
		}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
	}
}

// TestOperationLifecycleChildContextFailure asserts that a failing child
// context dispatches a STARTED start and a FAILED end carrying the error
// the operation returns, live and on replay, for RunInChildContext and
// RunInChildContextAsync.
func TestOperationLifecycleChildContextFailure(t *testing.T) {
	for variant := range failingChildHandlers(&opRecorder{}, &errBox{}) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			got := &errBox{}
			handler := failingChildHandlers(rec, got)[variant]
			ctxType, ctxSub := string(OperationTypeContext), OperationSubTypeRunInChildContext

			assertFailedEnd := func(t *testing.T, ev opEvent) {
				t.Helper()
				assertIdentity(t, ev, "1", "child", ctxType, ctxSub, "")
				if ev.info.Error == nil {
					t.Fatal("failed end Error is nil")
				}
				if ev.info.Error != got.get() {
					t.Errorf("failed end Error = %v, want the error the operation returned %v", ev.info.Error, got.get())
				}
				var cce *ChildContextError
				if !errors.As(ev.info.Error, &cce) || cce.Message != errChildBoom.Error() {
					t.Errorf("failed end Error = %#v, want *ChildContextError with message %q", ev.info.Error, errChildBoom.Error())
				}
				if ev.info.Result != "" {
					t.Errorf("failed end Result = %q, want empty", ev.info.Result)
				}
			}

			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationFailed)
			live := rec.take()
			assertSequence(t, live, "start:child:STARTED:false", "end:child:FAILED:false")
			assertIdentity(t, live[0], "1", "child", ctxType, ctxSub, "")
			assertLiveTimestamps(t, live[0])
			assertLiveTimestamps(t, live[1])
			assertFailedEnd(t, live[1])

			ops := []wireOperation{
				lifecycleExecOp(),
				contextOp("1", "", ctxSub, "child", "FAILED", &wireContextDetails{
					Error: &wireFullError{ErrorType: "Error", ErrorMessage: errChildBoom.Error()},
				}),
			}
			resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok2", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationFailed)
			replay := rec.take()
			assertSequence(t, replay, "end:child:FAILED:true")
			assertReplayedTimestamps(t, replay[0], true)
			assertFailedEnd(t, replay[0])
		})
	}
}

// TestOperationLifecycleChildContextStartBeforeWrap asserts that the start
// event of a child context is dispatched before WrapChildContextFn runs and
// that the wrap hook receives the same info, so a plugin keyed on the start
// event can correlate the two.
func TestOperationLifecycleChildContextStartBeforeWrap(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var startInfo, wrapInfo OperationHookInfo
	plugin := Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			defer mu.Unlock()
			if info.Type == string(OperationTypeContext) {
				order = append(order, "start")
				startInfo = info
			}
		},
		WrapChildContextFn: func(ctx context.Context, info OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			mu.Lock()
			order = append(order, "wrap")
			wrapInfo = info
			mu.Unlock()
			return fn(ctx)
		},
	}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "child", func(c Context) (string, error) {
			return Step(c, "s", func(StepContext) (string, error) { return "v", nil })
		})
	}, WithPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "start" || order[1] != "wrap" {
		t.Fatalf("order = %v, want [start wrap]", order)
	}
	if wrapInfo != startInfo {
		t.Errorf("wrap info = %+v, want the start info %+v", wrapInfo, startInfo)
	}
	if startInfo.ID != "1" || startInfo.Status != PluginOperationStarted || startInfo.IsReplay || startInfo.StartTimestamp.IsZero() {
		t.Errorf("start info = %+v", startInfo)
	}
}

// callbackLifecycleHandler returns a handler that creates a named callback
// and blocks on its result.
func callbackLifecycleHandler(rec *opRecorder, got *errBox) func(context.Context, []byte) ([]byte, error) {
	return Wrap(func(ctx Context, _ string) (string, error) {
		cb, err := CreateCallback[string](ctx, "cb")
		if err != nil {
			return "", err
		}
		out, err := cb.Result()
		got.set(err)
		return out, err
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
}

// TestOperationLifecycleCreateCallbackSuspendResume asserts the event
// sequence for a callback that settles on a later invocation than the one
// that created it.
//
// Invocation 1 creates the callback live: one STARTED start, no end, then
// the invocation suspends on Result. Invocation 2 observes the settled
// callback: no start and one end with the checkpointed timestamps and
// outcome. The end is live (IsReplay false) when the invocation payload
// lists the callback among the operations updated since the previous
// invocation, because this invocation is the first to observe the outcome;
// it is replayed (IsReplay true) otherwise. A succeeded, an externally
// failed, and a timed-out outcome are covered in both cases.
func TestOperationLifecycleCreateCallbackSuspendResume(t *testing.T) {
	failure := &wireFullError{ErrorType: "Error", ErrorMessage: "external system said no"}
	outcomes := []struct {
		name       string
		details    *wireCallbackDetails
		status     string
		wantStatus PluginOperationStatus
		wantFinal  string
		wantErr    any
	}{
		{
			name:       "succeeded",
			details:    &wireCallbackDetails{CallbackId: "cb-1", Result: `"ok"`},
			status:     "SUCCEEDED",
			wantStatus: PluginOperationSucceeded,
			wantFinal:  invocationSucceeded,
		},
		{
			name:       "failed",
			details:    &wireCallbackDetails{CallbackId: "cb-1", Error: failure},
			status:     "FAILED",
			wantStatus: PluginOperationFailed,
			wantFinal:  invocationFailed,
			wantErr:    new(*CallbackExternalError),
		},
		{
			name:       "timed out",
			details:    &wireCallbackDetails{CallbackId: "cb-1", Error: failure},
			status:     "TIMED_OUT",
			wantStatus: PluginOperationTimedOut,
			wantFinal:  invocationFailed,
			wantErr:    new(*CallbackTimeoutError),
		},
	}
	settled := []struct {
		name       string
		updatedIDs []string
		wantReplay bool
	}{
		{name: "replayed", updatedIDs: nil, wantReplay: true},
		{name: "updated between invocations", updatedIDs: []string{hashID("1")}, wantReplay: false},
	}
	for _, s := range settled {
		for _, tc := range outcomes {
			t.Run(s.name+"/"+tc.name, func(t *testing.T) {
				rec := &opRecorder{}
				got := &errBox{}
				handler := callbackLifecycleHandler(rec, got)
				op := wireOperation{
					Id: hashID("1"), Status: tc.status, Type: "CALLBACK", SubType: "Callback", Name: "cb",
					CallbackDetails: tc.details,
					StartTimestamp:  flexTimestamp{Time: lifecycleStart, Valid: true},
					EndTimestamp:    flexTimestamp{Time: lifecycleEnd, Valid: true},
				}
				first, second := runSuspendResumeUpdated(t, handler, rec, []wireOperation{op}, s.updatedIDs, tc.wantFinal)

				assertSequence(t, first, "start:cb:STARTED:false")
				assertIdentity(t, first[0], "1", "cb", string(OperationTypeCallback), OperationSubTypeCallback, "")
				assertLiveTimestamps(t, first[0])

				assertSequence(t, second, fmt.Sprintf("end:cb:%s:%v", tc.wantStatus, s.wantReplay))
				ev := second[0]
				assertIdentity(t, ev, "1", "cb", string(OperationTypeCallback), OperationSubTypeCallback, "")
				assertReplayedTimestamps(t, ev, true)
				if tc.wantErr == nil {
					if ev.info.Result != `"ok"` || ev.info.Error != nil {
						t.Errorf("end Result = %q Error = %v, want %q and nil", ev.info.Result, ev.info.Error, `"ok"`)
					}
					return
				}
				if ev.info.Result != "" {
					t.Errorf("end Result = %q, want empty", ev.info.Result)
				}
				if ev.info.Error == nil || ev.info.Error != got.get() {
					t.Fatalf("end Error = %v, want the error Result returned %v", ev.info.Error, got.get())
				}
				if !errors.As(ev.info.Error, tc.wantErr) {
					t.Errorf("end Error = %#v, want %T", ev.info.Error, tc.wantErr)
				}
			})
		}
	}
}

// TestOperationLifecycleCreateCallbackReplayedPendingSuspends asserts that
// a callback replayed while still in flight dispatches one replayed start
// with its checkpointed status and no end. The start is replayed even when
// the payload lists the callback as updated: an in-flight callback has no
// outcome to report live.
func TestOperationLifecycleCreateCallbackReplayedPendingSuspends(t *testing.T) {
	for _, status := range []string{"STARTED", "PENDING"} {
		t.Run(status, func(t *testing.T) {
			rec := &opRecorder{}
			handler := callbackLifecycleHandler(rec, &errBox{})
			ops := []wireOperation{lifecycleExecOp(), {
				Id: hashID("1"), Status: status, Type: "CALLBACK", SubType: "Callback", Name: "cb",
				CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1"},
				StartTimestamp:  flexTimestamp{Time: lifecycleStart, Valid: true},
			}}
			resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1", ops, []string{hashID("1")}))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationPending)
			evs := rec.take()
			assertSequence(t, evs, "start:cb:"+status+":true")
			assertReplayedTimestamps(t, evs[0], false)
		})
	}
}

// waitForCallbackHandler returns a handler that runs one WaitForCallback
// named "wfcb" whose submitter returns submitErr.
func waitForCallbackHandler(rec *opRecorder, submitErr error, got *errBox) func(context.Context, []byte) ([]byte, error) {
	return Wrap(func(ctx Context, _ string) (string, error) {
		out, err := WaitForCallback[string](ctx, "wfcb", func(StepContext, string) error {
			return submitErr
		}, WithSubmitterRetry(NoRetry()))
		got.set(err)
		return out, err
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
}

// assertWaitForCallbackHierarchy checks the identity of every event of a
// WaitForCallback tree: the context is "1" at the root, its callback is
// "1-1" and its submitter step is "1-2", both children of the context.
func assertWaitForCallbackHierarchy(t *testing.T, evs []opEvent) {
	t.Helper()
	for _, ev := range evs {
		switch ev.info.ID {
		case "1":
			assertIdentity(t, ev, "1", "wfcb", string(OperationTypeContext), OperationSubTypeWaitForCallback, "")
		case "1-1":
			assertIdentity(t, ev, "1-1", "", string(OperationTypeCallback), OperationSubTypeCallback, hashID("1"))
		case "1-2":
			assertIdentity(t, ev, "1-2", "", string(OperationTypeStep), OperationSubTypeStep, hashID("1"))
		default:
			t.Errorf("unexpected event %s for operation %q", ev.hook, ev.info.ID)
		}
	}
}

// wfcbFirstInvocation drives the live first invocation of a WaitForCallback
// that suspends on its callback and asserts its events: the context, the
// callback, and the submitter step start in order, the step ends, and the
// context and callback dispatch no end.
func wfcbFirstInvocation(t *testing.T, handler func(context.Context, []byte) ([]byte, error), rec *opRecorder) {
	t.Helper()
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	first := rec.take()
	assertSequenceByID(t, first,
		"start:1:STARTED:false",
		"start:1-1:STARTED:false",
		"start:1-2:STARTED:false",
		"end:1-2:SUCCEEDED:false",
	)
	assertWaitForCallbackHierarchy(t, first)
	for _, ev := range first {
		assertLiveTimestamps(t, ev)
	}
}

// wfcbResumeOps returns the state of a WaitForCallback whose context is
// still STARTED, whose submitter step succeeded, and whose callback settled
// with status and details.
func wfcbResumeOps(status string, details *wireCallbackDetails) []wireOperation {
	callback := wireOperation{
		Id: hashID("1-1"), ParentId: hashID("1"), Status: status, Type: "CALLBACK", SubType: "Callback",
		CallbackDetails: details,
		StartTimestamp:  flexTimestamp{Time: lifecycleStart, Valid: true},
		EndTimestamp:    flexTimestamp{Time: lifecycleEnd, Valid: true},
	}
	step := wireOperation{
		Id: hashID("1-2"), ParentId: hashID("1"), Status: "SUCCEEDED", Type: "STEP", SubType: "Step",
		StepDetails:    &wireStepDetails{Attempt: 1, Result: `{}`},
		StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
		EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true},
	}
	return []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", OperationSubTypeWaitForCallback, "wfcb", "STARTED", nil),
		callback,
		step,
	}
}

// TestOperationLifecycleWaitForCallbackSucceeded asserts the event sequence
// of a WaitForCallback whose callback succeeds on a later invocation.
//
// Invocation 1 is live; see wfcbFirstInvocation. Invocation 2 lists the
// callback as updated since invocation 1. It re-enters the STARTED context
// with a replayed start, dispatches the callback's end live (IsReplay
// false, since this invocation is the first to observe the outcome),
// replays the settled step as a start and an end, then completes the
// context live with a SUCCEEDED end carrying the result. Invocation 3
// replays the SUCCEEDED context: only its replayed end is dispatched.
func TestOperationLifecycleWaitForCallbackSucceeded(t *testing.T) {
	rec := &opRecorder{}
	got := &errBox{}
	handler := waitForCallbackHandler(rec, nil, got)
	wfcbFirstInvocation(t, handler, rec)

	ops := wfcbResumeOps("SUCCEEDED", &wireCallbackDetails{CallbackId: "cb-1", Result: `"ok"`})
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok2", ops, []string{hashID("1-1")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	second := rec.take()
	assertSequenceByID(t, second,
		"start:1:STARTED:true",
		"end:1-1:SUCCEEDED:false",
		"start:1-2:SUCCEEDED:true",
		"end:1-2:SUCCEEDED:true",
		"end:1:SUCCEEDED:false",
	)
	assertWaitForCallbackHierarchy(t, second)
	assertReplayedTimestamps(t, second[0], false)
	assertReplayedTimestamps(t, second[1], true)
	if second[1].info.Result != `"ok"` {
		t.Errorf("callback end Result = %q, want %q", second[1].info.Result, `"ok"`)
	}
	ctxEnd := second[4]
	if ctxEnd.info.Result != `"ok"` || ctxEnd.info.Error != nil {
		t.Errorf("context end Result = %q Error = %v, want %q and nil", ctxEnd.info.Result, ctxEnd.info.Error, `"ok"`)
	}
	if !ctxEnd.info.StartTimestamp.Equal(lifecycleStart) || ctxEnd.info.EndTimestamp.IsZero() {
		t.Errorf("context end timestamps = %v %v", ctxEnd.info.StartTimestamp, ctxEnd.info.EndTimestamp)
	}

	ops = []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", OperationSubTypeWaitForCallback, "wfcb", "SUCCEEDED", &wireContextDetails{Result: `"ok"`}),
	}
	resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok3", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	third := rec.take()
	assertSequenceByID(t, third, "end:1:SUCCEEDED:true")
	assertWaitForCallbackHierarchy(t, third)
	assertReplayedTimestamps(t, third[0], true)
	if third[0].info.Result != `"ok"` {
		t.Errorf("replayed context end Result = %q, want %q", third[0].info.Result, `"ok"`)
	}
}

// TestOperationLifecycleWaitForCallbackTimedOut asserts the event sequence
// of a WaitForCallback whose callback times out on a later invocation that
// lists it as updated: the callback's live end reports TIMED_OUT and the
// context's live end reports FAILED with the [*CallbackTimeoutError] the
// operation returns. A
// third invocation replays the FAILED context as one replayed end carrying
// the same error.
func TestOperationLifecycleWaitForCallbackTimedOut(t *testing.T) {
	rec := &opRecorder{}
	got := &errBox{}
	handler := waitForCallbackHandler(rec, nil, got)
	wfcbFirstInvocation(t, handler, rec)

	timeout := &wireFullError{ErrorType: "CallbackTimeoutError", ErrorMessage: "callback timed out"}
	ops := wfcbResumeOps("TIMED_OUT", &wireCallbackDetails{CallbackId: "cb-1", Error: timeout})
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok2", ops, []string{hashID("1-1")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationFailed)
	second := rec.take()
	assertSequenceByID(t, second,
		"start:1:STARTED:true",
		"end:1-1:TIMED_OUT:false",
		"start:1-2:SUCCEEDED:true",
		"end:1-2:SUCCEEDED:true",
		"end:1:FAILED:false",
	)
	assertWaitForCallbackHierarchy(t, second)
	var cbTimeout *CallbackTimeoutError
	if second[1].info.Error == nil || !errors.As(second[1].info.Error, &cbTimeout) {
		t.Errorf("callback end Error = %#v, want *CallbackTimeoutError", second[1].info.Error)
	}
	ctxEnd := second[4]
	if ctxEnd.info.Error == nil || ctxEnd.info.Error != got.get() {
		t.Fatalf("context end Error = %v, want the error WaitForCallback returned %v", ctxEnd.info.Error, got.get())
	}
	if !errors.As(ctxEnd.info.Error, &cbTimeout) || cbTimeout.Name != "wfcb" {
		t.Errorf("context end Error = %#v, want *CallbackTimeoutError named wfcb", ctxEnd.info.Error)
	}

	// The context's own FAILED record replays as one end; the child
	// records stay so the same error is rebuilt.
	ops[1] = contextOp("1", "", OperationSubTypeWaitForCallback, "wfcb", "FAILED", &wireContextDetails{Error: timeout})
	resp, err = handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok3", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationFailed)
	third := rec.take()
	assertSequenceByID(t, third, "end:1:FAILED:true")
	assertReplayedTimestamps(t, third[0], true)
	if third[0].info.Error == nil || third[0].info.Error != got.get() || !errors.As(third[0].info.Error, &cbTimeout) {
		t.Errorf("replayed context end Error = %#v, want the *CallbackTimeoutError WaitForCallback returned", third[0].info.Error)
	}
}

// TestOperationLifecycleWaitForCallbackFailedSubmitter asserts the event
// sequence of a WaitForCallback whose submitter step fails: the context,
// callback, and step start; the step ends FAILED; the context ends FAILED
// with the [*CallbackSubmitterError] the operation returns; the callback,
// which never settles, dispatches no end. A second invocation replays the
// FAILED context as one replayed end carrying the same error.
func TestOperationLifecycleWaitForCallbackFailedSubmitter(t *testing.T) {
	rec := &opRecorder{}
	got := &errBox{}
	submitErr := errors.New("submit boom")
	handler := waitForCallbackHandler(rec, submitErr, got)

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationFailed)
	live := rec.take()
	assertSequenceByID(t, live,
		"start:1:STARTED:false",
		"start:1-1:STARTED:false",
		"start:1-2:STARTED:false",
		"end:1-2:FAILED:false",
		"end:1:FAILED:false",
	)
	assertWaitForCallbackHierarchy(t, live)
	for _, ev := range live {
		assertLiveTimestamps(t, ev)
	}
	var submitter *CallbackSubmitterError
	ctxEnd := live[4]
	if ctxEnd.info.Error == nil || ctxEnd.info.Error != got.get() {
		t.Fatalf("context end Error = %v, want the error WaitForCallback returned %v", ctxEnd.info.Error, got.get())
	}
	if !errors.As(ctxEnd.info.Error, &submitter) || submitter.Message != submitErr.Error() {
		t.Errorf("context end Error = %#v, want *CallbackSubmitterError with message %q", ctxEnd.info.Error, submitErr.Error())
	}

	ops := []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", OperationSubTypeWaitForCallback, "wfcb", "FAILED", &wireContextDetails{
			Error: &wireFullError{ErrorType: "CallbackSubmitterError", ErrorMessage: submitErr.Error()},
		}),
		{
			Id: hashID("1-1"), ParentId: hashID("1"), Status: "STARTED", Type: "CALLBACK", SubType: "Callback",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1"},
			StartTimestamp:  flexTimestamp{Time: lifecycleStart, Valid: true},
		},
		{
			Id: hashID("1-2"), ParentId: hashID("1"), Status: "FAILED", Type: "STEP", SubType: "Step",
			StepDetails:    &wireStepDetails{Attempt: 1, Error: &wireStepError{ErrorType: "Error", ErrorMessage: submitErr.Error()}},
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
	assertSequenceByID(t, replay, "end:1:FAILED:true")
	assertReplayedTimestamps(t, replay[0], true)
	if replay[0].info.Error == nil || replay[0].info.Error != got.get() || !errors.As(replay[0].info.Error, &submitter) {
		t.Errorf("replayed context end Error = %#v, want the *CallbackSubmitterError WaitForCallback returned", replay[0].info.Error)
	}
}

// TestOperationLifecycleContextAtMostOnePerInvocation asserts that a
// handler running a child context and a WaitForCallback dispatches at most
// one start and at most one end per operation in one invocation, and that
// no end is dispatched for an operation that has not settled.
func TestOperationLifecycleContextAtMostOnePerInvocation(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		child := RunInChildContextAsync(ctx, "child", func(c Context) (string, error) {
			return Step(c, "s", func(StepContext) (string, error) { return "v", nil })
		})
		if _, err := child.Result(); err != nil {
			return "", err
		}
		return WaitForCallback[string](ctx, "wfcb", func(StepContext, string) error { return nil })
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	count := map[string]int{}
	for _, ev := range rec.take() {
		count[ev.hook+":"+ev.info.ID]++
	}
	for k, n := range count {
		if n > 1 {
			t.Errorf("%s dispatched %d times, want at most 1", k, n)
		}
	}
	for _, k := range []string{"start:1", "start:1-1", "end:1-1", "end:1", "start:2", "start:2-1", "start:2-2", "end:2-2"} {
		if count[k] != 1 {
			t.Errorf("%s dispatched %d times, want 1; counts %v", k, count[k], count)
		}
	}
	for _, k := range []string{"end:2", "end:2-1"} {
		if count[k] != 0 {
			t.Errorf("%s dispatched %d times, want 0: the operation has not settled", k, count[k])
		}
	}
}

// TestOperationLifecycleChildContextReplayedSucceededSerdesFailure asserts
// that a SUCCEEDED child context whose result cannot be deserialized still
// dispatches one replayed end, for RunInChildContext and
// RunInChildContextAsync: the context settled when the checkpoint recorded
// it, so the result Serdes failure belongs to the caller.
func TestOperationLifecycleChildContextReplayedSucceededSerdesFailure(t *testing.T) {
	cause := errors.New("unmarshal exploded")
	serdes := WithChildSerdes(failingSerdes{failUnmarshal: true, cause: cause})
	body := func(Context) (string, error) { return "v", nil }
	handlers := func(rec *opRecorder, got *errBox) map[string]func(context.Context, []byte) ([]byte, error) {
		return map[string]func(context.Context, []byte) ([]byte, error){
			"RunInChildContext": Wrap(func(ctx Context, _ string) (string, error) {
				_, err := RunInChildContext(ctx, "child", body, serdes)
				got.set(err)
				return "", nil
			}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
			"RunInChildContextAsync": Wrap(func(ctx Context, _ string) (string, error) {
				_, err := RunInChildContextAsync(ctx, "child", body, serdes).Result()
				got.set(err)
				return "", nil
			}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{})),
		}
	}
	for variant := range handlers(&opRecorder{}, &errBox{}) {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			got := &errBox{}
			handler := handlers(rec, got)[variant]
			ops := []wireOperation{
				lifecycleExecOp(),
				contextOp("1", "", OperationSubTypeRunInChildContext, "child", "SUCCEEDED", &wireContextDetails{Result: `"v"`}),
			}
			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			assertSerdesError(t, got.get(), "child", "unmarshal", cause)
			evs := rec.take()
			assertSequence(t, evs, "end:child:SUCCEEDED:true")
			assertReplayedTimestamps(t, evs[0], true)
			if evs[0].info.Result != `"v"` {
				t.Errorf("replayed end Result = %q, want %q", evs[0].info.Result, `"v"`)
			}
		})
	}
}

// TestOperationLifecycleChildContextLiveEndUsesCheckpointTimestamp asserts
// that the live end of a child context reports the end timestamp the
// terminal checkpoint response carried, when it carried one.
func TestOperationLifecycleChildContextLiveEndUsesCheckpointTimestamp(t *testing.T) {
	rec := &opRecorder{}
	client := &fakePluginClient{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "child", func(Context) (string, error) { return "v", nil })
	}, WithPlugins(rec.plugin()), withLambdaAPI(client))

	// Every checkpoint response carries the child's record as SUCCEEDED
	// with both timestamps.
	start, end := lifecycleStart, lifecycleEnd
	client.newState = []Operation{{
		Id: aws.String(hashID("1")), Status: OperationStatusSucceeded, Type: OperationTypeContext,
		SubType: aws.String(OperationSubTypeRunInChildContext), Name: aws.String("child"),
		StartTimestamp: &start, EndTimestamp: &end,
		ContextDetails: &ContextDetails{Result: aws.String(`"v"`)},
	}}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()
	assertSequence(t, evs, "start:child:STARTED:false", "end:child:SUCCEEDED:false")
	if !evs[0].info.StartTimestamp.Equal(lifecycleStart) {
		t.Errorf("start StartTimestamp = %v, want %v", evs[0].info.StartTimestamp, lifecycleStart)
	}
	if !evs[1].info.EndTimestamp.Equal(lifecycleEnd) {
		t.Errorf("end EndTimestamp = %v, want %v", evs[1].info.EndTimestamp, lifecycleEnd)
	}
}

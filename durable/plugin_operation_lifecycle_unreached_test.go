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

// TestOperationLifecycleUnreachedOutcomeDispatchesEnd asserts the end of an
// outcome that is new to an invocation whose handler does not reach the
// operation. That invocation dispatches the end, with IsReplay false and
// the checkpointed fields, before OnInvocationEnd. A later invocation that
// reaches the operation replays an outcome an earlier invocation already
// observed, so it dispatches no end for it.
func TestOperationLifecycleUnreachedOutcomeDispatchesEnd(t *testing.T) {
	rec := &opRecorder{}
	var mu sync.Mutex
	var operations map[string]OperationHookInfo
	// endsAtInvocationEnd is the number of operation events recorded when
	// OnInvocationEnd runs.
	endsAtInvocationEnd := -1
	plugin := rec.plugin()
	plugin.OnInvocationStart = func(_ context.Context, info InvocationHookInfo) {
		mu.Lock()
		operations = info.Operations
		mu.Unlock()
	}
	plugin.OnInvocationEnd = func(context.Context, InvocationEndHookInfo) {
		rec.mu.Lock()
		n := len(rec.events)
		rec.mu.Unlock()
		mu.Lock()
		endsAtInvocationEnd = n
		mu.Unlock()
	}
	// skip makes the handler return before it reaches the wait. It
	// simulates an invocation that ends before the handler reaches an
	// operation listed as updated.
	var skip atomic.Bool
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if skip.Load() {
			return "skipped", nil
		}
		if err := Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		return Step(ctx, "after", func(StepContext) (string, error) { return "ok", nil })
	}, withTestPlugins(plugin), withLambdaAPI(&fakePluginClient{}))

	waitID := hashID("1")
	waitOp := wireOperation{Id: waitID, Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause",
		StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
		EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true}}

	skip.Store(true)
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok2",
		[]wireOperation{lifecycleExecOp(), waitOp}, []string{waitID}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()
	assertSequence(t, evs, "end:pause:SUCCEEDED:false")
	assertIdentity(t, evs[0], waitID, "pause", string(OperationTypeWait), OperationSubTypeWait, "")
	assertReplayedTimestamps(t, evs[0], true)

	mu.Lock()
	got, ok := operations[waitID]
	atEnd := endsAtInvocationEnd
	mu.Unlock()
	if atEnd != 1 {
		t.Errorf("OnInvocationEnd ran after %d operation events, want 1: the unreached end precedes it", atEnd)
	}
	if !ok {
		t.Fatalf("Operations lacks the wait %q: %v", waitID, operations)
	}
	if got.Status != PluginOperationSucceeded || got.Name != "pause" {
		t.Errorf("Operations[%q] = %+v, want the SUCCEEDED wait %q", waitID, got, "pause")
	}

	skip.Store(false)
	evs = runReplayAgain(t, handler, rec, []wireOperation{waitOp}, invocationSucceeded)
	assertSequence(t, evs,
		"start:after:STARTED:false",
		"end:after:SUCCEEDED:false",
	)
}

// TestOperationLifecycleUnreachedEndByType asserts which operation types
// dispatch an end when the handler returns without reaching an operation
// listed as updated. A wait, a callback, and an invoke settle outside the
// invocation, so each dispatches its end. A step and a context dispatch
// none: a plugin reads their outcome from InvocationHookInfo.Operations.
func TestOperationLifecycleUnreachedEndByType(t *testing.T) {
	stamped := func(op wireOperation) wireOperation {
		op.Id = hashID("1")
		op.Name = "op"
		op.StartTimestamp = flexTimestamp{Time: lifecycleStart, Valid: true}
		op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}
		return op
	}
	cases := []struct {
		name string
		op   wireOperation
		want []string
	}{
		{
			name: "wait",
			op:   wireOperation{Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait"},
			want: []string{"end:op:SUCCEEDED:false"},
		},
		{
			name: "callback",
			op:   wireOperation{Status: "SUCCEEDED", Type: "CALLBACK", SubType: "Callback", CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1"}},
			want: []string{"end:op:SUCCEEDED:false"},
		},
		{
			name: "timed out callback",
			op:   wireOperation{Status: "TIMED_OUT", Type: "CALLBACK", SubType: "Callback", CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1"}},
			want: []string{"end:op:TIMED_OUT:false"},
		},
		{
			name: "invoke",
			op:   wireOperation{Status: "SUCCEEDED", Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"out"`}},
			want: []string{"end:op:SUCCEEDED:false"},
		},
		{
			name: "step",
			op:   wireOperation{Status: "SUCCEEDED", Type: "STEP", SubType: "Step", StepDetails: &wireStepDetails{Attempt: 1, Result: `"v"`}},
		},
		{
			name: "context",
			op:   wireOperation{Status: "SUCCEEDED", Type: "CONTEXT", SubType: "RunInChildContext", ContextDetails: &wireContextDetails{Result: `"v"`}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &opRecorder{}
			handler := Wrap(func(Context, string) (string, error) {
				return "early", nil
			}, withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
			op := stamped(tc.op)
			resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1",
				[]wireOperation{lifecycleExecOp(), op}, []string{op.Id}))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			evs := rec.take()
			assertSequence(t, evs, tc.want...)
			for _, ev := range evs {
				assertIdentity(t, ev, op.Id, "op", tc.op.Type, tc.op.SubType, "")
				assertReplayedTimestamps(t, ev, true)
			}
			if tc.name == "invoke" && evs[0].info.Result != `"out"` {
				t.Errorf("invoke end Result = %q, want %q", evs[0].info.Result, `"out"`)
			}
		})
	}
}

// TestOperationLifecycleUnreachedEndErrorMatchesReachedEnd asserts that the
// end of a callback or an invoke that did not succeed carries the same
// Error whether the handler reaches the operation or returns before it:
// the same error type, the same text, and the same sentinel.
func TestOperationLifecycleUnreachedEndErrorMatchesReachedEnd(t *testing.T) {
	failure := &wireError{ErrorType: "RemoteError", ErrorMessage: "remote failed"}
	callback := func(status string, e *wireError) wireOperation {
		return wireOperation{Status: status, Type: "CALLBACK", SubType: "Callback",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1", Error: e}}
	}
	invoke := func(status string, details *wireChainedInvokeDetails) wireOperation {
		return wireOperation{Status: status, Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", ChainedInvokeDetails: details}
	}
	cases := []struct {
		name string
		op   wireOperation
		// sentinel, when non-nil, is an error both ends must match.
		sentinel error
		// callback reports that both ends must match *CallbackError.
		callback bool
	}{
		{name: "failed callback", op: callback("FAILED", failure), callback: true},
		{name: "failed callback without details", op: callback("FAILED", nil), callback: true},
		{name: "timed out callback", op: callback("TIMED_OUT", nil), sentinel: ErrCallbackTimedOut, callback: true},
		{name: "failed invoke", op: invoke("FAILED", &wireChainedInvokeDetails{Error: failure})},
		{name: "failed invoke without details", op: invoke("FAILED", nil)},
		{name: "timed out invoke", op: invoke("TIMED_OUT", nil), sentinel: ErrInvokeTimedOut},
		{name: "stopped invoke", op: invoke("STOPPED", nil), sentinel: ErrExecutionStopped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := tc.op
			op.Id = hashID("1")
			op.Name = "op"
			op.StartTimestamp = flexTimestamp{Time: lifecycleStart, Valid: true}
			op.EndTimestamp = flexTimestamp{Time: lifecycleEnd, Valid: true}

			// endError runs one invocation that lists op as updated and
			// returns the Error of the single end it dispatches.
			endError := func(reach bool) error {
				t.Helper()
				rec := &opRecorder{}
				handler := Wrap(func(ctx Context, _ string) (string, error) {
					if !reach {
						return "early", nil
					}
					// The handler ignores the operation's failure, so both
					// invocations succeed.
					if tc.op.Type == "CALLBACK" {
						_, _ = CreateCallback[string](ctx, "op")
					} else {
						_, _ = Invoke[string](ctx, "op", "fn", "in")
					}
					return "reached", nil
				}, withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
				resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1",
					[]wireOperation{lifecycleExecOp(), op}, []string{op.Id}))
				if err != nil {
					t.Fatal(err)
				}
				assertPluginResponseStatus(t, resp, invocationSucceeded)
				evs := rec.take()
				assertSequence(t, evs, "end:op:"+tc.op.Status+":false")
				return evs[0].info.Error
			}

			reached, unreached := endError(true), endError(false)
			if reached == nil || unreached == nil {
				t.Fatalf("reached Error = %v, unreached Error = %v, want both non-nil", reached, unreached)
			}
			if gotType, wantType := fmt.Sprintf("%T", unreached), fmt.Sprintf("%T", reached); gotType != wantType {
				t.Errorf("unreached Error type = %s, want the reached type %s", gotType, wantType)
			}
			if unreached.Error() != reached.Error() {
				t.Errorf("unreached Error = %q, want the reached text %q", unreached, reached)
			}
			for path, err := range map[string]error{"reached": reached, "unreached": unreached} {
				if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
					t.Errorf("%s Error %v does not match the sentinel %v", path, err, tc.sentinel)
				}
				var cbErr *CallbackError
				if tc.callback && !errors.As(err, &cbErr) {
					t.Errorf("%s Error %T does not match *CallbackError", path, err)
				}
			}
		})
	}
}

// TestOperationLifecycleUnreachedEndWhenSuspended asserts that an
// invocation that suspends dispatches the end of a settled wait the
// handler did not reach. The handler suspends on the wait "first". The
// wait "pause" comes after it and is listed as updated.
func TestOperationLifecycleUnreachedEndWhenSuspended(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if err := Wait(ctx, "first", time.Hour); err != nil {
			return "", err
		}
		return "", Wait(ctx, "pause", 5*time.Second)
	}, withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: hashID("1"), Status: "STARTED", Type: "WAIT", SubType: "Wait", Name: "first",
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true}},
		{Id: hashID("2"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause",
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true}},
	}
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1", ops, []string{hashID("2")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)
	assertSequence(t, rec.take(),
		"start:first:STARTED:true",
		"end:pause:SUCCEEDED:false",
	)
}

// TestOperationLifecycleUnreachedEndSkippedWhenRetrying asserts that an
// invocation that ends as Retrying dispatches no end for an operation the
// handler did not reach. The retry observes the same outcome as new, so
// the retry dispatches the end.
func TestOperationLifecycleUnreachedEndSkippedWhenRetrying(t *testing.T) {
	rec := &opRecorder{}
	handlerErr := &ClientError{Scope: ErrorScopeInvocation, Err: errors.New("transient")}
	var fail atomic.Bool
	handler := Wrap(func(Context, string) (string, error) {
		if fail.Load() {
			return "", handlerErr
		}
		return "early", nil
	}, withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	waitID := hashID("1")
	payload := func() []byte {
		return makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1", []wireOperation{
			lifecycleExecOp(),
			{Id: waitID, Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause",
				StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
				EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true}},
		}, []string{waitID})
	}

	fail.Store(true)
	if _, err := handler(makePluginContext(), payload()); !errors.Is(err, handlerErr) {
		t.Fatalf("Invoke error = %v, want the handler's invocation-scoped error", err)
	}
	assertSequence(t, rec.take())

	// The retry receives the same payload and dispatches the end.
	fail.Store(false)
	resp, err := handler(makePluginContext(), payload())
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	assertSequence(t, rec.take(), "end:pause:SUCCEEDED:false")
}

// TestOperationLifecycleUnreachedEndObeysDepthBound asserts that the
// plugin depth bound applies to the ends of operations the handler did not
// reach. With WithPluginChildOperationsDepth(0) the top-level wait
// dispatches its end, and the wait inside the child context dispatches
// none.
func TestOperationLifecycleUnreachedEndObeysDepthBound(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(Context, string) (string, error) {
		return "early", nil
	}, withTestPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}), WithPluginChildOperationsDepth(0))

	wait := func(id, parent, name string) wireOperation {
		op := wireOperation{Id: hashID(id), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: name,
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true},
			EndTimestamp:   flexTimestamp{Time: lifecycleEnd, Valid: true}}
		if parent != "" {
			op.ParentId = hashID(parent)
		}
		return op
	}
	ops := []wireOperation{
		lifecycleExecOp(),
		wait("1", "", "top"),
		contextOp("2", "", "RunInChildContext", "child", "STARTED", nil),
		wait("2-1", "2", "inner"),
	}
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:lifecycle", "tok1", ops,
		[]string{hashID("1"), hashID("2-1")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	assertSequence(t, rec.take(), "end:top:SUCCEEDED:false")
}

// TestUnreachedEndClaimedOnce asserts that an operation dispatches one end
// per invocation when a branch goroutine replays it while the invocation
// dispatches the unreached ends. Both paths run concurrently for the same
// operations; executionState.claimEnd lets exactly one of them notify.
func TestUnreachedEndClaimedOnce(t *testing.T) {
	const n = 64
	ops := make([]*operation, 0, n)
	for i := range n {
		ops = append(ops, &operation{
			id: hashID(string(rune('a' + i))), opType: string(OperationTypeWait), subType: OperationSubTypeWait,
			status: statusSucceeded, endTimestamp: lifecycleEnd,
		})
	}
	state := newExecutionState(ops)
	updated := make([]string, 0, n)
	for _, op := range ops {
		updated = append(updated, op.id)
	}
	state.setUpdatedOperationIDs(updated)

	var mu sync.Mutex
	ends := make(map[string]int)
	d := newPluginDispatcher([]Plugin{{
		OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			ends[info.ID]++
			mu.Unlock()
		},
	}})
	ec := &execContext{Context: context.Background(), executionArn: "arn", state: state, pluginDispatcher: d}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatchUnreachedEnds(d, context.Background(), "arn", state, 0)
	}()
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := string(rune('a' + i))
			info := ec.operationHookInfo(id, "", string(OperationTypeWait), OperationSubTypeWait, true)
			dispatchReplayedEnd(ec, id, info, PluginOperationSucceeded)
		}()
	}
	wg.Wait()

	if len(ends) != n {
		t.Fatalf("ends dispatched for %d operations, want %d", len(ends), n)
	}
	for id, count := range ends {
		if count != 1 {
			t.Errorf("operation %s dispatched %d ends, want 1", id, count)
		}
	}
}

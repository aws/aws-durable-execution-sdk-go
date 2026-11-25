package durable

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stepEchoClient is a fake ExecutionClient. Like the service, it returns
// the record of a step in the response to the checkpoint request that
// started or completed it: STARTED for a START, SUCCEEDED with the result
// and an end timestamp for a SUCCEED. Other updates are accepted and not
// returned.
type stepEchoClient struct{}

func (stepEchoClient) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (stepEchoClient) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	out := CheckpointOutput{CheckpointToken: in.CheckpointToken}
	for _, u := range in.Updates {
		if u.Type != OperationTypeStep {
			continue
		}
		start, end := lifecycleStart, lifecycleEnd
		op := Operation{
			Id: u.Id, Type: u.Type, SubType: u.SubType, Name: u.Name, ParentId: u.ParentId,
			StartTimestamp: &start,
		}
		switch u.Action {
		case OperationActionStart:
			op.Status = OperationStatusStarted
			op.StepDetails = &StepDetails{Attempt: 1}
		case OperationActionSucceed:
			op.Status = OperationStatusSucceeded
			op.EndTimestamp = &end
			op.StepDetails = &StepDetails{Attempt: 1, Result: u.Payload}
		default:
			continue
		}
		out.NewExecutionState = append(out.NewExecutionState, op)
	}
	return out, nil
}

// changeRecorder is a plugin that records every OnOperationChange info and
// the order of the change and invocation-end hooks.
type changeRecorder struct {
	mu     sync.Mutex
	calls  []OperationChangeHookInfo
	events []string
	// active counts the OnOperationChange calls in progress. overlapped
	// is set when a call starts while another is in progress.
	active     atomic.Int32
	overlapped atomic.Bool
}

func (r *changeRecorder) plugin() Plugin {
	return Plugin{
		OnOperationChange: func(_ context.Context, info OperationChangeHookInfo) {
			if r.active.Add(1) > 1 {
				r.overlapped.Store(true)
			}
			defer r.active.Add(-1)
			r.mu.Lock()
			r.calls = append(r.calls, info)
			r.events = append(r.events, "change")
			r.mu.Unlock()
		},
		OnInvocationEnd: func(context.Context, InvocationEndHookInfo) {
			r.mu.Lock()
			r.events = append(r.events, "end")
			r.mu.Unlock()
		},
	}
}

// succeededCalls returns how many recorded calls report the operation with
// the wire ID as SUCCEEDED in UpdatedOperations.
func (r *changeRecorder) succeededCalls(wireID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, info := range r.calls {
		if op, ok := info.UpdatedOperations[wireID]; ok && op.Status == PluginOperationSucceeded {
			n++
		}
	}
	return n
}

// assertEndIsLast fails the test unless OnInvocationEnd was dispatched
// exactly once and after every OnOperationChange.
func (r *changeRecorder) assertEndIsLast(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	ends := 0
	for _, e := range r.events {
		if e == "end" {
			ends++
		}
	}
	if ends != 1 || r.events[len(r.events)-1] != "end" {
		t.Errorf("hook order = %v, want every change before one end", r.events)
	}
}

// TestOperationChangeReportsStepSucceededInInvocation runs one step to
// SUCCEEDED inside one invocation. The hook reports that step with status
// SUCCEEDED exactly once, the step is in Operations, and every report
// precedes OnInvocationEnd.
func TestOperationChangeReportsStepSucceededInInvocation(t *testing.T) {
	const arn = "arn:test:change"
	rec := &changeRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "greet", func(StepContext) (string, error) { return "task-a", nil })
	}, withTestPlugins(rec.plugin()), withLambdaAPI(stepEchoClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, arn, "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	stepID := hashID("1")
	if got := rec.succeededCalls(stepID); got != 1 {
		t.Fatalf("step reported SUCCEEDED %d times, want 1", got)
	}
	rec.assertEndIsLast(t)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i, info := range rec.calls {
		if info.ExecutionArn != arn {
			t.Errorf("call %d: ExecutionArn = %q, want %q", i, info.ExecutionArn, arn)
		}
		if len(info.UpdatedOperations) == 0 {
			t.Errorf("call %d: UpdatedOperations is empty", i)
		}
		for id, op := range info.UpdatedOperations {
			if _, ok := info.Operations[id]; !ok {
				t.Errorf("call %d: updated operation %q is not in Operations", i, id)
			}
			if op.IsReplay {
				t.Errorf("call %d: updated operation %q has IsReplay set", i, id)
			}
		}
		// The execution operation was recorded before the invocation
		// and no response changed it.
		if exec, ok := info.Operations["exec"]; !ok || !exec.IsReplay {
			t.Errorf("call %d: Operations[exec] = %+v, present %v; want it present with IsReplay set", i, exec, ok)
		}
		op, ok := info.UpdatedOperations[stepID]
		if !ok || op.Status != PluginOperationSucceeded {
			continue
		}
		if op.Name != "greet" || op.Type != string(OperationTypeStep) || op.Attempt != 1 || op.Result != `"task-a"` {
			t.Errorf("succeeded step info = %+v, want name greet, type STEP, attempt 1, result \"task-a\"", op)
		}
		if op.StartTimestamp.IsZero() || op.EndTimestamp.IsZero() {
			t.Errorf("succeeded step timestamps = %v, %v; want both set", op.StartTimestamp, op.EndTimestamp)
		}
		if full := info.Operations[stepID]; full.Status != PluginOperationSucceeded || full.IsReplay {
			t.Errorf("Operations[step] = %+v, want SUCCEEDED with IsReplay unset", full)
		}
	}
}

// TestOperationChangeHookDoesNotDelayCheckpoints asserts the two halves of
// the dispatch contract. The checkpoint path does not wait for the hook:
// the first hook call blocks until the second step has run, which can only
// happen if the first step's checkpoints returned meanwhile. And the
// invocation waits for the hook: both steps are reported before
// OnInvocationEnd although the hook was still blocked when the handler was
// running.
func TestOperationChangeHookDoesNotDelayCheckpoints(t *testing.T) {
	rec := &changeRecorder{}
	secondStepRan := make(chan struct{})
	var blockedOnce sync.Once
	var timedOut atomic.Bool
	p := rec.plugin()
	record := p.OnOperationChange
	p.OnOperationChange = func(ctx context.Context, info OperationChangeHookInfo) {
		blockedOnce.Do(func() {
			select {
			case <-secondStepRan:
			case <-time.After(10 * time.Second):
				timedOut.Store(true)
			}
		})
		record(ctx, info)
	}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "first", func(StepContext) (string, error) { return "a", nil }); err != nil {
			return "", err
		}
		return Step(ctx, "second", func(StepContext) (string, error) {
			close(secondStepRan)
			return "b", nil
		})
	}, withTestPlugins(p), withLambdaAPI(stepEchoClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:change", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	if timedOut.Load() {
		t.Fatal("the second step did not run while the hook was blocked: the checkpoint path waited for the hook")
	}
	for _, id := range []string{hashID("1"), hashID("2")} {
		if got := rec.succeededCalls(id); got != 1 {
			t.Errorf("step %s reported SUCCEEDED %d times before the invocation ended, want 1", id, got)
		}
	}
	rec.assertEndIsLast(t)
}

// TestOperationChangeFromConcurrentBranches runs steps on concurrent Map
// branches with two plugins registered, for the race detector. Each plugin
// receives every step's SUCCEEDED change exactly once, before
// OnInvocationEnd, and its OnOperationChange calls never overlap.
func TestOperationChangeFromConcurrentBranches(t *testing.T) {
	const items = 8
	recs := []*changeRecorder{{}, {}}
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		br, err := Map(ctx, "batch", make([]int, items), func(c Context, _ int, idx int) (int, error) {
			return Step(c, fmt.Sprintf("step-%d", idx), func(StepContext) (int, error) { return idx, nil })
		}, WithMaxConcurrency(items))
		if err != nil {
			return 0, err
		}
		return len(br.Results()), nil
	}, withTestPlugins(recs[0].plugin(), recs[1].plugin()), withLambdaAPI(stepEchoClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:change", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	for i, rec := range recs {
		if rec.overlapped.Load() {
			t.Errorf("plugin %d: OnOperationChange calls overlapped", i)
		}
		rec.assertEndIsLast(t)
		succeeded := map[string]int{}
		rec.mu.Lock()
		for _, info := range rec.calls {
			for id, op := range info.UpdatedOperations {
				if op.Status == PluginOperationSucceeded {
					succeeded[id]++
				}
			}
		}
		rec.mu.Unlock()
		if len(succeeded) != items {
			t.Errorf("plugin %d: %d steps reported SUCCEEDED, want %d", i, len(succeeded), items)
		}
		for id, n := range succeeded {
			if n != 1 {
				t.Errorf("plugin %d: step %s reported SUCCEEDED %d times, want 1", i, id, n)
			}
		}
	}
}

// slowStepStartClient is a stepEchoClient that holds the checkpoint call
// carrying a step START for a fixed time. It closes waitSeen when a call
// carrying a WAIT update returns, and stepSent when the held call begins.
type slowStepStartClient struct {
	stepEchoClient
	hold     time.Duration
	waitSeen chan struct{}
	stepSent chan struct{}
	waitOnce sync.Once
	stepOnce sync.Once
}

func (c *slowStepStartClient) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	for _, u := range in.Updates {
		if u.Type == OperationTypeStep && u.Action == OperationActionStart {
			c.stepOnce.Do(func() { close(c.stepSent) })
			time.Sleep(c.hold)
		}
		if u.Type == OperationTypeWait {
			defer c.waitOnce.Do(func() { close(c.waitSeen) })
		}
	}
	return c.stepEchoClient.Checkpoint(ctx, in)
}

// TestOperationChangeReportsCallInFlightWhenInvocationPends asserts that an
// invocation which ends as pending reports the response of a checkpoint
// call that was in flight when the handler returned. One branch blocks on
// a wait, so the invocation is pending. A second branch starts a step, and
// the client holds that call. The handler returns while the call is held.
// The step's STARTED change is reported before OnInvocationEnd.
func TestOperationChangeReportsCallInFlightWhenInvocationPends(t *testing.T) {
	rec := &changeRecorder{}
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

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:change", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)

	rec.assertEndIsLast(t)
	started := 0
	rec.mu.Lock()
	for _, info := range rec.calls {
		for _, op := range info.UpdatedOperations {
			if op.Name == "held" && op.Status == PluginOperationStarted {
				started++
			}
		}
	}
	rec.mu.Unlock()
	if started != 1 {
		t.Errorf("step reported STARTED %d times, want 1: its checkpoint call was in flight when the handler returned", started)
	}
}

// TestOperationChangeNotDispatchedForChangesBetweenInvocations asserts
// that an operation which changed between two invocations is reported in
// InvocationHookInfo.UpdatedOperations and through no OnOperationChange
// call: the hook reports checkpoint responses only.
func TestOperationChangeNotDispatchedForChangesBetweenInvocations(t *testing.T) {
	rec := &changeRecorder{}
	var start InvocationHookInfo
	p := rec.plugin()
	p.OnInvocationStart = func(_ context.Context, info InvocationHookInfo) {
		rec.mu.Lock()
		start = info
		rec.mu.Unlock()
	}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return "done", Wait(ctx, "pause", time.Second)
	}, withTestPlugins(p), withLambdaAPI(&fakePluginClient{}))

	waitID := hashID("1")
	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: waitID, Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
	}
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:change", "tok2", ops, []string{waitID}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, ok := start.UpdatedOperations[waitID]; !ok {
		t.Errorf("InvocationHookInfo.UpdatedOperations = %v, want the wait", start.UpdatedOperations)
	}
	if len(rec.calls) != 0 {
		t.Errorf("OnOperationChange fired %d times for a change between invocations, want 0: %v", len(rec.calls), rec.calls)
	}
}

// TestExecutionStateMergeReportsStatusChanges asserts what merge counts as
// a change: a new operation and a different status do, the same status
// does not, and nothing does when tracking is off.
func TestExecutionStateMergeReportsStatusChanges(t *testing.T) {
	s := newExecutionState([]*operation{{id: "a", status: statusStarted}})
	if got := s.merge([]*operation{{id: "a", status: statusSucceeded}}); got != nil {
		t.Errorf("merge without tracking returned %v, want nil", got)
	}

	s = newExecutionState([]*operation{{id: "a", status: statusStarted}})
	s.trackChanges = true
	got := s.merge([]*operation{
		{id: "a", status: statusStarted},
		{id: "b", status: statusStarted},
	})
	if len(got) != 1 || got[0].id != "b" {
		t.Errorf("merge returned %v, want the new operation b alone", got)
	}
	got = s.merge([]*operation{{id: "a", status: statusSucceeded}, {id: "b", status: statusStarted}})
	if len(got) != 1 || got[0].id != "a" {
		t.Errorf("merge returned %v, want the operation a whose status changed", got)
	}
	// The first merge changed b, the second changed a.
	_, seq := s.snapshot()
	if seq != 2 {
		t.Errorf("merge sequence = %d, want 2", seq)
	}
	for through, want := range map[uint64][]string{1: {"b"}, 2: {"a", "b"}} {
		var ids []string
		s.rangeChangedThrough(through, func(id string) { ids = append(ids, id) })
		slices.Sort(ids)
		if !slices.Equal(ids, want) {
			t.Errorf("changed through merge %d = %v, want %v", through, ids, want)
		}
	}
}

// TestOperationChangeQueueClose asserts the two rules of close. A report
// queued after close is dropped. And when the context ends while a hook is
// still running, the reports not yet dispatched are dropped, so no hook
// starts after OnInvocationEnd.
func TestOperationChangeQueueClose(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	d := newPluginDispatcher([]Plugin{{OnOperationChange: func(context.Context, OperationChangeHookInfo) {
		calls.Add(1)
		// Non-blocking, so a regression that runs more calls fails the
		// count below instead of blocking here.
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	}}})
	q := newOperationChangeQueue(context.Background(), d, "arn:test:change", newExecutionState(nil), -1)
	q.report = func(operationChange) (OperationChangeHookInfo, bool) { return OperationChangeHookInfo{}, true }
	for range 3 {
		q.enqueue(nil)
	}
	<-started
	q.mu.Lock()
	idle := q.idle
	q.mu.Unlock()

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	q.close(ended)
	q.enqueue(nil)
	close(release)
	select {
	case <-idle:
	case <-time.After(10 * time.Second):
		t.Fatal("the queue goroutine did not exit")
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("hook ran %d times, want 1: the call in progress when the context ended", got)
	}
}

// TestOperationChangeQueueCloseAbandonsReportBeingBuilt asserts that no
// hook starts after close returned because its context ended. The queue
// goroutine has taken a change off the queue and is building its report
// when close returns. The hook is not called for that change.
func TestOperationChangeQueueCloseAbandonsReportBeingBuilt(t *testing.T) {
	var calls atomic.Int32
	d := newPluginDispatcher([]Plugin{{OnOperationChange: func(context.Context, OperationChangeHookInfo) {
		calls.Add(1)
	}}})
	q := newOperationChangeQueue(context.Background(), d, "arn:test:change", newExecutionState(nil), -1)
	building := make(chan struct{})
	release := make(chan struct{})
	q.report = func(operationChange) (OperationChangeHookInfo, bool) {
		close(building)
		<-release
		return OperationChangeHookInfo{}, true
	}
	q.enqueue(nil)
	<-building
	q.mu.Lock()
	idle := q.idle
	q.mu.Unlock()

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	q.close(ended)
	close(release)
	select {
	case <-idle:
	case <-time.After(10 * time.Second):
		t.Fatal("the queue goroutine did not exit")
	}

	if got := calls.Load(); got != 0 {
		t.Errorf("hook ran %d times, want 0: close returned before the report was built", got)
	}
}

// TestOperationChangeQueueNilWithoutHook asserts that an invocation whose
// plugins do not implement OnOperationChange gets no queue, and that the
// nil queue is safe to close.
func TestOperationChangeQueueNilWithoutHook(t *testing.T) {
	d := newPluginDispatcher([]Plugin{{OnInvocationEnd: func(context.Context, InvocationEndHookInfo) {}}})
	q := newOperationChangeQueue(context.Background(), d, "arn:test:change", newExecutionState(nil), -1)
	if q != nil {
		t.Fatalf("queue = %v, want nil when no plugin implements OnOperationChange", q)
	}
	q.close(context.Background())
}

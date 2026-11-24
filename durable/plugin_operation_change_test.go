package durable

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
)

// operationEchoClient is a fake ExecutionClient. The response to a
// checkpoint call carries the record of every operation the call updated,
// in the status the update's action leads to. A step record carries
// attempt 1 and, once succeeded, its result.
//
// When hold is set, the first call that carries an update hold accepts
// closes held and returns only after release is closed.
type operationEchoClient struct {
	hold    func(OperationUpdate) bool
	held    chan struct{}
	release chan struct{}

	mu     sync.Mutex
	isHeld bool
}

func newHoldingEchoClient(hold func(OperationUpdate) bool) *operationEchoClient {
	return &operationEchoClient{hold: hold, held: make(chan struct{}), release: make(chan struct{})}
}

func (c *operationEchoClient) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (c *operationEchoClient) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	out := CheckpointOutput{CheckpointToken: in.CheckpointToken}
	now := time.Now()
	hold := false
	for _, u := range in.Updates {
		var status OperationStatus
		switch u.Action {
		case OperationActionStart:
			status = OperationStatusStarted
		case OperationActionSucceed:
			status = OperationStatusSucceeded
		case OperationActionFail:
			status = OperationStatusFailed
		default:
			continue
		}
		op := Operation{
			Id: u.Id, Type: u.Type, SubType: u.SubType, Name: u.Name, ParentId: u.ParentId,
			Status: status, StartTimestamp: &now,
		}
		if status != OperationStatusStarted {
			op.EndTimestamp = &now
		}
		if u.Type == OperationTypeStep {
			op.StepDetails = &StepDetails{Attempt: 1}
			if status == OperationStatusSucceeded {
				op.StepDetails.Result = u.Payload
			}
		}
		out.NewExecutionState = append(out.NewExecutionState, op)
		if c.hold != nil && c.hold(u) {
			c.mu.Lock()
			if !c.isHeld {
				c.isHeld, hold = true, true
			}
			c.mu.Unlock()
		}
	}
	if hold {
		close(c.held)
		<-c.release
	}
	return out, nil
}

// changeRecorder records, in dispatch order, the invocation-level hooks
// and every OnOperationChange call of one plugin.
type changeRecorder struct {
	mu      sync.Mutex
	order   []string
	changes []OperationChangeHookInfo
}

func (r *changeRecorder) plugin() Plugin {
	record := func(s string) {
		r.mu.Lock()
		r.order = append(r.order, s)
		r.mu.Unlock()
	}
	return Plugin{
		OnInvocationStart: func(context.Context, InvocationHookInfo) { record("invocation-start") },
		OnInvocationEnd:   func(context.Context, InvocationEndHookInfo) { record("invocation-end") },
		OnOperationChange: func(_ context.Context, info OperationChangeHookInfo) {
			r.mu.Lock()
			r.order = append(r.order, "operation-change")
			r.changes = append(r.changes, info)
			r.mu.Unlock()
		},
	}
}

// reports returns every entry the calls reported for the operation named
// name in UpdatedOperations, in dispatch order.
func (r *changeRecorder) reports(name string) []OperationHookInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got []OperationHookInfo
	for _, c := range r.changes {
		for _, op := range c.UpdatedOperations {
			if op.Name == name {
				got = append(got, op)
			}
		}
	}
	return got
}

// TestOperationChangeReportsCheckpointResponse runs one step to completion
// in one invocation. The step's success is reported through
// OnOperationChange exactly once, with the checkpointed fields, between
// OnInvocationStart and OnInvocationEnd.
func TestOperationChangeReportsCheckpointResponse(t *testing.T) {
	rec := &changeRecorder{}
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "ok", nil })
	}, withTestPlugins(rec.plugin()), withLambdaAPI(&operationEchoClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:change", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	stepID := hashID("1")
	var succeeded []OperationHookInfo
	for _, op := range rec.reports("s1") {
		if op.Status == PluginOperationSucceeded {
			succeeded = append(succeeded, op)
		}
	}
	if len(succeeded) != 1 {
		t.Fatalf("SUCCEEDED reports of s1 = %d, want 1: %+v", len(succeeded), rec.reports("s1"))
	}
	op := succeeded[0]
	if op.ID != stepID || op.Type != string(OperationTypeStep) || op.ExecutionArn != "arn:test:change" {
		t.Errorf("identity = %+v, want ID %q, type STEP, the execution ARN", op, stepID)
	}
	if op.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", op.Attempt)
	}
	if op.IsReplay {
		t.Error("IsReplay = true, want false")
	}
	if op.Result != `"ok"` {
		t.Errorf("Result = %q, want %q", op.Result, `"ok"`)
	}
	if op.StartTimestamp.IsZero() || op.EndTimestamp.IsZero() {
		t.Errorf("timestamps = %v, %v, want both set", op.StartTimestamp, op.EndTimestamp)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, c := range rec.changes {
		if c.ExecutionArn != "arn:test:change" {
			t.Errorf("ExecutionArn = %q, want the execution ARN", c.ExecutionArn)
		}
		if _, ok := c.Operations["exec"]; !ok {
			t.Errorf("Operations = %v, want the execution operation", c.Operations)
		}
		for id, updated := range c.UpdatedOperations {
			full, ok := c.Operations[id]
			if !ok {
				t.Errorf("UpdatedOperations[%q] is missing from Operations", id)
				continue
			}
			if full.Status != updated.Status || full.IsReplay {
				t.Errorf("Operations[%q] = %+v, want status %s and IsReplay false", id, full, updated.Status)
			}
		}
	}
	if n := len(rec.order); n < 3 || rec.order[0] != "invocation-start" || rec.order[n-1] != "invocation-end" {
		t.Errorf("hook order = %v, want invocation-start first, invocation-end last, a change between", rec.order)
	}
}

// TestOperationChangeOmitsOperationsUpdatedBetweenInvocations resumes an
// execution whose wait completed between invocations. The wait is reported
// in InvocationHookInfo.UpdatedOperations. OnOperationChange reports only
// the step this invocation checkpoints, and its Operations holds both.
func TestOperationChangeOmitsOperationsUpdatedBetweenInvocations(t *testing.T) {
	rec := &changeRecorder{}
	var start InvocationHookInfo
	p := rec.plugin()
	p.OnInvocationStart = func(_ context.Context, info InvocationHookInfo) { start = info }
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		if err := Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return Step(ctx, "after", func(StepContext) (string, error) { return "ok", nil })
	}, withTestPlugins(p), withLambdaAPI(&operationEchoClient{}))

	waitID := hashID("1")
	ops := []wireOperation{
		lifecycleExecOp(),
		{Id: waitID, Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
	}
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:change", "tok1", ops, []string{waitID}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	if _, ok := start.UpdatedOperations[waitID]; !ok || len(start.UpdatedOperations) != 1 {
		t.Errorf("InvocationHookInfo.UpdatedOperations = %v, want the wait alone", start.UpdatedOperations)
	}
	if got := rec.reports("pause"); len(got) != 0 {
		t.Errorf("OnOperationChange reported the wait: %+v", got)
	}
	after := rec.reports("after")
	if len(after) == 0 || after[len(after)-1].Status != PluginOperationSucceeded {
		t.Fatalf("reports of the step = %+v, want the last one SUCCEEDED", after)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	last := rec.changes[len(rec.changes)-1]
	if got, ok := last.Operations[waitID]; !ok || got.Status != PluginOperationSucceeded || got.IsReplay {
		t.Errorf("Operations[wait] = %+v (present %v), want SUCCEEDED with IsReplay false", got, ok)
	}
}

// TestOperationChangeBeforeInvocationEndWhenCallInFlight returns from the
// handler while the call that carries a step START is still in flight. The
// change that call's response reports is dispatched before OnInvocationEnd.
func TestOperationChangeBeforeInvocationEndWhenCallInFlight(t *testing.T) {
	client := newHoldingEchoClient(func(u OperationUpdate) bool {
		return u.Type == OperationTypeStep && u.Action == OperationActionStart
	})
	rec := &changeRecorder{}
	bodyDone := make(chan struct{})
	defer close(bodyDone)
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		StepAsync(ctx, "slow", func(StepContext) (string, error) {
			<-bodyDone
			return "", nil
		})
		return "done", nil
	}, withTestPlugins(rec.plugin()), withLambdaAPI(client))

	type result struct {
		resp []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:change", "tok1", nil))
		done <- result{resp, err}
	}()
	<-client.held
	rec.mu.Lock()
	early := slices.Clone(rec.order)
	rec.mu.Unlock()
	if slices.Contains(early, "invocation-end") {
		t.Errorf("hook order while the call is in flight = %v, want no invocation-end", early)
	}
	close(client.release)
	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	assertPluginResponseStatus(t, res.resp, invocationSucceeded)

	if got := rec.reports("slow"); len(got) != 1 || got[0].Status != PluginOperationStarted {
		t.Errorf("reports of the step = %+v, want one STARTED", got)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if want := []string{"invocation-start", "operation-change", "invocation-end"}; !slices.Equal(rec.order, want) {
		t.Errorf("hook order = %v, want %v", rec.order, want)
	}
}

// TestOperationChangeDepthBound asserts that both maps of
// OperationChangeHookInfo omit the operations beyond the plugin depth
// bound, and that no call is made with an empty UpdatedOperations.
func TestOperationChangeDepthBound(t *testing.T) {
	fn := func(ctx Context, _ string) (string, error) {
		if _, err := RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			return RunInChildContext(c, "mid", func(cc Context) (string, error) {
				return Step(cc, "leaf", func(StepContext) (string, error) { return "v", nil })
			})
		}); err != nil {
			return "", err
		}
		return Step(ctx, "top", func(StepContext) (string, error) { return "v", nil })
	}
	for _, tc := range []struct {
		depth            int
		wantUpd, wantAll []string
	}{
		{-1, []string{"leaf", "mid", "outer", "top"}, []string{"", "leaf", "mid", "outer", "top"}},
		{0, []string{"outer", "top"}, []string{"", "outer", "top"}},
		{1, []string{"mid", "outer", "top"}, []string{"", "mid", "outer", "top"}},
		{2, []string{"leaf", "mid", "outer", "top"}, []string{"", "leaf", "mid", "outer", "top"}},
	} {
		t.Run(fmt.Sprintf("depth=%d", tc.depth), func(t *testing.T) {
			rec := &changeRecorder{}
			opts := []HandlerOption{withTestPlugins(rec.plugin()), withLambdaAPI(&operationEchoClient{})}
			if tc.depth >= 0 {
				opts = append(opts, WithPluginChildOperationsDepth(tc.depth))
			}
			resp, err := Wrap(fn, opts...)(makePluginContext(), makePluginPayload(t, "arn:test:depth", "tok1", nil))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)

			rec.mu.Lock()
			defer rec.mu.Unlock()
			upd, all := map[string]bool{}, map[string]bool{}
			for _, c := range rec.changes {
				if len(c.UpdatedOperations) == 0 {
					t.Error("OnOperationChange called with an empty UpdatedOperations")
				}
				for _, op := range c.UpdatedOperations {
					upd[op.Name] = true
				}
				for _, op := range c.Operations {
					all[op.Name] = true
				}
			}
			if got := sortedKeys(upd); !slices.Equal(got, tc.wantUpd) {
				t.Errorf("operations in UpdatedOperations = %v, want %v", got, tc.wantUpd)
			}
			if got := sortedKeys(all); !slices.Equal(got, tc.wantAll) {
				t.Errorf("operations in Operations = %v, want %v", got, tc.wantAll)
			}
		})
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestExecutionStateMergeReturnsStatusChanges asserts merge returns the
// operations that are new or whose status differs, and no operation whose
// status is unchanged.
func TestExecutionStateMergeReturnsStatusChanges(t *testing.T) {
	state := newExecutionState([]*operation{
		{id: "a", status: statusStarted},
		{id: "b", status: statusStarted},
	})
	changed := state.merge([]*operation{
		{id: "a", status: statusSucceeded},
		{id: "b", status: statusStarted, name: "renamed"},
		{id: "c", status: statusStarted},
	})
	var ids []string
	for _, op := range changed {
		ids = append(ids, op.id)
	}
	if want := []string{"a", "c"}; !slices.Equal(ids, want) {
		t.Errorf("changed = %v, want %v", ids, want)
	}
	if again := state.merge([]*operation{{id: "a", status: statusSucceeded}}); len(again) != 0 {
		t.Errorf("second merge of the same status returned %d operations, want 0", len(again))
	}
}

// TestOperationChangeGate asserts close waits for a dispatch in progress
// and that run dispatches nothing once the gate is closed.
func TestOperationChangeGate(t *testing.T) {
	g := &operationChangeGate{}
	entered, release := make(chan struct{}), make(chan struct{})
	finished := false
	go g.run(func() {
		close(entered)
		<-release
		finished = true
	})
	<-entered
	closed := make(chan bool, 1)
	go func() {
		g.close()
		// The gate's lock orders this read after the dispatch's write.
		closed <- finished
	}()
	select {
	case <-closed:
		t.Fatal("close returned while a dispatch was in progress")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if !<-closed {
		t.Error("close returned before the dispatch finished")
	}
	g.run(func() { t.Error("run dispatched after close") })
}

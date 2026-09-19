package durable

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
)

// depthRecorder records every operation-level hook by operation name, so a
// test can assert which operations a depth bound reports and which it
// omits. Operation start and end events also record ChildrenOmitted.
type depthRecorder struct {
	mu sync.Mutex
	// ops holds "hook:name" for OnOperationStart and OnOperationEnd,
	// "attempt-start:name" and "attempt-end:name" for the attempt hooks,
	// and "wrap-attempt:name" and "wrap-child:name" for the wrap hooks.
	hooks []string
	// omitted records ChildrenOmitted per operation name, from the start
	// and end hooks; every event of one operation must agree.
	omitted map[string]bool
}

func (r *depthRecorder) record(hook, name string) {
	r.mu.Lock()
	r.hooks = append(r.hooks, hook+":"+name)
	r.mu.Unlock()
}

func (r *depthRecorder) recordOp(t *testing.T, hook string, info OperationHookInfo) {
	r.record(hook, info.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.omitted == nil {
		r.omitted = map[string]bool{}
	}
	if prev, ok := r.omitted[info.Name]; ok && prev != info.ChildrenOmitted {
		t.Errorf("%s %s: ChildrenOmitted = %v, earlier event reported %v", hook, info.Name, info.ChildrenOmitted, prev)
	}
	r.omitted[info.Name] = info.ChildrenOmitted
}

func (r *depthRecorder) plugin(t *testing.T) Plugin {
	return Plugin{
		OnOperationStart: func(_ context.Context, info OperationHookInfo) { r.recordOp(t, "start", info) },
		OnOperationEnd:   func(_ context.Context, info OperationHookInfo) { r.recordOp(t, "end", info) },
		OnOperationAttemptStart: func(_ context.Context, info AttemptHookInfo) {
			r.recordOp(t, "attempt-start", info.OperationHookInfo)
		},
		OnOperationAttemptEnd: func(_ context.Context, info AttemptEndHookInfo) {
			r.recordOp(t, "attempt-end", info.OperationHookInfo)
		},
		WrapOperationAttemptFn: func(ctx context.Context, info AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			r.record("wrap-attempt", info.Name)
			return fn(ctx)
		},
		WrapChildContextFn: func(ctx context.Context, info OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			r.record("wrap-child", info.Name)
			return fn(ctx)
		},
	}
}

// names returns the set of operation names any hook reported, sorted.
func (r *depthRecorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	for _, h := range r.hooks {
		seen[h[strings.IndexByte(h, ':')+1:]] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// hooksFor returns the hooks reported for the operation name, in order.
func (r *depthRecorder) hooksFor(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, h := range r.hooks {
		if h[strings.IndexByte(h, ':')+1:] == name {
			out = append(out, h[:strings.IndexByte(h, ':')])
		}
	}
	return out
}

func (r *depthRecorder) childrenOmitted(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.omitted[name]
}

// assertReported checks that exactly the operations named in want were
// reported by some hook, and that ChildrenOmitted is set on exactly the
// operations named in wantOmitted.
func (r *depthRecorder) assertReported(t *testing.T, want, wantOmitted []string) {
	t.Helper()
	got := r.names()
	sorted := slices.Clone(want)
	sort.Strings(sorted)
	if !slices.Equal(got, sorted) {
		t.Errorf("reported operations = %v, want %v", got, sorted)
	}
	for _, n := range want {
		wantFlag := slices.Contains(wantOmitted, n)
		if r.childrenOmitted(n) != wantFlag {
			t.Errorf("%s: ChildrenOmitted = %v, want %v", n, r.childrenOmitted(n), wantFlag)
		}
	}
}

// countingPluginClient counts checkpoint calls and the operation updates
// they carry, so a test can assert a depth bound leaves checkpointing
// unchanged.
type countingPluginClient struct {
	fakePluginClient
	calls, updates int
}

func (c *countingPluginClient) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	c.mu.Lock()
	c.calls++
	c.updates += len(in.Updates)
	c.mu.Unlock()
	return c.fakePluginClient.Checkpoint(ctx, in)
}

func (c *countingPluginClient) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.updates
}

// runDepthHandler wraps fn with the recorder's plugin and, when depth is
// non-negative, WithPluginChildOperationsDepth(depth); it runs one
// invocation with ops as the initial state (nil for an empty execution)
// and asserts the response status. It returns the recorder and the
// checkpoint counts.
func runDepthHandler(t *testing.T, depth int, fn func(Context, string) (string, error), ops []wireOperation, wantStatus string) (*depthRecorder, int, int) {
	t.Helper()
	rec := &depthRecorder{}
	client := &countingPluginClient{}
	opts := []HandlerOption{WithPlugins(rec.plugin(t)), withLambdaAPI(client)}
	if depth >= 0 {
		opts = append(opts, WithPluginChildOperationsDepth(depth))
	}
	handler := Wrap(fn, opts...)
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:depth", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, wantStatus)
	calls, updates := client.counts()
	return rec, calls, updates
}

// TestWithPluginChildOperationsDepthRejectsNegative asserts Wrap panics
// on a negative depth, as it does for other invalid handler options.
func TestWithPluginChildOperationsDepthRejectsNegative(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap did not panic on a negative depth")
		}
		if !strings.Contains(fmt.Sprint(r), "WithPluginChildOperationsDepth") {
			t.Fatalf("panic = %v, want it to name the option", r)
		}
	}()
	Wrap(func(Context, string) (string, error) { return "", nil }, WithPluginChildOperationsDepth(-1))
}

// TestWithPluginChildOperationsDepthOptionCapture asserts the option is
// captured into handlerOptions and that the bound derives from it: unset
// reports every depth (bound 0), depth n gives bound n+1, and the largest
// depth also reports every depth (bound 0) rather than overflowing to a
// negative bound.
func TestWithPluginChildOperationsDepthOptionCapture(t *testing.T) {
	var unset handlerOptions
	if got := unset.pluginDepthBound(); got != 0 {
		t.Errorf("unset bound = %d, want 0", got)
	}
	for _, depth := range []int{0, 1, 5, math.MaxInt - 1} {
		var o handlerOptions
		WithPluginChildOperationsDepth(depth).applyHandler(&o)
		if got := o.pluginDepthBound(); got != depth+1 {
			t.Errorf("depth %d: bound = %d, want %d", depth, got, depth+1)
		}
	}
	var o handlerOptions
	WithPluginChildOperationsDepth(math.MaxInt).applyHandler(&o)
	if err := validateHandlerOptions(&o); err != nil {
		t.Errorf("depth math.MaxInt: validation error %v, want nil", err)
	}
	if got := o.pluginDepthBound(); got != 0 {
		t.Errorf("depth math.MaxInt: bound = %d, want 0", got)
	}
}

// TestPluginChildOperationsDepthMaxIntReportsEverything asserts that a
// depth of math.MaxInt behaves as the default and reports the whole tree,
// rather than suppressing every operation through an overflowed bound.
func TestPluginChildOperationsDepthMaxIntReportsEverything(t *testing.T) {
	rec, _, _ := runDepthHandler(t, math.MaxInt, depthTreeHandler, nil, invocationSucceeded)
	rec.assertReported(t, []string{"outer", "mid", "inner", "leaf", "top"}, nil)
}

// depthTreeHandler runs a three-level nesting of child contexts whose
// innermost level runs a step, then a top-level step:
//
//	outer (0) > mid (1) > inner (2) > leaf (3); top (0)
func depthTreeHandler(ctx Context, _ string) (string, error) {
	_, err := RunInChildContext(ctx, "outer", func(c Context) (string, error) {
		return RunInChildContext(c, "mid", func(c Context) (string, error) {
			return RunInChildContext(c, "inner", func(c Context) (string, error) {
				return Step(c, "leaf", func(StepContext) (string, error) { return "v", nil })
			})
		})
	})
	if err != nil {
		return "", err
	}
	return Step(ctx, "top", func(StepContext) (string, error) { return "t", nil })
}

// TestPluginChildOperationsDepthChildContexts asserts that a depth bound
// omits every hook of the operations below it in a nested child-context
// tree, that the operations at the bound carry ChildrenOmitted, that the
// default reports the whole tree, and that checkpointing is the same at
// every bound.
func TestPluginChildOperationsDepthChildContexts(t *testing.T) {
	all := []string{"outer", "mid", "inner", "leaf", "top"}
	cases := []struct {
		depth   int
		want    []string
		omitted []string
	}{
		{-1, all, nil},
		{0, []string{"outer", "top"}, []string{"outer", "top"}},
		{1, []string{"outer", "mid", "top"}, []string{"mid"}},
		{2, []string{"outer", "mid", "inner", "top"}, []string{"inner"}},
		{3, all, []string{"leaf"}},
		{4, all, nil},
	}
	wantCalls, wantUpdates := -1, -1
	for _, tc := range cases {
		t.Run(fmt.Sprintf("depth=%d", tc.depth), func(t *testing.T) {
			rec, calls, updates := runDepthHandler(t, tc.depth, depthTreeHandler, nil, invocationSucceeded)
			rec.assertReported(t, tc.want, tc.omitted)
			if wantCalls < 0 {
				wantCalls, wantUpdates = calls, updates
			} else if calls != wantCalls || updates != wantUpdates {
				t.Errorf("checkpoints = %d calls, %d updates; want %d calls, %d updates as without a bound", calls, updates, wantCalls, wantUpdates)
			}
		})
	}
}

// TestPluginChildOperationsDepthOmitsEveryHookKind asserts that an
// operation beyond the bound is omitted from the attempt and wrap hooks as
// well as the start and end hooks, and that an operation at the bound still
// receives all of them.
func TestPluginChildOperationsDepthOmitsEveryHookKind(t *testing.T) {
	rec, _, _ := runDepthHandler(t, 2, depthTreeHandler, nil, invocationSucceeded)
	if got := rec.hooksFor("leaf"); got != nil {
		t.Errorf("leaf hooks = %v, want none", got)
	}
	if got := rec.hooksFor("inner"); !slices.Equal(got, []string{"start", "wrap-child", "end"}) {
		t.Errorf("inner hooks = %v, want start, wrap-child, end", got)
	}
	if got := rec.hooksFor("top"); !slices.Equal(got, []string{"start", "attempt-start", "wrap-attempt", "attempt-end", "end"}) {
		t.Errorf("top hooks = %v, want start, attempt-start, wrap-attempt, attempt-end, end", got)
	}
}

// depthBatchHandler runs a Map named "batch" of two items under opts, each
// item running a step named after its index, inside a child context when
// nested is set:
//
//	[wrap (0) >] batch (0|1) > item-i (1|2) > step-i (2|3)
//
// Under NestingFlat the items have no operation, so the steps are one
// level below the batch.
func depthBatchHandler(nested bool, opts ...BatchOption) func(Context, string) (string, error) {
	batch := func(ctx Context) (string, error) {
		opts := append([]BatchOption{WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) })}, opts...)
		br, err := Map(ctx, "batch", []int{1, 2}, func(c Context, item, idx int) (int, error) {
			return Step(c, fmt.Sprintf("step-%d", idx), func(StepContext) (int, error) { return item, nil })
		}, opts...)
		if err != nil {
			return "", err
		}
		return fmt.Sprint(br.SuccessCount()), nil
	}
	return func(ctx Context, _ string) (string, error) {
		if nested {
			return RunInChildContext(ctx, "wrap", batch)
		}
		return batch(ctx)
	}
}

// TestPluginChildOperationsDepthBatch asserts the depth counting over a
// live Map: items are one below the batch and the steps inside them one
// further, under sequential and concurrent execution, at the root and
// inside a child context. Under NestingFlat the items add no level.
func TestPluginChildOperationsDepthBatch(t *testing.T) {
	items := []string{"item-0", "item-1"}
	steps := []string{"step-0", "step-1"}
	type want struct {
		reported, omitted []string
	}
	byDepth := map[string]map[int]want{
		"nested": {
			0: {[]string{"batch"}, []string{"batch"}},
			1: {append([]string{"batch"}, items...), items},
			2: {slices.Concat([]string{"batch"}, items, steps), steps},
			3: {slices.Concat([]string{"batch"}, items, steps), nil},
		},
		"flat": {
			0: {[]string{"batch"}, []string{"batch"}},
			1: {append([]string{"batch"}, steps...), steps},
			2: {append([]string{"batch"}, steps...), nil},
		},
		"nested-in-child": {
			0: {[]string{"wrap"}, []string{"wrap"}},
			1: {[]string{"wrap", "batch"}, []string{"batch"}},
			2: {append([]string{"wrap", "batch"}, items...), items},
			3: {slices.Concat([]string{"wrap", "batch"}, items, steps), steps},
			4: {slices.Concat([]string{"wrap", "batch"}, items, steps), nil},
		},
	}
	for _, conc := range []int{1, 2} {
		for variant, depths := range byDepth {
			opts := []BatchOption{WithMaxConcurrency(conc)}
			if variant == "flat" {
				opts = append(opts, WithNesting(NestingFlat))
			}
			handler := depthBatchHandler(variant == "nested-in-child", opts...)
			for depth, w := range depths {
				t.Run(fmt.Sprintf("%s/concurrency=%d/depth=%d", variant, conc, depth), func(t *testing.T) {
					rec, _, _ := runDepthHandler(t, depth, handler, nil, invocationSucceeded)
					rec.assertReported(t, w.reported, w.omitted)
				})
			}
		}
	}
}

// TestPluginChildOperationsDepthBatchReplayed asserts the counting holds
// on the replay paths of a batch, where the items run from a context
// minted for the batch: a batch resumed with one item still running, and
// a batch too large to store, replayed from its decision record.
func TestPluginChildOperationsDepthBatchReplayed(t *testing.T) {
	t.Run("resumed", func(t *testing.T) {
		// batch (0) > item-0, item-1 (1) > pause (2). Depth 1 reports the
		// batch and its items in both invocations and never the wait.
		fn := func(ctx Context, _ string) (string, error) {
			br, err := Map(ctx, "batch", []int{1, 2}, func(c Context, item, idx int) (int, error) {
				if idx == 1 {
					if err := Wait(c, "pause", 5e9); err != nil {
						return 0, err
					}
				}
				return item, nil
			}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }), WithMaxConcurrency(1))
			if err != nil {
				return "", err
			}
			return fmt.Sprint(br.SuccessCount()), nil
		}
		first, _, _ := runDepthHandler(t, 1, fn, nil, invocationPending)
		first.assertReported(t, []string{"batch", "item-0", "item-1"}, []string{"item-0", "item-1"})

		resumeOps := []wireOperation{
			lifecycleExecOp(),
			contextOp("1", "", OperationSubTypeMap, "batch", "STARTED", nil),
			contextOp("2", "1", OperationSubTypeMapIteration, "item-0", "SUCCEEDED", &wireContextDetails{Result: "1"}),
			contextOp("3", "1", OperationSubTypeMapIteration, "item-1", "STARTED", nil),
			{Id: hashID("3-1"), ParentId: hashID("3"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
		}
		second, _, _ := runDepthHandler(t, 1, fn, resumeOps, invocationSucceeded)
		second.assertReported(t, []string{"batch", "item-0", "item-1"}, []string{"item-0", "item-1"})
		if got := second.hooksFor("item-1"); !slices.Equal(got, []string{"start", "end"}) {
			t.Errorf("resumed item-1 hooks = %v, want start, end", got)
		}
	})

	t.Run("from-record", func(t *testing.T) {
		fn := func(ctx Context, _ string) (string, error) {
			br, err := Map(ctx, "batch", []int{0, 1, 2}, func(c Context, item, idx int) (string, error) {
				return "unused", nil
			}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }),
				WithCompletion(CompletionConfig{MinSuccessful: 1}))
			if err != nil {
				return "", err
			}
			return fmt.Sprint(br.StartedCount()), nil
		}
		record := `{"completionReason":2,"totalCount":3,"indexSet":"started","indexes":[1,2]}`
		ops := []wireOperation{
			lifecycleExecOp(),
			contextOp("1", "", OperationSubTypeMap, "batch", "SUCCEEDED", &wireContextDetails{Result: record, ReplayChildren: true}),
			contextOp("2", "1", OperationSubTypeMapIteration, "item-0", "SUCCEEDED", &wireContextDetails{Result: `"fast"`}),
			contextOp("3", "1", OperationSubTypeMapIteration, "item-1", "STARTED", nil),
			contextOp("4", "1", OperationSubTypeMapIteration, "item-2", "STARTED", nil),
		}
		rec, _, _ := runDepthHandler(t, 0, fn, ops, invocationSucceeded)
		rec.assertReported(t, []string{"batch"}, []string{"batch"})
		rec, _, _ = runDepthHandler(t, 1, fn, ops, invocationSucceeded)
		rec.assertReported(t, []string{"batch", "item-0", "item-1", "item-2"}, []string{"item-0", "item-1", "item-2"})
	})
}

// TestPluginChildOperationsDepthInvocationMaps asserts that the Operations
// and UpdatedOperations maps of the invocation hooks omit the checkpointed
// operations beyond the bound, computed from their parent chain, and mark
// the operations at the bound with ChildrenOmitted.
func TestPluginChildOperationsDepthInvocationMaps(t *testing.T) {
	ops := []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", OperationSubTypeRunInChildContext, "outer", "STARTED", nil),
		contextOp("1-1", "1", OperationSubTypeRunInChildContext, "mid", "STARTED", nil),
		{Id: hashID("1-1-1"), ParentId: hashID("1-1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
		{Id: hashID("2"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "top"},
	}
	updated := []string{hashID("1-1-1"), hashID("2")}

	type seen struct {
		names   []string
		omitted map[string]bool
	}
	collect := func(m map[string]OperationHookInfo) seen {
		s := seen{omitted: map[string]bool{}}
		for _, info := range m {
			s.names = append(s.names, info.Name)
			s.omitted[info.Name] = info.ChildrenOmitted
		}
		sort.Strings(s.names)
		return s
	}
	for _, tc := range []struct {
		depth              int
		wantAll, wantUpd   []string
		wantOmitted        []string
		wantChangeNotified bool
	}{
		{-1, []string{"", "mid", "outer", "pause", "top"}, []string{"pause", "top"}, nil, true},
		{0, []string{"", "outer", "top"}, []string{"top"}, []string{"", "outer", "top"}, true},
		{1, []string{"", "mid", "outer", "top"}, []string{"top"}, []string{"mid"}, true},
		{2, []string{"", "mid", "outer", "pause", "top"}, []string{"pause", "top"}, []string{"pause"}, true},
	} {
		t.Run(fmt.Sprintf("depth=%d", tc.depth), func(t *testing.T) {
			var mu sync.Mutex
			var all, upd, change seen
			changed := false
			p := Plugin{
				OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
					mu.Lock()
					all, upd = collect(info.Operations), collect(info.UpdatedOperations)
					mu.Unlock()
				},
				OnOperationChange: func(_ context.Context, info OperationChangeHookInfo) {
					mu.Lock()
					changed, change = true, collect(info.UpdatedOperations)
					mu.Unlock()
				},
			}
			opts := []HandlerOption{WithPlugins(p), withLambdaAPI(&fakePluginClient{})}
			if tc.depth >= 0 {
				opts = append(opts, WithPluginChildOperationsDepth(tc.depth))
			}
			handler := Wrap(func(ctx Context, _ string) (string, error) { return "done", nil }, opts...)
			resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:depth", "tok1", ops, updated))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(all.names, tc.wantAll) {
				t.Errorf("Operations = %v, want %v", all.names, tc.wantAll)
			}
			if !slices.Equal(upd.names, tc.wantUpd) {
				t.Errorf("UpdatedOperations = %v, want %v", upd.names, tc.wantUpd)
			}
			if changed != tc.wantChangeNotified {
				t.Errorf("OnOperationChange fired = %v, want %v", changed, tc.wantChangeNotified)
			}
			if changed && !slices.Equal(change.names, tc.wantUpd) {
				t.Errorf("OnOperationChange UpdatedOperations = %v, want %v", change.names, tc.wantUpd)
			}
			for _, n := range all.names {
				if got, want := all.omitted[n], slices.Contains(tc.wantOmitted, n); got != want {
					t.Errorf("Operations[%q].ChildrenOmitted = %v, want %v", n, got, want)
				}
			}
		})
	}
}

// TestPluginChildOperationsDepthOmitsChangeNotification asserts that
// OnOperationChange does not fire when every updated operation lies beyond
// the bound.
func TestPluginChildOperationsDepthOmitsChangeNotification(t *testing.T) {
	ops := []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", OperationSubTypeRunInChildContext, "outer", "STARTED", nil),
		{Id: hashID("1-1"), ParentId: hashID("1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
	}
	fired := false
	p := Plugin{OnOperationChange: func(context.Context, OperationChangeHookInfo) { fired = true }}
	handler := Wrap(func(ctx Context, _ string) (string, error) { return "done", nil },
		WithPlugins(p), withLambdaAPI(&fakePluginClient{}), WithPluginChildOperationsDepth(0))
	resp, err := handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:depth", "tok1", ops, []string{hashID("1-1")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	if fired {
		t.Error("OnOperationChange fired for an operation beyond the bound")
	}
}

// TestCheckpointedOperationDepth asserts the depth walk over the wire
// parent chain, including a parent the state does not hold, and that the
// walk stops at the limit: a chain longer than the limit reports the limit.
func TestCheckpointedOperationDepth(t *testing.T) {
	root := &operation{id: "r"}
	child := &operation{id: "c", parentID: "r"}
	grandchild := &operation{id: "g", parentID: "c"}
	orphan := &operation{id: "o", parentID: "missing"}
	state := newExecutionState([]*operation{root, child, grandchild, orphan})
	for _, tc := range []struct {
		op    *operation
		limit int
		want  int
	}{
		{root, 10, 0}, {child, 10, 1}, {grandchild, 10, 2}, {orphan, 10, 1},
		{grandchild, 2, 2}, {grandchild, 1, 1}, {child, 1, 1}, {root, 1, 0},
	} {
		if got := checkpointedOperationDepth(state, tc.op, tc.limit); got != tc.want {
			t.Errorf("depth(%s, limit %d) = %d, want %d", tc.op.id, tc.limit, got, tc.want)
		}
	}
}

// TestCheckpointedOperationDepthCyclicChain asserts that a cyclic parent
// chain, which only malformed execution state can produce, terminates at
// the limit instead of looping forever, and that the invocation maps then
// omit every operation on the cycle.
func TestCheckpointedOperationDepthCyclicChain(t *testing.T) {
	a := &operation{id: "a", parentID: "b"}
	b := &operation{id: "b", parentID: "a"}
	self := &operation{id: "s", parentID: "s"}
	state := newExecutionState([]*operation{a, b, self})
	for _, op := range []*operation{a, b, self} {
		if got := checkpointedOperationDepth(state, op, 3); got != 3 {
			t.Errorf("depth(%s, limit 3) = %d, want 3", op.id, got)
		}
	}
	m := map[string]OperationHookInfo{}
	for _, op := range []*operation{a, b, self} {
		addCheckpointedOperationInfo(m, "arn:test:depth", state, 3, op)
	}
	if len(m) != 0 {
		t.Errorf("invocation map = %v, want every operation on the cycle omitted", m)
	}
}

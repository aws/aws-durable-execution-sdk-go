package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// virtualChildHandlers returns the blocking and asynchronous handlers under
// test. Each runs one virtual child context named "virtual" and a step
// named "inner" inside it, and returns the child's result. opts are added
// to WithChildVirtual.
func virtualChildHandlers(opts ...ChildOption) map[string]Handler[string, string] {
	opts = append([]ChildOption{WithChildVirtual()}, opts...)
	body := func(c Context) (string, error) {
		return Step(c, "inner", func(StepContext) (string, error) { return "v", nil })
	}
	return map[string]Handler[string, string]{
		"RunInChildContext": func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "virtual", body, opts...)
		},
		"Go": func(ctx Context, _ string) (string, error) {
			return Go(ctx, "virtual", body, opts...).Result()
		},
	}
}

// TestVirtualChildCheckpointsOnlyItsOperations asserts that a virtual
// child context writes no CONTEXT checkpoint of its own: the only updates
// are the step's, numbered under the child's position ("1-1") and
// recording the root (no parent) as ParentId. Plugins see the child's own
// start and end, both IsReplay true because the child records nothing,
// around the step's live events, all with an empty ParentID.
func TestVirtualChildCheckpointsOnlyItsOperations(t *testing.T) {
	for variant, handler := range virtualChildHandlers() {
		t.Run(variant, func(t *testing.T) {
			rec := &opRecorder{}
			fake := &fakeLambda{}
			h := Wrap(handler, WithPlugins(rec.plugin()), withLambdaAPI(fake))

			resp, err := h(context.Background(), childPayload(`"x"`))
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
				t.Fatalf("response = %s, want %s", resp, want)
			}

			updates := updateBatch(t, fake)
			if len(updates) == 0 {
				t.Fatal("no checkpoint updates recorded")
			}
			for _, u := range updates {
				if u.Type == OperationTypeContext {
					t.Errorf("recorded a CONTEXT update (%s %s): a virtual child must not checkpoint itself", aws.ToString(u.Name), u.Action)
				}
				if got, want := aws.ToString(u.Id), hashID("1-1"); got != want {
					t.Errorf("update Id = %q, want the step numbered under the child, %q", got, want)
				}
				if got := aws.ToString(u.ParentId); got != "" {
					t.Errorf("update ParentId = %q, want the root (empty)", got)
				}
			}

			evs := rec.take()
			assertSequence(t, evs,
				"start:virtual:STARTED:true",
				"start:inner:STARTED:false", "end:inner:SUCCEEDED:false",
				"end:virtual:SUCCEEDED:true")
			for _, ev := range eventsForName(evs, "virtual") {
				assertIdentity(t, ev, "1", "virtual", string(OperationTypeContext), OperationSubTypeRunInChildContext, "")
			}
			for _, ev := range eventsForName(evs, "inner") {
				assertIdentity(t, ev, "1-1", "inner", string(OperationTypeStep), OperationSubTypeStep, "")
			}
		})
	}
}

// TestVirtualChildInsideCheckpointedChild asserts that the operations of a
// virtual child nested in a checkpointed child record that child, the
// nearest checkpointed ancestor, as their parent, both in the checkpoint
// and in plugin notifications.
func TestVirtualChildInsideCheckpointedChild(t *testing.T) {
	rec := &opRecorder{}
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			return RunInChildContext(c, "virtual", func(vc Context) (string, error) {
				return Step(vc, "inner", func(StepContext) (string, error) { return "v", nil })
			}, WithChildVirtual())
		})
	}, WithPlugins(rec.plugin()), withLambdaAPI(fake))

	resp, err := h(context.Background(), childPayload(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	contexts := contextUpdates(t, fake)
	if len(contexts) != 2 {
		t.Fatalf("context updates = %d, want the outer child's START and SUCCEED only", len(contexts))
	}
	for _, u := range contexts {
		if got := aws.ToString(u.Name); got != "outer" {
			t.Errorf("context update Name = %q, want outer", got)
		}
	}
	steps := 0
	for _, u := range updateBatch(t, fake) {
		if u.Type != OperationTypeStep {
			continue
		}
		steps++
		if got, want := aws.ToString(u.Id), hashID("1-1-1"); got != want {
			t.Errorf("step Id = %q, want %q", got, want)
		}
		if got, want := aws.ToString(u.ParentId), hashID("1"); got != want {
			t.Errorf("step ParentId = %q, want the outer child %q", got, want)
		}
	}
	if steps == 0 {
		t.Fatal("no STEP update recorded")
	}
	inner := eventsForName(rec.take(), "inner")
	assertSequence(t, inner, "start:inner:STARTED:false", "end:inner:SUCCEEDED:false")
	for _, ev := range inner {
		assertIdentity(t, ev, "1-1-1", "inner", string(OperationTypeStep), OperationSubTypeStep, hashID("1"))
	}
}

// TestVirtualChildFailure asserts a failing virtual child returns a
// ChildContextError built from the body's error, with the mapper applied,
// and checkpoints nothing for the child.
func TestVirtualChildFailure(t *testing.T) {
	boom := errors.New("boom")
	type mapped struct{ error }
	cases := []struct {
		name string
		opts []ChildOption
		want func(err error) bool
	}{
		{"unmapped", nil, func(err error) bool {
			var childErr *ChildContextError
			return errors.As(err, &childErr) && childErr.Message == "boom"
		}},
		{"mapped", []ChildOption{WithChildErrorMapper(func(e *ChildContextError) error { return mapped{e} })}, func(err error) bool {
			var m mapped
			return errors.As(err, &m)
		}},
	}
	for _, tc := range cases {
		for variant, run := range map[string]func(ctx Context, opts ...ChildOption) error{
			"RunInChildContext": func(ctx Context, opts ...ChildOption) error {
				_, err := RunInChildContext(ctx, "virtual", func(Context) (string, error) { return "", boom }, opts...)
				return err
			},
			"Go": func(ctx Context, opts ...ChildOption) error {
				_, err := Go(ctx, "virtual", func(Context) (string, error) { return "", boom }, opts...).Result()
				return err
			},
		} {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				fake := &fakeLambda{}
				h := Wrap(func(ctx Context, _ string) (string, error) {
					err := run(ctx, append([]ChildOption{WithChildVirtual()}, tc.opts...)...)
					if !tc.want(err) {
						return "", fmt.Errorf("unexpected error %T: %v", err, err)
					}
					return "handled", nil
				}, withLambdaAPI(fake))

				resp, err := h(context.Background(), childPayload(`"x"`))
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"handled\""}`; string(resp) != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				for _, u := range updateBatch(t, fake) {
					if u.Type == OperationTypeContext {
						t.Errorf("recorded a CONTEXT update (%s): a failed virtual child must not checkpoint itself", u.Action)
					}
				}
			})
		}
	}
}

// TestVirtualChildRoundTripsResult asserts the child's serdes is applied to
// the result on the first run, as it is for a checkpointed child, even
// though nothing is stored. upperSerdes uppercases on marshal only, so a
// value that passed through it is distinguishable from one that did not.
func TestVirtualChildRoundTripsResult(t *testing.T) {
	for variant, handler := range virtualChildHandlers(WithChildSerdes(upperSerdes{})) {
		t.Run(variant, func(t *testing.T) {
			fake := &fakeLambda{}
			resp := invokeStep(t, fake, stepPayload(`"x"`), handler)
			if want := `{"Status":"SUCCEEDED","Result":"\"V\""}`; resp != want {
				t.Errorf("response = %s, want %s", resp, want)
			}
		})
	}
}

// TestVirtualChildNestedInVirtualChildRejected asserts a virtual child
// inside a virtual child is a configuration error returned before an ID is
// claimed, on both paths, so nothing runs and nothing is checkpointed.
func TestVirtualChildNestedInVirtualChildRejected(t *testing.T) {
	for variant, run := range map[string]func(ctx Context) (string, error){
		"RunInChildContext": func(ctx Context) (string, error) {
			return RunInChildContext(ctx, "inner", func(Context) (string, error) { return "ran", nil }, WithChildVirtual())
		},
		"Go": func(ctx Context) (string, error) {
			return Go(ctx, "inner", func(Context) (string, error) { return "ran", nil }, WithChildVirtual()).Result()
		},
	} {
		t.Run(variant, func(t *testing.T) {
			fake := &fakeLambda{}
			h := Wrap(func(ctx Context, _ string) (string, error) {
				return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
					_, err := run(c)
					if err == nil || !strings.Contains(err.Error(), `child context "inner": WithChildVirtual cannot be used inside a virtual child context`) {
						return "", fmt.Errorf("nested virtual child error = %v, want a configuration error", err)
					}
					// The rejected child claimed no ID: the next operation is "1-1".
					return Step(c, "after", func(StepContext) (string, error) { return "ok", nil })
				}, WithChildVirtual())
			}, withLambdaAPI(fake))

			resp, err := h(context.Background(), childPayload(`"x"`))
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; string(resp) != want {
				t.Fatalf("response = %s, want %s", resp, want)
			}
			for _, u := range updateBatch(t, fake) {
				if got, want := aws.ToString(u.Id), hashID("1-1"); got != want {
					t.Errorf("update Id = %q, want %q: the rejected child must not consume an ID", got, want)
				}
			}
		})
	}
}

// TestVirtualChildInsideFlatBatchItem asserts a virtual child is accepted
// inside a NestingFlat item, another virtual context, and that its
// operations record the batch as parent.
func TestVirtualChildInsideFlatBatchItem(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]int, error) {
		br, err := Map(ctx, "flat", []int{1, 2}, func(c Context, item int, _ int) (int, error) {
			return RunInChildContext(c, "virtual", func(vc Context) (int, error) {
				return Step(vc, "s", func(StepContext) (int, error) { return item, nil })
			}, WithChildVirtual())
		}, WithNesting(NestingFlat))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	if resp.Result != `[1,2]` {
		t.Fatalf("result = %s, want [1,2]", resp.Result)
	}
	stepIDs := map[string]bool{}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext && aws.ToString(u.Name) == "virtual" {
			t.Errorf("recorded a CONTEXT update for the virtual child")
		}
		if u.Type == OperationTypeStep {
			stepIDs[aws.ToString(u.Id)] = true
			if got, want := aws.ToString(u.ParentId), hashID("1"); got != want {
				t.Errorf("step ParentId = %q, want the batch %q", got, want)
			}
		}
	}
	// Item i is "1-<i+1>", the virtual child inside it "1-<i+1>-1", and
	// the step "1-<i+1>-1-1".
	for _, want := range []string{"1-1-1-1", "1-2-1-1"} {
		if !stepIDs[hashID(want)] {
			t.Errorf("no STEP update with Id %q (%s); got %v", want, hashID(want), stepIDs)
		}
	}
	if len(stepIDs) != 2 {
		t.Errorf("distinct step Ids = %d, want 2", len(stepIDs))
	}
}

// TestVirtualChildAtCheckpointedPosition asserts that replaying code that
// declares a virtual child where the checkpoint records an operation is a
// NonDeterministicReplayError, not a silent re-execution.
func TestVirtualChildAtCheckpointedPosition(t *testing.T) {
	for variant, handler := range virtualChildHandlers() {
		t.Run(variant, func(t *testing.T) {
			var got error
			fake := &fakeLambda{}
			h := Wrap(func(ctx Context, in string) (string, error) {
				out, err := handler(ctx, in)
				got = err
				return out, err
			}, withLambdaAPI(fake))
			payload := childPayload(`"x"`,
				contextOp("1", "", OperationSubTypeRunInChildContext, "virtual", "SUCCEEDED", &wireContextDetails{Result: `"v"`}))
			if _, err := h(context.Background(), payload); err != nil {
				t.Fatal(err)
			}
			var nde *NonDeterministicReplayError
			if !errors.As(got, &nde) {
				t.Fatalf("error = %T %v, want a NonDeterministicReplayError", got, got)
			}
			if !strings.Contains(nde.Error(), "declares a virtual child context") {
				t.Errorf("error = %q, want it to name the virtual child context", nde.Error())
			}
		})
	}
}

// TestChildReplayModeSeesChildrenOfVirtualFirstChild asserts a checkpointed
// child whose first operation is a virtual child, so that "id-1" is
// absent, is recognised as replaying whenever an operation recorded under
// it is checkpointed: the virtual child's own first operation "id-1-1",
// or, when the virtual child holds nothing durable, a later sibling.
func TestChildReplayModeSeesChildrenOfVirtualFirstChild(t *testing.T) {
	cases := map[string]string{"inside the virtual child": "1-1-1", "after an empty virtual child": "1-2"}
	for name, opID := range cases {
		t.Run(name, func(t *testing.T) {
			state := newExecutionState([]*operation{{id: hashID(opID), parentID: hashID("1"), status: statusSucceeded}})
			ec := &execContext{Context: context.Background(), state: state}
			if got := childReplayMode(ec, "1", &operation{status: statusStarted}); got != modeReplay {
				t.Errorf("childReplayMode() = %d, want modeReplay (%d)", got, modeReplay)
			}
			if got := childReplayMode(ec, "1", nil); got != modeReplay {
				t.Errorf("childReplayMode(nil op) = %d, want modeReplay (%d)", got, modeReplay)
			}
		})
	}
}

// TestVirtualChildInheritsParentMode asserts a virtual child starts in
// its parent's mode, whatever it is: replay continues across the
// uncheckpointed wrapper until an operation settles it, and a context
// replaying with its result already recorded keeps that mode, so an
// operation inside the child that never completed parks instead of
// re-executing.
func TestVirtualChildInheritsParentMode(t *testing.T) {
	ec := newTestContext(t, nil)
	for _, mode := range []executionMode{modeReplaySucceededContext, modeReplay, modeExecution} {
		ec.mode.Store(int32(mode))
		if got := virtualChildReplayMode(ec); got != mode {
			t.Errorf("virtualChildReplayMode() = %d, want the parent's mode %d", got, mode)
		}
	}
}

// TestPluginChildOperationsDepthVirtualChild asserts a virtual child adds
// no level to the depth WithPluginChildOperationsDepth counts: a step
// inside a virtual child at the root has depth 0, the depth of the virtual
// child itself, and inside a virtual child nested in a checkpointed child
// it has depth 1, the depth of the checkpointed child's own operations.
// The virtual child is reported at its own depth and never carries
// ChildrenOmitted: the step names the enclosing context as its parent, so
// no reported operation is a child of the virtual child at any bound.
func TestPluginChildOperationsDepthVirtualChild(t *testing.T) {
	step := func(c Context) (string, error) {
		return Step(c, "step", func(StepContext) (string, error) { return "v", nil })
	}
	virtual := func(c Context) (string, error) {
		return RunInChildContext(c, "virtual", step, WithChildVirtual())
	}
	type want struct {
		reported, omitted []string // omitted: reported with ChildrenOmitted set
	}
	cases := []struct {
		name    string
		handler func(Context, string) (string, error)
		byDepth map[int]want
	}{
		{"at-root", func(ctx Context, _ string) (string, error) { return virtual(ctx) },
			map[int]want{
				0: {[]string{"virtual", "step"}, []string{"step"}},
				1: {[]string{"virtual", "step"}, nil},
			}},
		{"in-child", func(ctx Context, _ string) (string, error) { return RunInChildContext(ctx, "wrap", virtual) },
			map[int]want{
				0: {[]string{"wrap"}, []string{"wrap"}},
				1: {[]string{"wrap", "virtual", "step"}, []string{"step"}},
				2: {[]string{"wrap", "virtual", "step"}, nil},
			}},
	}
	for _, tc := range cases {
		for depth, w := range tc.byDepth {
			t.Run(fmt.Sprintf("%s/depth=%d", tc.name, depth), func(t *testing.T) {
				rec, _, _ := runDepthHandler(t, depth, tc.handler, nil, invocationSucceeded)
				rec.assertReported(t, w.reported, w.omitted)
			})
		}
	}
}

// virtualLifecycleRuns returns, per path, a function that runs one virtual
// child context named "virtual" with the subtype "Enrich" inside the
// checkpointed child c, with fn as its body, and returns the child's
// outcome. extra options are appended to the child's options.
func virtualLifecycleRuns(extra ...ChildOption) map[string]func(c Context, fn func(Context) (string, error)) (string, error) {
	opts := append([]ChildOption{WithChildVirtual(), WithChildSubType("Enrich")}, extra...)
	return map[string]func(c Context, fn func(Context) (string, error)) (string, error){
		"RunInChildContext": func(c Context, fn func(Context) (string, error)) (string, error) {
			return RunInChildContext(c, "virtual", fn, opts...)
		},
		"Go": func(c Context, fn func(Context) (string, error)) (string, error) {
			return Go(c, "virtual", fn, opts...).Result()
		},
	}
}

// TestVirtualChildLifecycleHooks asserts a virtual child context dispatches
// the operation lifecycle hooks of a checkpointed child, on the blocking
// and the asynchronous path, for a body that succeeds and one that fails:
// a start with the resolved subtype and the enclosing child as ParentID,
// then an end carrying the result or the error returned to the caller.
// Both report IsReplay true, because the child records no checkpoint,
// carry fresh timestamps, and have ChildrenOmitted unset. The step inside
// the child reports the same ParentID, and nothing is checkpointed for the
// child.
func TestVirtualChildLifecycleHooks(t *testing.T) {
	boom := errors.New("boom")
	outcomes := map[string]struct {
		body    func(Context) (string, error)
		wantEnd string
	}{
		"succeeds": {func(c Context) (string, error) {
			return Step(c, "inner", func(StepContext) (string, error) { return "v", nil })
		}, "end:virtual:SUCCEEDED:true"},
		"fails": {func(c Context) (string, error) {
			if _, err := Step(c, "inner", func(StepContext) (string, error) { return "v", nil }); err != nil {
				return "", err
			}
			return "", boom
		}, "end:virtual:FAILED:true"},
	}
	for outcome, oc := range outcomes {
		for variant, run := range virtualLifecycleRuns() {
			t.Run(outcome+"/"+variant, func(t *testing.T) {
				rec := &opRecorder{}
				fake := &fakeLambda{}
				var got error
				h := Wrap(func(ctx Context, _ string) (string, error) {
					return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
						out, err := run(c, oc.body)
						got = err
						if err != nil {
							return "handled", nil
						}
						return out, nil
					})
				}, WithPlugins(rec.plugin()), withLambdaAPI(fake))

				if _, err := h(context.Background(), childPayload(`"x"`)); err != nil {
					t.Fatal(err)
				}
				for _, u := range contextUpdates(t, fake) {
					if aws.ToString(u.Name) != "outer" {
						t.Errorf("recorded a CONTEXT update for %q: a virtual child must not checkpoint itself", aws.ToString(u.Name))
					}
				}

				evs := rec.take()
				assertSequence(t, eventsForName(evs, "virtual"), "start:virtual:STARTED:true", oc.wantEnd)
				for _, ev := range eventsForName(evs, "virtual") {
					assertIdentity(t, ev, "1-1", "virtual", string(OperationTypeContext), "Enrich", hashID("1"))
					assertLiveTimestamps(t, ev)
					if ev.info.ChildrenOmitted {
						t.Errorf("%s ChildrenOmitted = true, want false: no reported operation names the virtual child as parent", ev.hook)
					}
				}
				for _, ev := range eventsForName(evs, "inner") {
					assertIdentity(t, ev, "1-1-1", "inner", string(OperationTypeStep), OperationSubTypeStep, hashID("1"))
				}
				end := eventsForName(evs, "virtual")[1]
				if outcome == "fails" {
					var childErr *ChildContextError
					if !errors.As(got, &childErr) || childErr.Message != "boom" {
						t.Fatalf("caller error = %T %v, want a ChildContextError from boom", got, got)
					}
					if end.info.Error != got { //nolint:errorlint // the end must carry the caller's error itself
						t.Errorf("end Error = %v, want the error returned to the caller %v", end.info.Error, got)
					}
				} else {
					if got != nil {
						t.Fatalf("caller error = %v, want nil", got)
					}
					if end.info.Result != `"v"` || end.info.Error != nil {
						t.Errorf("end Result = %q, Error = %v; want the serialized result and no error", end.info.Result, end.info.Error)
					}
				}
			})
		}
	}
}

// TestVirtualChildStartBeforeWrap asserts, on the blocking and the
// asynchronous path, that the start of a virtual child is dispatched
// before WrapChildContextFn runs and that the wrap hook receives the
// start's info, as for a checkpointed child, so a plugin keyed on the
// start event can correlate the two. The one field that differs is
// IsReplay: the start reports true because the child records nothing,
// while the wrap hook reports the child's mode, false on this first run.
// The context the wrap hook supplies becomes the child's parent context.
func TestVirtualChildStartBeforeWrap(t *testing.T) {
	type key struct{}
	for variant, run := range virtualLifecycleRuns() {
		t.Run(variant, func(t *testing.T) {
			var mu sync.Mutex
			var order []string
			var startInfo, wrapInfo OperationHookInfo
			var seen any
			plugin := Plugin{
				OnOperationStart: func(_ context.Context, info OperationHookInfo) {
					mu.Lock()
					defer mu.Unlock()
					if info.Type == string(OperationTypeContext) && info.Name == "virtual" {
						order = append(order, "start")
						startInfo = info
					}
				},
				WrapChildContextFn: func(ctx context.Context, info OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
					mu.Lock()
					if info.Name == "virtual" {
						order = append(order, "wrap")
						wrapInfo = info
					}
					mu.Unlock()
					return fn(context.WithValue(ctx, key{}, "from-wrap"))
				},
			}
			fake := &fakeLambda{}
			h := Wrap(func(ctx Context, _ string) (string, error) {
				return run(ctx, func(c Context) (string, error) {
					mu.Lock()
					seen = c.Value(key{})
					mu.Unlock()
					return Step(c, "s", func(StepContext) (string, error) { return "v", nil })
				})
			}, WithPlugins(plugin), withLambdaAPI(fake))

			if _, err := h(context.Background(), childPayload(`"x"`)); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(order) != 2 || order[0] != "start" || order[1] != "wrap" {
				t.Fatalf("order = %v, want [start wrap]", order)
			}
			if startInfo.ID != "1" || startInfo.Status != PluginOperationStarted || !startInfo.IsReplay || startInfo.StartTimestamp.IsZero() {
				t.Errorf("start info = %+v, want ID 1, STARTED, IsReplay true, and a start timestamp", startInfo)
			}
			if wrapInfo.IsReplay {
				t.Errorf("wrap IsReplay = true on the first run, want false: the child executes live")
			}
			wantWrap := startInfo
			wantWrap.IsReplay = false
			if wrapInfo != wantWrap {
				t.Errorf("wrap info = %+v, want the start info with IsReplay false: %+v", wrapInfo, wantWrap)
			}
			if seen != "from-wrap" {
				t.Errorf("child context value = %v, want the value the wrap hook attached", seen)
			}
		})
	}
}

// TestVirtualChildSerdesFailureEndsWithError asserts, on the blocking and
// the asynchronous path, that a virtual child context whose result fails
// the serdes round-trip dispatches one failed end carrying the
// [SerdesError] the caller receives, for a Marshal failure and for an
// Unmarshal failure. A virtual child has no checkpoint that could settle
// it, so the end is dispatched from the outcome returned to the caller;
// without it a plugin would observe a start with no end.
func TestVirtualChildSerdesFailureEndsWithError(t *testing.T) {
	cause := errors.New("serdes exploded")
	directions := map[string]failingSerdes{
		"marshal":   {failMarshal: true, cause: cause},
		"unmarshal": {failUnmarshal: true, cause: cause},
	}
	body := func(c Context) (string, error) {
		return Step(c, "inner", func(StepContext) (string, error) { return "v", nil })
	}
	for direction, serdes := range directions {
		for variant, run := range virtualLifecycleRuns(WithChildSerdes(serdes)) {
			t.Run(direction+"/"+variant, func(t *testing.T) {
				rec := &opRecorder{}
				fake := &fakeLambda{}
				var got error
				h := Wrap(func(ctx Context, _ string) (string, error) {
					return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
						_, got = run(c, body)
						return "handled", nil
					})
				}, WithPlugins(rec.plugin()), withLambdaAPI(fake))

				if _, err := h(context.Background(), childPayload(`"x"`)); err != nil {
					t.Fatal(err)
				}
				assertSerdesError(t, got, "virtual", direction, cause)
				for _, u := range contextUpdates(t, fake) {
					if aws.ToString(u.Name) != "outer" {
						t.Errorf("recorded a CONTEXT update for %q: a virtual child must not checkpoint itself", aws.ToString(u.Name))
					}
				}

				evs := eventsForName(rec.take(), "virtual")
				assertSequence(t, evs, "start:virtual:STARTED:true", "end:virtual:FAILED:true")
				end := evs[1]
				assertLiveTimestamps(t, end)
				if end.info.Error != got { //nolint:errorlint // the end must carry the caller's error itself
					t.Errorf("end Error = %v, want the SerdesError returned to the caller %v", end.info.Error, got)
				}
				if end.info.Result != "" {
					t.Errorf("end Result = %q, want empty: the result never round-tripped", end.info.Result)
				}
			})
		}
	}
}

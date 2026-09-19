package durable_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// virtualWaitRun is what the handlers below return: the value the virtual
// child produced, how many times its body ran across all invocations, how
// many times the step inside it ran, and whether the child reported
// replaying on its last run.
type virtualWaitRun struct {
	Value     string `json:"value"`
	BodyRuns  int32  `json:"bodyRuns"`
	StepRuns  int32  `json:"stepRuns"`
	Replaying bool   `json:"replaying"`
}

// virtualWaitHandlers returns handlers that run one virtual child context
// named "virtual" whose body is a step, a Wait that suspends the
// invocation, and a second step, on the blocking and the asynchronous
// path. The counters are shared across invocations of one runner.
func virtualWaitHandlers(bodyRuns, stepRuns *atomic.Int32) map[string]durable.Handler[any, virtualWaitRun] {
	body := func(c durable.Context) (string, error) {
		bodyRuns.Add(1)
		before, err := durable.Step(c, "before", func(durable.StepContext) (string, error) {
			stepRuns.Add(1)
			return "a", nil
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(c, "pause", time.Minute); err != nil {
			return "", err
		}
		after, err := durable.Step(c, "after", func(durable.StepContext) (string, error) {
			stepRuns.Add(1)
			return "b", nil
		})
		if err != nil {
			return "", err
		}
		return before + after, nil
	}
	finish := func(value string, err error) (virtualWaitRun, error) {
		if err != nil {
			return virtualWaitRun{}, err
		}
		return virtualWaitRun{Value: value, BodyRuns: bodyRuns.Load(), StepRuns: stepRuns.Load()}, nil
	}
	return map[string]durable.Handler[any, virtualWaitRun]{
		"RunInChildContext": func(ctx durable.Context, _ any) (virtualWaitRun, error) {
			return finish(durable.RunInChildContext(ctx, "virtual", body, durable.WithChildVirtual()))
		},
		"Go": func(ctx durable.Context, _ any) (virtualWaitRun, error) {
			return finish(durable.Go(ctx, "virtual", body, durable.WithChildVirtual()).Result())
		},
	}
}

// TestVirtualChildSuspendsAndReplays runs a virtual child context whose
// body suspends on a Wait through a full replay cycle: the first
// invocation ends PENDING, the second replays the step from its checkpoint
// and resumes after the wait. The body itself runs on both invocations,
// because nothing is recorded for it; each step body runs once. The
// execution history holds no CONTEXT operation, and every operation inside
// the child is a top-level operation of the execution.
func TestVirtualChildSuspendsAndReplays(t *testing.T) {
	var bodyRuns, stepRuns atomic.Int32
	for variant, handler := range virtualWaitHandlers(&bodyRuns, &stepRuns) {
		t.Run(variant, func(t *testing.T) {
			bodyRuns.Store(0)
			stepRuns.Store(0)
			runner := durabletest.NewLocalRunner(handler)
			result := runner.RunUntilComplete(t, nil)
			if result.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED (error %v)", result.Status, result.Error)
			}
			out, err := durabletest.ResultAs[virtualWaitRun](result)
			if err != nil {
				t.Fatal(err)
			}
			if out.Value != "ab" {
				t.Errorf("value = %q, want ab", out.Value)
			}
			if got := len(result.Invocations); got != 2 {
				t.Errorf("invocations = %d, want 2: one that suspends on the wait and one that resumes", got)
			}
			if out.BodyRuns != 2 {
				t.Errorf("body runs = %d, want 2: a virtual child re-runs its body on every invocation", out.BodyRuns)
			}
			if out.StepRuns != 2 {
				t.Errorf("step runs = %d, want 2: each step body runs once across the cycle", out.StepRuns)
			}
			if ctxOps := result.OperationsByType("CONTEXT"); len(ctxOps) != 0 {
				t.Errorf("CONTEXT operations = %+v, want none for a virtual child", ctxOps)
			}
			for _, name := range []string{"before", "pause", "after"} {
				op := result.Operation(name)
				if op == nil {
					t.Errorf("operation %q not recorded", name)
					continue
				}
				if op.ParentID != "" {
					t.Errorf("operation %q ParentID = %q, want empty: the root is the nearest checkpointed ancestor", name, op.ParentID)
				}
				if op.Status != "SUCCEEDED" {
					t.Errorf("operation %q Status = %q, want SUCCEEDED", name, op.Status)
				}
			}
		})
	}
}

// TestVirtualChildFirstInCheckpointedChildReplays asserts a checkpointed
// child whose first operation is a virtual child replays correctly across
// a suspension: the outer child reports IsReplaying on the resuming
// invocation, and the step after the virtual child is not re-executed.
func TestVirtualChildFirstInCheckpointedChildReplays(t *testing.T) {
	var outerRuns, tailRuns atomic.Int32
	runner := durabletest.NewLocalRunner(func(ctx durable.Context, _ any) (virtualWaitRun, error) {
		return durable.RunInChildContext(ctx, "outer", func(c durable.Context) (virtualWaitRun, error) {
			outerRuns.Add(1)
			replaying := c.IsReplaying()
			v, err := durable.RunInChildContext(c, "virtual", func(vc durable.Context) (string, error) {
				if err := durable.Wait(vc, "pause", time.Minute); err != nil {
					return "", err
				}
				return "v", nil
			}, durable.WithChildVirtual())
			if err != nil {
				return virtualWaitRun{}, err
			}
			tail, err := durable.Step(c, "tail", func(durable.StepContext) (string, error) {
				tailRuns.Add(1)
				return "t", nil
			})
			if err != nil {
				return virtualWaitRun{}, err
			}
			return virtualWaitRun{Value: v + tail, BodyRuns: outerRuns.Load(), StepRuns: tailRuns.Load(), Replaying: replaying}, nil
		})
	})
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %v)", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[virtualWaitRun](result)
	if err != nil {
		t.Fatal(err)
	}
	if out.Value != "vt" || out.BodyRuns != 2 || out.StepRuns != 1 {
		t.Errorf("result = %+v, want value vt, 2 outer runs, 1 tail run", out)
	}
	if !out.Replaying {
		t.Errorf("outer child IsReplaying = false on the resuming invocation, want true: its first checkpoint is the wait inside the virtual child")
	}
	outer := result.Operation("outer")
	if outer == nil {
		t.Fatal("outer child not recorded")
	}
	for _, name := range []string{"pause", "tail"} {
		op := result.Operation(name)
		if op == nil {
			t.Fatalf("operation %q not recorded", name)
		}
		if op.ParentID != outer.ID {
			t.Errorf("operation %q ParentID = %q, want the outer child %q", name, op.ParentID, outer.ID)
		}
	}
	if virt := result.Operation("virtual"); virt != nil {
		t.Errorf("virtual child recorded as %+v, want no operation", *virt)
	}
}

// emptyVirtualRun is what the handlers below return: whether the enclosing
// context reported replaying right after an empty virtual child returned
// on the handler's last run, whether the empty virtual child's own context
// reported replaying on that run, how many times the enclosing body ran,
// and how many times the step after the virtual child ran.
type emptyVirtualRun struct {
	ReplayingAfter  bool  `json:"replayingAfter"`
	ReplayingInside bool  `json:"replayingInside"`
	BodyRuns        int32 `json:"bodyRuns"`
	StepRuns        int32 `json:"stepRuns"`
}

// emptyVirtualBody runs, on c: a virtual child named "empty" with no
// durable operation inside it, on the blocking or the asynchronous path;
// then a step; then a Wait that suspends the invocation. It reports the
// replay state c and the virtual child observed.
func emptyVirtualBody(variant string, bodyRuns, stepRuns *atomic.Int32) func(c durable.Context) (emptyVirtualRun, error) {
	return func(c durable.Context) (emptyVirtualRun, error) {
		bodyRuns.Add(1)
		var inside bool
		empty := func(vc durable.Context) (string, error) {
			inside = vc.IsReplaying()
			return "e", nil
		}
		var err error
		if variant == "Go" {
			_, err = durable.Go(c, "empty", empty, durable.WithChildVirtual()).Result()
		} else {
			_, err = durable.RunInChildContext(c, "empty", empty, durable.WithChildVirtual())
		}
		if err != nil {
			return emptyVirtualRun{}, err
		}
		after := c.IsReplaying()
		if _, err := durable.Step(c, "tail", func(durable.StepContext) (string, error) {
			stepRuns.Add(1)
			return "t", nil
		}); err != nil {
			return emptyVirtualRun{}, err
		}
		if err := durable.Wait(c, "pause", time.Minute); err != nil {
			return emptyVirtualRun{}, err
		}
		return emptyVirtualRun{ReplayingAfter: after, ReplayingInside: inside, BodyRuns: bodyRuns.Load(), StepRuns: stepRuns.Load()}, nil
	}
}

// TestEmptyVirtualChildPreservesReplay asserts an empty virtual child
// context does not end replay early. On the resuming invocation the step
// after it and the wait are checkpointed, so the enclosing context is
// replaying when it reaches the virtual child; nothing is recorded for the
// child, and the enclosing context must still report replaying after it
// returns, until the checkpointed step confirms it. The virtual child
// itself inherits that state. The step is not re-executed. Covered at the
// root and inside a checkpointed child re-entered as STARTED, on both
// paths.
func TestEmptyVirtualChildPreservesReplay(t *testing.T) {
	for _, variant := range []string{"RunInChildContext", "Go"} {
		for _, enclosing := range []string{"root", "child"} {
			t.Run(enclosing+"/"+variant, func(t *testing.T) {
				var bodyRuns, stepRuns atomic.Int32
				body := emptyVirtualBody(variant, &bodyRuns, &stepRuns)
				handler := func(ctx durable.Context, _ any) (emptyVirtualRun, error) {
					if enclosing == "child" {
						return durable.RunInChildContext(ctx, "outer", body)
					}
					return body(ctx)
				}
				runner := durabletest.NewLocalRunner(handler)
				result := runner.RunUntilComplete(t, nil)
				if result.Status != durabletest.Succeeded {
					t.Fatalf("status = %s, want SUCCEEDED (error %v)", result.Status, result.Error)
				}
				out, err := durabletest.ResultAs[emptyVirtualRun](result)
				if err != nil {
					t.Fatal(err)
				}
				if got := len(result.Invocations); got != 2 {
					t.Fatalf("invocations = %d, want 2: one that suspends on the wait and one that resumes", got)
				}
				if out.BodyRuns != 2 || out.StepRuns != 1 {
					t.Errorf("body runs = %d, step runs = %d; want 2 and 1: the step replays from its checkpoint", out.BodyRuns, out.StepRuns)
				}
				if !out.ReplayingAfter {
					t.Errorf("IsReplaying = false after the empty virtual child on the resuming invocation, want true: the step after it is checkpointed")
				}
				if !out.ReplayingInside {
					t.Errorf("IsReplaying = false inside the empty virtual child on the resuming invocation, want true: it inherits the enclosing context's replay state")
				}
				if virt := result.Operation("empty"); virt != nil {
					t.Errorf("virtual child recorded as %+v, want no operation", *virt)
				}
			})
		}
	}
}

// virtualHookEvent is one plugin hook call the virtual child under test
// dispatched: the hook name, the invocation it happened in (1-based), and
// the info it carried.
type virtualHookEvent struct {
	hook       string
	invocation int
	info       durable.OperationHookInfo
}

// virtualHookRecorder records the start, wrap, and end hooks of the
// operation named "virtual" across the invocations of one runner.
type virtualHookRecorder struct {
	mu         sync.Mutex
	invocation int
	events     []virtualHookEvent
}

func (r *virtualHookRecorder) record(hook string, info durable.OperationHookInfo) {
	if info.Name != "virtual" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, virtualHookEvent{hook: hook, invocation: r.invocation, info: info})
}

func (r *virtualHookRecorder) plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(context.Context, durable.InvocationHookInfo) {
			r.mu.Lock()
			r.invocation++
			r.mu.Unlock()
		},
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) { r.record("start", info) },
		OnOperationEnd:   func(_ context.Context, info durable.OperationHookInfo) { r.record("end", info) },
		WrapChildContextFn: func(ctx context.Context, info durable.OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			r.record("wrap", info)
			return fn(ctx)
		},
	}
}

// summary renders the recorded events as "invocation:hook:status:replay".
func (r *virtualHookRecorder) summary() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.events))
	for _, ev := range r.events {
		out = append(out, fmt.Sprintf("%d:%s:%s:%v", ev.invocation, ev.hook, ev.info.Status, ev.info.IsReplay))
	}
	return out
}

// TestVirtualChildHookReplayFlags asserts the IsReplay each plugin hook of
// a virtual child reports across a replay cycle, on both paths. The child
// records no checkpoint, so its start and end report IsReplay true on
// every invocation. WrapChildContextFn reports the mode the child runs
// in: false on the first invocation, where the child executes live and
// then suspends on the wait, so no end follows; true on the resuming
// invocation, where the child replays the operations inside it. Every
// event carries the same identity as the start it belongs to.
func TestVirtualChildHookReplayFlags(t *testing.T) {
	var bodyRuns, stepRuns atomic.Int32
	for variant, handler := range virtualWaitHandlers(&bodyRuns, &stepRuns) {
		t.Run(variant, func(t *testing.T) {
			rec := &virtualHookRecorder{}
			runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(rec.plugin()))
			result := runner.RunUntilComplete(t, nil)
			if result.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED (error %v)", result.Status, result.Error)
			}
			if got := len(result.Invocations); got != 2 {
				t.Fatalf("invocations = %d, want 2", got)
			}
			want := []string{
				"1:start:STARTED:true",
				"1:wrap:STARTED:false",
				"2:start:STARTED:true",
				"2:wrap:STARTED:true",
				"2:end:SUCCEEDED:true",
			}
			got := rec.summary()
			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("hook events = %v, want %v", got, want)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			for _, ev := range rec.events {
				if ev.info.ID != "1" || ev.info.Type != "CONTEXT" || ev.info.SubType != durable.OperationSubTypeRunInChildContext || ev.info.ParentID != "" {
					t.Errorf("%d:%s identity = %+v, want ID 1, CONTEXT/%s, empty ParentID", ev.invocation, ev.hook, ev.info, durable.OperationSubTypeRunInChildContext)
				}
				if ev.info.StartTimestamp.IsZero() {
					t.Errorf("%d:%s StartTimestamp is zero", ev.invocation, ev.hook)
				}
				if ev.hook == "end" && ev.info.EndTimestamp.Before(ev.info.StartTimestamp) {
					t.Errorf("end EndTimestamp %v before StartTimestamp %v", ev.info.EndTimestamp, ev.info.StartTimestamp)
				}
				if ev.hook == "end" && ev.info.Result != `"ab"` {
					t.Errorf("end Result = %q, want the serialized child result", ev.info.Result)
				}
			}
		})
	}
}

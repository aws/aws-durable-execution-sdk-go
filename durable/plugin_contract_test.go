package durable

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

// hookCounter is a plugin that implements every hook and counts each
// dispatch. The counts are guarded by one mutex, which is the
// synchronization the Plugin contract requires of a hook that mutates
// shared state. The notification and enrichment hooks ignore their ctx and
// the wrap hooks pass theirs to fn unchanged; the context each wrap hook
// threads into user code is the subject of plugin_wrap_context_test.go.
//
// It also checks the sequencing the Concurrency section of Plugin promises
// per plugin: one plugin's invocation-level hooks never overlap one another
// within an invocation, and its hooks for one operation never overlap one
// another. Every hook but EnrichLogContext, which may overlap anything,
// marks its scope active on entry and inactive on exit; an entry that finds
// its scope already active is recorded in overlaps.
type hookCounter struct {
	mu     sync.Mutex
	counts map[string]int
	// replay counts the operation-level dispatches with IsReplay set.
	replay map[string]int
	// active holds the scopes with a hook in progress: the invocation
	// scope for invocation-level hooks, an operation ID for the rest.
	active map[string]string
	// overlaps records each hook that entered a scope another hook of
	// this plugin still held, as "hook over other".
	overlaps []string
}

// invocationScope is the scope key of the invocation-level hooks.
const invocationScope = "invocation"

func newHookCounter() *hookCounter {
	return &hookCounter{counts: map[string]int{}, replay: map[string]int{}, active: map[string]string{}}
}

// enter marks scope active for hook and returns the function that marks
// it inactive again. The caller defers the result around the hook's work.
func (h *hookCounter) enter(hook, scope string) func() {
	h.mu.Lock()
	if other, busy := h.active[scope]; busy {
		h.overlaps = append(h.overlaps, hook+" over "+other)
	}
	h.active[scope] = hook
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.active, scope)
		h.mu.Unlock()
	}
}

func (h *hookCounter) hit(hook string) {
	h.mu.Lock()
	h.counts[hook]++
	h.mu.Unlock()
}

func (h *hookCounter) hitOp(hook string, info OperationHookInfo) {
	h.mu.Lock()
	h.counts[hook]++
	if info.IsReplay {
		h.replay[hook]++
	}
	h.mu.Unlock()
}

func (h *hookCounter) count(hook string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[hook]
}

func (h *hookCounter) plugin() Plugin {
	return Plugin{
		OnInvocationStart: func(_ context.Context, _ InvocationHookInfo) {
			defer h.enter("OnInvocationStart", invocationScope)()
			h.hit("OnInvocationStart")
		},
		OnInvocationEnd: func(_ context.Context, _ InvocationEndHookInfo) {
			defer h.enter("OnInvocationEnd", invocationScope)()
			h.hit("OnInvocationEnd")
		},
		OnOperationStart: func(_ context.Context, info OperationHookInfo) {
			defer h.enter("OnOperationStart", info.ID)()
			h.hitOp("OnOperationStart", info)
		},
		OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			defer h.enter("OnOperationEnd", info.ID)()
			h.hitOp("OnOperationEnd", info)
		},
		OnOperationAttemptStart: func(_ context.Context, info AttemptHookInfo) {
			defer h.enter("OnOperationAttemptStart", info.ID)()
			h.hitOp("OnOperationAttemptStart", info.OperationHookInfo)
		},
		OnOperationAttemptEnd: func(_ context.Context, info AttemptEndHookInfo) {
			defer h.enter("OnOperationAttemptEnd", info.ID)()
			h.hitOp("OnOperationAttemptEnd", info.OperationHookInfo)
		},
		OnOperationChange: func(_ context.Context, _ OperationChangeHookInfo) {
			defer h.enter("OnOperationChange", invocationScope)()
			h.hit("OnOperationChange")
		},
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			defer h.enter("WrapInvocation", invocationScope)()
			h.hit("WrapInvocation")
			return fn(ctx)
		},
		WrapOperationAttemptFn: func(ctx context.Context, info AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			defer h.enter("WrapOperationAttemptFn", info.ID)()
			h.hitOp("WrapOperationAttemptFn", info.OperationHookInfo)
			return fn(ctx)
		},
		WrapChildContextFn: func(ctx context.Context, info OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			defer h.enter("WrapChildContextFn", info.ID)()
			h.hitOp("WrapChildContextFn", info)
			return fn(ctx)
		},
		EnrichLogContext: func(_ context.Context) map[string]any {
			h.hit("EnrichLogContext")
			return map[string]any{"hook": "enrich"}
		},
	}
}

// allHooks lists every hook field of Plugin. The test below fails when a
// hook is added to Plugin without being added here, so the list stays
// complete: the reflection check compares it against the type.
var allHooks = []string{
	"OnInvocationStart",
	"OnInvocationEnd",
	"OnOperationStart",
	"OnOperationEnd",
	"OnOperationAttemptStart",
	"OnOperationAttemptEnd",
	"OnOperationChange",
	"WrapInvocation",
	"WrapOperationAttemptFn",
	"WrapChildContextFn",
	"EnrichLogContext",
}

// TestPluginEveryHookFiresFromConcurrentBranches drives one execution that
// dispatches every hook of the Plugin type from concurrent branches and
// runs it with two registered plugins, so the multi-plugin dispatch path
// fans each notification out to its own goroutines. It is the race-detector
// verification of the concurrency contract on Plugin: hooks of concurrent
// operations run in parallel, a plugin that guards its own state with a
// mutex observes every dispatch exactly once, and one plugin's hooks for
// one invocation or one operation never overlap one another.
//
// The first invocation suspends on a wait. The second invocation resumes
// with the wait completed and listed as updated, so OnOperationChange
// fires, then runs a four-item Map at concurrency four. Each item runs a
// child context around a step that logs, so the item's goroutine dispatches
// the child wrap hook, the attempt hooks, the attempt wrap hook, the
// operation start and end hooks, and the log enrichment hook while the
// other items do the same.
func TestPluginEveryHookFiresFromConcurrentBranches(t *testing.T) {
	const items = 4

	counters := []*hookCounter{newHookCounter(), newHookCounter()}
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		if err := Wait(ctx, "pause", 5*time.Second); err != nil {
			return 0, err
		}
		br, err := Map(ctx, "batch", []int{1, 2, 3, 4}, func(c Context, item, idx int) (int, error) {
			return RunInChildContext(c, fmt.Sprintf("child-%d", idx), func(cc Context) (int, error) {
				return Step(cc, fmt.Sprintf("step-%d", idx), func(sc StepContext) (int, error) {
					sc.Logger().Info("working", "item", item)
					return item * 2, nil
				})
			})
		}, WithMaxConcurrency(items))
		if err != nil {
			return 0, err
		}
		sum := 0
		for _, r := range br.Results() {
			sum += r
		}
		return sum, nil
	},
		WithPlugins(counters[0].plugin(), counters[1].plugin()),
		WithLogHandler(slog.NewTextHandler(io.Discard, nil)),
		withLambdaAPI(&fakePluginClient{}),
	)

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:all-hooks", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationPending)

	resumeOps := []wireOperation{
		lifecycleExecOp(),
		{Id: hashID("1"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause"},
	}
	resp, err = handler(makePluginContext(), makePluginPayloadWithUpdated(t, "arn:test:all-hooks", "tok2", resumeOps, []string{hashID("1")}))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)

	assertPluginHookFieldsListed(t)

	for i, c := range counters {
		if len(c.overlaps) != 0 {
			t.Errorf("plugin %d: hooks of one scope overlapped: %v", i, c.overlaps)
		}
		for _, hook := range allHooks {
			if c.count(hook) == 0 {
				t.Errorf("plugin %d: hook %s never fired", i, hook)
			}
		}
		// Per-invocation hooks fire exactly once per invocation, twice
		// over the two invocations. OnOperationChange fires only on the
		// second invocation, the one with an updated operation.
		for hook, want := range map[string]int{
			"OnInvocationStart": 2,
			"OnInvocationEnd":   2,
			"WrapInvocation":    2,
			"OnOperationChange": 1,
		} {
			if got := c.count(hook); got != want {
				t.Errorf("plugin %d: %s fired %d times, want %d", i, hook, got, want)
			}
		}
		// Each item runs one child context with one step attempt, so the
		// per-item hooks fire once per item. The wait suspended before the
		// Map, so the Map runs live in the second invocation and none of
		// these dispatches is a replay.
		for _, hook := range []string{"WrapChildContextFn", "OnOperationAttemptStart", "OnOperationAttemptEnd", "WrapOperationAttemptFn"} {
			if got := c.count(hook); got != items {
				t.Errorf("plugin %d: %s fired %d times, want %d", i, hook, got, items)
			}
			if got := c.replay[hook]; got != 0 {
				t.Errorf("plugin %d: %s fired %d times with IsReplay, want 0", i, hook, got)
			}
		}
		// Operation starts: the live wait in the first invocation; then
		// the batch, four items, four children, and four steps in the
		// second. Ends: the same, with the wait's end replayed in the
		// second invocation.
		wantOps := 1 + 1 + items + items + items
		if got := c.count("OnOperationStart"); got != wantOps {
			t.Errorf("plugin %d: OnOperationStart fired %d times, want %d", i, got, wantOps)
		}
		if got := c.count("OnOperationEnd"); got != wantOps {
			t.Errorf("plugin %d: OnOperationEnd fired %d times, want %d", i, got, wantOps)
		}
		if got := c.replay["OnOperationEnd"]; got != 1 {
			t.Errorf("plugin %d: OnOperationEnd fired %d times with IsReplay, want 1 (the wait)", i, got)
		}
		// One log record per step, each enriched once.
		if got := c.count("EnrichLogContext"); got != items {
			t.Errorf("plugin %d: EnrichLogContext fired %d times, want %d", i, got, items)
		}
	}
}

// assertPluginHookFieldsListed fails when the exported fields of Plugin
// differ from allHooks, so a hook added to the type must be added to the
// contract test above and be exercised by it.
func assertPluginHookFieldsListed(t *testing.T) {
	t.Helper()
	typ := reflect.TypeFor[Plugin]()
	var fields []string
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.IsExported() {
			fields = append(fields, f.Name)
		}
	}
	slices.Sort(fields)
	want := slices.Clone(allHooks)
	slices.Sort(want)
	if !slices.Equal(fields, want) {
		t.Fatalf("Plugin hook fields = %v, but the contract test covers %v; add the new hook to allHooks and to hookCounter.plugin", fields, want)
	}
}

// TestPluginNotificationFanOutRunsPluginsInParallel verifies the
// multi-plugin dispatch path the Dispatch section of Plugin describes: with
// several plugins registered, one notification runs each plugin's hook on a
// goroutine of its own, and the dispatch returns only after every hook has
// returned. Each hook announces its entry and then waits for the other's
// announcement, so the dispatch completes only if both hooks are in flight
// at once; a sequential dispatch would time out instead of deadlocking.
func TestPluginNotificationFanOutRunsPluginsInParallel(t *testing.T) {
	entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var returned sync.WaitGroup
	hook := func(i int) func(context.Context, InvocationHookInfo) {
		return func(context.Context, InvocationHookInfo) {
			defer returned.Done()
			close(entered[i])
			select {
			case <-entered[1-i]:
			case <-time.After(5 * time.Second):
				t.Errorf("plugin %d: the other plugin's hook never started while this one was running", i)
			}
		}
	}
	d := newPluginDispatcher([]Plugin{{OnInvocationStart: hook(0)}, {OnInvocationStart: hook(1)}})
	returned.Add(2)
	dispatchNotification(d, func(p *Plugin) { p.OnInvocationStart(t.Context(), InvocationHookInfo{}) })
	// Both hooks have returned by the time dispatchNotification returns.
	// Wait reports that without blocking; a hook still running would
	// mean the dispatch did not join it.
	done := make(chan struct{})
	go func() { returned.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatchNotification returned before every hook had returned")
	}
}

// TestEnrichLogContextFollowsReplayLogMode verifies the replay behavior the
// EnrichLogContext documentation states. The execution replays one
// checkpointed step, so the handler's first record is written while
// replaying and its second while live. Under the default suppress mode the
// hook runs for the live record only. Under ReplayLogModeEmit the replayed
// record is emitted and the hook runs for it too.
func TestEnrichLogContextFollowsReplayLogMode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []HandlerOption
		calls int
	}{
		{name: "suppress", calls: 1},
		{name: "emit", opts: []HandlerOption{WithReplayLogMode(ReplayLogModeEmit)}, calls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			plugin := Plugin{EnrichLogContext: func(context.Context) map[string]any {
				mu.Lock()
				calls++
				mu.Unlock()
				return map[string]any{"enriched": true}
			}}
			rec := newRecordingHandler()
			opts := append([]HandlerOption{withLambdaAPI(&fakeLambda{}), WithLogHandler(rec), WithPlugins(plugin)}, tc.opts...)
			h := Wrap(func(ctx Context, _ string) (string, error) {
				ctx.Logger().Info("replaying")
				if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
					return "", err
				}
				if _, err := Step(ctx, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
					return "", err
				}
				ctx.Logger().Info("live")
				return "ok", nil
			}, opts...)
			if _, err := h(t.Context(), replayedStepPayload()); err != nil {
				t.Fatalf("Invoke() error: %v", err)
			}
			mu.Lock()
			got := calls
			mu.Unlock()
			if got != tc.calls {
				t.Errorf("EnrichLogContext ran %d times, want %d (records emitted: %v)", got, tc.calls, rec.messages())
			}
			for _, r := range rec.all() {
				if r.attrs["enriched"] != true {
					t.Errorf("record %q attrs = %v, want the plugin field on every emitted record", r.message, r.attrs)
				}
			}
			if len(rec.all()) != tc.calls {
				t.Errorf("emitted %d records %v, want %d", len(rec.all()), rec.messages(), tc.calls)
			}
		})
	}
}

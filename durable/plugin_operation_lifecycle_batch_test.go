package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// batchItemEvents returns the events whose ParentID is the wire ID of the
// batch parentID, grouped by item name, in dispatch order.
func batchItemEvents(evs []opEvent, parentID string) map[string][]opEvent {
	out := map[string][]opEvent{}
	for _, ev := range evs {
		if ev.info.ParentID == hashID(parentID) {
			out[ev.info.Name] = append(out[ev.info.Name], ev)
		}
	}
	return out
}

// mapLifecycleHandler returns a handler running a three-item Map named
// "batch" at the root, with item names from WithItemNamer, under the given
// options. Each item runs one step named after its index and returns its
// item doubled.
func mapLifecycleHandler(rec *opRecorder, opts ...BatchOption) func(context.Context, []byte) ([]byte, error) {
	return Wrap(func(ctx Context, _ string) (int, error) {
		opts := append([]BatchOption{WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) })}, opts...)
		br, err := Map(ctx, "batch", []int{1, 2, 3}, func(c Context, item, idx int) (int, error) {
			return Step(c, fmt.Sprintf("step-%d", idx), func(StepContext) (int, error) { return item * 2, nil })
		}, opts...)
		if err != nil {
			return 0, err
		}
		sum := 0
		for _, r := range br.Results() {
			sum += r
		}
		return sum, nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))
}

// TestOperationLifecycleMapLive asserts the events of a live Map in NORMAL
// nesting, sequentially and under concurrency. The batch dispatches STARTED
// first and SUCCEEDED last. Each item dispatches STARTED before its body and
// SUCCEEDED after its checkpoint, with the item name from WithItemNamer and
// the batch as ParentID; the step inside each item names the item as
// ParentID. The concurrent variant runs under the race detector with three
// items dispatching events from their own goroutines.
func TestOperationLifecycleMapLive(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []BatchOption
	}{
		{"sequential", []BatchOption{WithMaxConcurrency(1)}},
		{"concurrent", []BatchOption{WithMaxConcurrency(3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &opRecorder{}
			handler := mapLifecycleHandler(rec, tc.opts...)
			resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
			if err != nil {
				t.Fatal(err)
			}
			assertPluginResponseStatus(t, resp, invocationSucceeded)
			evs := rec.take()

			batch := eventsForName(evs, "batch")
			assertSequence(t, batch, "start:batch:STARTED:false", "end:batch:SUCCEEDED:false")
			for _, ev := range batch {
				assertIdentity(t, ev, "1", "batch", string(OperationTypeContext), operationSubTypeMap, "")
				assertLiveTimestamps(t, ev)
			}
			if batch[1].info.Result == "" {
				t.Error("batch end: Result is empty, want the checkpointed payload")
			}
			if summarize(evs[:1])[0] != "start:batch:STARTED:false" {
				t.Errorf("first event = %v, want the batch start", summarize(evs[:1]))
			}
			if last := summarize(evs[len(evs)-1:])[0]; last != "end:batch:SUCCEEDED:false" {
				t.Errorf("last event = %v, want the batch end", last)
			}

			// NORMAL items are claimed from the root after the batch: "2",
			// "3", "4". Their events name the batch as ParentID.
			items := batchItemEvents(evs, "1")
			if len(items) != 3 {
				t.Fatalf("item event groups = %v, want 3", summarize(evs))
			}
			for i := range 3 {
				name := fmt.Sprintf("item-%d", i)
				id := fmt.Sprint(i + 2)
				assertSequence(t, items[name], "start:"+name+":STARTED:false", "end:"+name+":SUCCEEDED:false")
				for _, ev := range items[name] {
					assertIdentity(t, ev, id, name, string(OperationTypeContext), operationSubTypeMapIteration, hashID("1"))
					assertLiveTimestamps(t, ev)
				}
				if want := fmt.Sprint((i + 1) * 2); items[name][1].info.Result != want {
					t.Errorf("%s end Result = %q, want %q", name, items[name][1].info.Result, want)
				}
				// The step inside the item names the item as ParentID and
				// runs between the item's start and end.
				step := eventsForName(evs, fmt.Sprintf("step-%d", i))
				assertSequence(t, step, fmt.Sprintf("start:step-%d:STARTED:false", i), fmt.Sprintf("end:step-%d:SUCCEEDED:false", i))
				assertIdentity(t, step[0], id+"-1", fmt.Sprintf("step-%d", i), string(OperationTypeStep), operationSubTypeStep, hashID(id))
				assertEventsOrdered(t, evs, items[name][0], step[0], step[1], items[name][1])
			}
		})
	}
}

// assertEventsOrdered checks that want appear in evs in the given relative order.
func assertEventsOrdered(t *testing.T, evs []opEvent, want ...opEvent) {
	t.Helper()
	pos := func(w opEvent) int {
		for i, ev := range evs {
			if ev.hook == w.hook && ev.info.ID == w.info.ID && ev.info.Status == w.info.Status {
				return i
			}
		}
		t.Fatalf("event %s:%s:%s not found", w.hook, w.info.ID, w.info.Status)
		return -1
	}
	for i := 1; i < len(want); i++ {
		if pos(want[i-1]) >= pos(want[i]) {
			t.Errorf("event %s:%s dispatched after %s:%s; sequence %v", want[i-1].hook, want[i-1].info.ID, want[i].hook, want[i].info.ID, summarize(evs))
		}
	}
}

// TestOperationLifecycleParallelBranchNames asserts Parallel branches
// dispatch events named by Branch.Name with the batch as ParentID, and that
// a failed branch dispatches a FAILED end carrying its error while the batch
// end reports its SUCCEEDED checkpoint.
func TestOperationLifecycleParallelBranchNames(t *testing.T) {
	rec := &opRecorder{}
	errBoom := errors.New("boom")
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		br, err := Parallel(ctx, "par", []Branch[string]{
			{Name: "alpha", Func: func(Context) (string, error) { return "a", nil }},
			{Name: "beta", Func: func(Context) (string, error) { return "", errBoom }},
		}, WithMaxConcurrency(1))
		if err != nil && !br.HasFailure() {
			return "", err
		}
		return fmt.Sprintf("%d/%d", br.SuccessCount(), br.FailureCount()), nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	assertSequence(t, evs,
		"start:par:STARTED:false",
		"start:alpha:STARTED:false",
		"end:alpha:SUCCEEDED:false",
		"start:beta:STARTED:false",
		"end:beta:FAILED:false",
		"end:par:SUCCEEDED:false",
	)
	assertIdentity(t, evs[1], "2", "alpha", string(OperationTypeContext), operationSubTypeParallelBranch, hashID("1"))
	assertIdentity(t, evs[3], "3", "beta", string(OperationTypeContext), operationSubTypeParallelBranch, hashID("1"))
	if evs[2].info.Result != `"a"` {
		t.Errorf("alpha end Result = %q, want %q", evs[2].info.Result, `"a"`)
	}
	var cerr *ChildContextError
	if !errors.As(evs[4].info.Error, &cerr) || !strings.Contains(evs[4].info.Error.Error(), "boom") {
		t.Errorf("beta end Error = %v, want a ChildContextError wrapping boom", evs[4].info.Error)
	}
	if evs[4].info.Result != "" {
		t.Errorf("beta end Result = %q, want empty", evs[4].info.Result)
	}
	if evs[5].info.Error != nil {
		t.Errorf("par end Error = %v, want nil: the batch checkpoint is SUCCEEDED", evs[5].info.Error)
	}
}

// TestOperationLifecycleBatchAbandonedItems asserts that items the batch
// abandons on early completion dispatch an end with status STARTED, the
// status of their checkpoint, once every worker has drained and before the
// batch end. The abandoned branches spin on steps until the abandon signal
// unwinds them, so they never succeed; MinSuccessful=1 is met by the
// immediate branch on every run.
func TestOperationLifecycleBatchAbandonedItems(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		br, err := Map(ctx, "batch", []int{0, 1, 2}, func(c Context, item, idx int) (string, error) {
			if idx == 0 {
				return "fast", nil
			}
			for {
				if _, err := Step(c, "spin", func(StepContext) (string, error) { return "", nil }); err != nil {
					return "", err
				}
			}
		}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }),
			WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return 0, err
		}
		return br.StartedCount(), nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	items := batchItemEvents(evs, "1")
	assertSequence(t, items["item-0"], "start:item-0:STARTED:false", "end:item-0:SUCCEEDED:false")
	var abandoned []opEvent
	for _, name := range []string{"item-1", "item-2"} {
		assertSequence(t, items[name], "start:"+name+":STARTED:false", "end:"+name+":STARTED:false")
		end := items[name][1]
		assertLiveTimestamps(t, end)
		if end.info.Error != nil || end.info.Result != "" {
			t.Errorf("%s abandoned end: Error = %v Result = %q, want nil and empty", name, end.info.Error, end.info.Result)
		}
		abandoned = append(abandoned, end)
	}
	// Abandoned ends are dispatched on the coordinator after the workers
	// drained, in index order, and before the batch end.
	batch := eventsForName(evs, "batch")
	assertSequence(t, batch, "start:batch:STARTED:false", "end:batch:SUCCEEDED:false")
	assertEventsOrdered(t, evs, abandoned[0], abandoned[1], batch[1])
	// Every event of the spinning steps precedes the abandoned ends: the
	// items dispatch their end only once their workers have unwound.
	lastSpin := -1
	for i, ev := range evs {
		if ev.info.Name == "spin" {
			lastSpin = i
		}
	}
	firstAbandoned := len(evs)
	for i, ev := range evs {
		if ev.hook == "end" && ev.info.Status == PluginOperationStarted {
			firstAbandoned = i
			break
		}
	}
	if lastSpin > firstAbandoned {
		t.Errorf("a spin step event at %d follows the first abandoned end at %d", lastSpin, firstAbandoned)
	}
}

// TestOperationLifecycleBatchFlatNesting asserts that a FLAT batch
// dispatches its own start and end and no item events: the operations
// inside each item name the batch as ParentID, as their checkpoints do.
func TestOperationLifecycleBatchFlatNesting(t *testing.T) {
	rec := &opRecorder{}
	handler := mapLifecycleHandler(rec, WithNesting(NestingFlat), WithMaxConcurrency(1))
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	assertSequence(t, evs,
		"start:batch:STARTED:false",
		"start:step-0:STARTED:false",
		"end:step-0:SUCCEEDED:false",
		"start:step-1:STARTED:false",
		"end:step-1:SUCCEEDED:false",
		"start:step-2:STARTED:false",
		"end:step-2:SUCCEEDED:false",
		"end:batch:SUCCEEDED:false",
	)
	for i := range 3 {
		assertIdentity(t, evs[1+2*i], fmt.Sprintf("1-%d-1", i+1), fmt.Sprintf("step-%d", i), string(OperationTypeStep), operationSubTypeStep, hashID("1"))
	}
	for _, ev := range evs {
		if ev.info.SubType == operationSubTypeMapIteration {
			t.Errorf("flat item dispatched %v", summarize([]opEvent{ev}))
		}
	}
}

// TestOperationLifecycleBatchReplayed asserts the events of a batch across
// a suspend-and-resume cycle. Invocation 1 runs item 0, then item 1
// suspends on a wait: the batch and both items dispatch starts, item 0
// dispatches its end, nothing else ends. Invocation 2 replays: item 0 is
// terminal and dispatches only a replayed end with its checkpointed
// timestamps; item 1 re-enters STARTED with a replayed start, then ends
// live; the batch re-enters with a replayed start and ends live.
func TestOperationLifecycleBatchReplayed(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		br, err := Map(ctx, "batch", []int{1, 2}, func(c Context, item, idx int) (int, error) {
			if idx == 1 {
				if err := Wait(c, "pause", 5e9); err != nil {
					return 0, err
				}
			}
			return item, nil
		}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }), WithMaxConcurrency(1))
		if err != nil {
			return 0, err
		}
		return br.SuccessCount(), nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	resumeOps := []wireOperation{
		contextOp("1", "", operationSubTypeMap, "batch", "STARTED", nil),
		contextOp("2", "1", operationSubTypeMapIteration, "item-0", "SUCCEEDED", &wireContextDetails{Result: "1"}),
		contextOp("3", "1", operationSubTypeMapIteration, "item-1", "STARTED", nil),
		{Id: hashID("3-1"), ParentId: hashID("3"), Status: "SUCCEEDED", Type: "WAIT", SubType: "Wait", Name: "pause",
			StartTimestamp: flexTimestamp{Time: lifecycleStart, Valid: true}, EndTimestamp: flexTimestamp{Time: lifecycleEnd, Valid: true}},
	}
	first, second := runSuspendResume(t, handler, rec, resumeOps, invocationSucceeded)

	assertSequence(t, first,
		"start:batch:STARTED:false",
		"start:item-0:STARTED:false",
		"end:item-0:SUCCEEDED:false",
		"start:item-1:STARTED:false",
		"start:pause:STARTED:false",
	)
	assertSequence(t, second,
		"start:batch:STARTED:true",
		"end:item-0:SUCCEEDED:true",
		"start:item-1:STARTED:true",
		"end:pause:SUCCEEDED:true",
		"end:item-1:SUCCEEDED:false",
		"end:batch:SUCCEEDED:false",
	)
	assertReplayedTimestamps(t, second[1], true)
	assertIdentity(t, second[1], "2", "item-0", string(OperationTypeContext), operationSubTypeMapIteration, hashID("1"))
	if second[1].info.Result != "1" {
		t.Errorf("replayed item-0 end Result = %q, want %q", second[1].info.Result, "1")
	}
	assertReplayedTimestamps(t, second[2], false)
	assertIdentity(t, second[2], "3", "item-1", string(OperationTypeContext), operationSubTypeMapIteration, hashID("1"))
	assertReplayedTimestamps(t, second[0], false)
}

// TestOperationLifecycleBatchReplayedTerminal asserts a batch replayed from
// its SUCCEEDED aggregate checkpoint dispatches one replayed end with the
// checkpointed timestamps and payload, and does not visit its items.
func TestOperationLifecycleBatchReplayedTerminal(t *testing.T) {
	rec := &opRecorder{}
	handler := mapLifecycleHandler(rec, WithMaxConcurrency(1))
	payload := `{"results":[{"index":0,"name":"item-0","status":1,"result":"2"},{"index":1,"name":"item-1","status":1,"result":"4"},{"index":2,"name":"item-2","status":1,"result":"6"}],"reason":1}`
	ops := []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", operationSubTypeMap, "batch", "SUCCEEDED", &wireContextDetails{Result: payload}),
	}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	assertSequence(t, evs, "end:batch:SUCCEEDED:true")
	assertIdentity(t, evs[0], "1", "batch", string(OperationTypeContext), operationSubTypeMap, "")
	assertReplayedTimestamps(t, evs[0], true)
	if evs[0].info.Result != payload {
		t.Errorf("replayed batch end Result = %q, want the checkpointed payload", evs[0].info.Result)
	}
}

// TestOperationLifecycleBatchReplayedAbandonedFromRecord asserts that a
// batch too large to store, replayed from its decision record, dispatches a
// replayed end for each admitted item: SUCCEEDED for the terminal ones and
// STARTED for the abandoned ones, then its own replayed end.
func TestOperationLifecycleBatchReplayedAbandonedFromRecord(t *testing.T) {
	rec := &opRecorder{}
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		br, err := Map(ctx, "batch", []int{0, 1, 2}, func(c Context, item, idx int) (string, error) {
			return "unused", nil
		}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }),
			WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return 0, err
		}
		return br.StartedCount(), nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	record := `{"completionReason":2,"totalCount":3,"indexSet":"started","indexes":[1,2]}`
	ops := []wireOperation{
		lifecycleExecOp(),
		contextOp("1", "", operationSubTypeMap, "batch", "SUCCEEDED", &wireContextDetails{Result: record, ReplayChildren: true}),
		contextOp("2", "1", operationSubTypeMapIteration, "item-0", "SUCCEEDED", &wireContextDetails{Result: `"fast"`}),
		contextOp("3", "1", operationSubTypeMapIteration, "item-1", "STARTED", nil),
		contextOp("4", "1", operationSubTypeMapIteration, "item-2", "STARTED", nil),
	}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	assertSequence(t, evs,
		"end:item-0:SUCCEEDED:true",
		"end:item-1:STARTED:true",
		"end:item-2:STARTED:true",
		"end:batch:SUCCEEDED:true",
	)
	for i, id := range []string{"2", "3", "4"} {
		assertIdentity(t, evs[i], id, fmt.Sprintf("item-%d", i), string(OperationTypeContext), operationSubTypeMapIteration, hashID("1"))
	}
	assertReplayedTimestamps(t, evs[1], false)
}

// TestOperationLifecycleBatchConcurrentDispatchRace runs a Map with
// concurrency above one whose items each run several steps, so plugin hooks
// are dispatched from several goroutines at once. It asserts the recorded
// event set is complete and consistent; the race detector covers the rest.
func TestOperationLifecycleBatchConcurrentDispatchRace(t *testing.T) {
	rec := &opRecorder{}
	const items, steps = 8, 4
	handler := Wrap(func(ctx Context, _ string) (int, error) {
		in := make([]int, items)
		br, err := Map(ctx, "batch", in, func(c Context, _, idx int) (int, error) {
			for s := range steps {
				if _, err := Step(c, fmt.Sprintf("s%d", s), func(StepContext) (int, error) { return s, nil }); err != nil {
					return 0, err
				}
			}
			return idx, nil
		}, WithMaxConcurrency(items))
		if err != nil {
			return 0, err
		}
		return br.SuccessCount(), nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(&fakePluginClient{}))

	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:lifecycle", "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
	evs := rec.take()

	// 2 batch events + 2 per item + 2 per step.
	if want := 2 + items*2 + items*steps*2; len(evs) != want {
		t.Fatalf("len(events) = %d, want %d", len(evs), want)
	}
	// The items are unnamed, so their events form one group: one STARTED
	// start and one SUCCEEDED end per item, each item ID once per hook.
	counts := map[string]int{}
	for _, ev := range batchItemEvents(evs, "1")[""] {
		counts[ev.hook+":"+ev.info.ID+":"+string(ev.info.Status)]++
	}
	for i := range items {
		id := fmt.Sprint(i + 2)
		if counts["start:"+id+":STARTED"] != 1 || counts["end:"+id+":SUCCEEDED"] != 1 {
			t.Errorf("item %s events = %v, want one start and one end", id, counts)
		}
	}
	if len(counts) != items*2 {
		t.Errorf("distinct item events = %d, want %d: %v", len(counts), items*2, counts)
	}
}

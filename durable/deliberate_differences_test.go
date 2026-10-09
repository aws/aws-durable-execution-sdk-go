package durable_test

import (
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// All returns values in input order, independent of the order the futures
// settle. The three bodies are gated so they finish in the order c, b, a
// (indexes 2, 1, 0), yet All returns [10, 20, 30].
func TestAllInputOrder(t *testing.T) {
	var mu sync.Mutex
	var completion []int
	handler := func(ctx durable.Context, _ any) ([]int, error) {
		gateA := make(chan struct{}) // closed by b, unblocks a
		gateB := make(chan struct{}) // closed by c, unblocks b
		a := durable.StepAsync(ctx, "a", func(_ durable.StepContext) (int, error) {
			<-gateA
			mu.Lock()
			completion = append(completion, 0)
			mu.Unlock()
			return 10, nil
		})
		b := durable.StepAsync(ctx, "b", func(_ durable.StepContext) (int, error) {
			<-gateB
			mu.Lock()
			completion = append(completion, 1)
			mu.Unlock()
			close(gateA)
			return 20, nil
		})
		c := durable.StepAsync(ctx, "c", func(_ durable.StepContext) (int, error) {
			mu.Lock()
			completion = append(completion, 2)
			mu.Unlock()
			close(gateB)
			return 30, nil
		})
		return durable.All(ctx, "gather", []*durable.Future[int]{a, b, c})
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[[]int](r)
	t.Logf("all status=%s result=%v completionOrder=%v err=%v", r.Status, got, completion, err)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{10, 20, 30}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result = %v, want %v", got, want)
	}
	if want := []int{2, 1, 0}; !reflect.DeepEqual(completion, want) {
		t.Fatalf("completion order = %v, want %v", completion, want)
	}
}

// Race returns the first future to settle and not its identity. The winner
// is forced: "win" closes a channel and returns at once, "lose" blocks on
// it then sleeps.
func TestRaceFirstSettled(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		ch := make(chan struct{})
		win := durable.StepAsync(ctx, "win", func(_ durable.StepContext) (string, error) {
			close(ch)
			return "win", nil
		})
		lose := durable.StepAsync(ctx, "lose", func(_ durable.StepContext) (string, error) {
			<-ch
			time.Sleep(50 * time.Millisecond)
			return "lose", nil
		})
		return durable.Race(ctx, "race", []*durable.Future[string]{win, lose})
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[string](r)
	t.Logf("race status=%s result=%q err=%v", r.Status, got, err)
	if err != nil {
		t.Fatal(err)
	}
	if got != "win" {
		t.Fatalf("result = %q, want %q", got, "win")
	}
}

// Select reports the name of the first branch to settle together with its
// value. Same forced ordering as Race.
func TestSelectReportsName(t *testing.T) {
	type sel struct {
		Winner string
		Value  string
	}
	handler := func(ctx durable.Context, _ any) (sel, error) {
		ch := make(chan struct{})
		branches := []durable.Branch[string]{
			{Name: "fast", Func: func(c durable.Context) (string, error) {
				return durable.Step(c, "f", func(_ durable.StepContext) (string, error) {
					close(ch)
					return "F", nil
				})
			}},
			{Name: "slow", Func: func(c durable.Context) (string, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (string, error) {
					<-ch
					time.Sleep(50 * time.Millisecond)
					return "S", nil
				})
			}},
		}
		name, val, err := durable.Select(ctx, "sel", branches)
		return sel{Winner: name, Value: val}, err
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[sel](r)
	t.Logf("select status=%s winner=%q value=%q err=%v", r.Status, got.Winner, got.Value, err)
	if err != nil {
		t.Fatal(err)
	}
	if got.Winner != "fast" || got.Value != "F" {
		t.Fatalf("select = %+v, want {fast F}", got)
	}
}

// A StepAsync the handler never awaits records its start, because the
// call queues the START before it returns. The invocation answers with the
// handler's outcome in that invocation. The body may or may not finish
// before the handler returns, so the step is STARTED or SUCCEEDED.
func TestUnawaitedStep(t *testing.T) {
	var ran int32
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.StepAsync(ctx, "bg", func(_ durable.StepContext) (string, error) {
			atomic.StoreInt32(&ran, 1)
			return "bg-result", nil
		})
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-step status=%s events=%v bodyRan=%d", r.Status, r.EventTypes(), atomic.LoadInt32(&ran))
	assertUnawaitedRecorded(t, r, "bg", "StepStarted", "STARTED", "SUCCEEDED")
}

// A WaitAsync the handler never awaits records its start and does not
// keep the invocation PENDING: the execution succeeds in one invocation.
func TestUnawaitedWait(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.WaitAsync(ctx, "bgwait", time.Hour)
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-wait status=%s events=%v", r.Status, r.EventTypes())
	assertUnawaitedRecorded(t, r, "bgwait", "WaitStarted", "STARTED")
}

// assertUnawaitedRecorded asserts that the execution in r succeeded in
// exactly one invocation, that the operation name is recorded with one of
// statuses, and that the history holds a startEvent event for it.
func assertUnawaitedRecorded(t *testing.T, r *durabletest.TestResult, name, startEvent string, statuses ...string) {
	t.Helper()
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if n := countEvents(r, "InvocationCompleted"); n != 1 {
		t.Fatalf("InvocationCompleted events = %d, want 1 (events %v)", n, r.EventTypes())
	}
	op := r.Operation(name)
	if op == nil {
		t.Fatalf("never-awaited %s recorded no operation", name)
	}
	if !slices.Contains(statuses, op.Status) {
		t.Fatalf("never-awaited %s status = %s, want one of %v", name, op.Status, statuses)
	}
	if eventIndex(r, startEvent, name) < 0 {
		t.Fatalf("history holds no %s event for %s: %v", startEvent, name, r.EventTypes())
	}
}

// countEvents returns the number of events of type eventType in r.
func countEvents(r *durabletest.TestResult, eventType string) int {
	n := 0
	for _, e := range r.EventTypes() {
		if e == eventType {
			n++
		}
	}
	return n
}

// eventIndex returns the index of the first event of type eventType for
// the operation name in r, or -1.
func eventIndex(r *durabletest.TestResult, eventType, name string) int {
	for i, ev := range r.Events {
		if string(ev.EventType) == eventType && aws.ToString(ev.Name) == name {
			return i
		}
	}
	return -1
}

// A Go or RunInChildContextAsync future the handler never awaits records
// its start operation, because the child context checkpoints its start
// when it is created. Its body may not run to completion before the
// invocation ends, and the execution still succeeds.
func TestUnawaitedChildContext(t *testing.T) {
	var innerRan int32
	release := make(chan struct{})
	defer close(release)
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.Go(ctx, "bggo", func(c durable.Context) (string, error) {
			<-release
			return durable.Step(c, "inner", func(_ durable.StepContext) (string, error) {
				atomic.StoreInt32(&innerRan, 1)
				return "x", nil
			})
		})
		_ = durable.RunInChildContextAsync(ctx, "bgchild", func(c durable.Context) (string, error) {
			<-release
			return "y", nil
		})
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-child status=%s bggoRecorded=%v bgchildRecorded=%v innerRecorded=%v events=%v innerRan=%d",
		r.Status, r.Operation("bggo") != nil, r.Operation("bgchild") != nil,
		r.Operation("inner") != nil, r.EventTypes(), atomic.LoadInt32(&innerRan))
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	for _, name := range []string{"bggo", "bgchild"} {
		op := r.Operation(name)
		if op == nil {
			t.Fatalf("never-awaited %s recorded no operation", name)
		}
		if op.Status != "STARTED" {
			t.Fatalf("never-awaited %s status = %s, want STARTED", name, op.Status)
		}
	}
	if r.Operation("inner") != nil {
		t.Fatal("blocked child body recorded its inner step")
	}
}

// An InvokeAsync future the handler never awaits records its start and
// does not keep the invocation PENDING: the execution succeeds in one
// invocation.
func TestUnawaitedInvoke(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.InvokeAsync[string](ctx, "bginvoke", "target-function:$LATEST", "in")
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-invoke status=%s events=%v", r.Status, r.EventTypes())
	assertUnawaitedRecorded(t, r, "bginvoke", "ChainedInvokeStarted", "STARTED")
}

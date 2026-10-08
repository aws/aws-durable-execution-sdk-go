package durable_test

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// A StepAsync the handler never awaits records no operation and no
// StepStarted event, and the execution still succeeds.
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
	t.Logf("unawaited-step status=%s bgOpRecorded=%v events=%v bodyRan=%d",
		r.Status, r.Operation("bg") != nil, r.EventTypes(), atomic.LoadInt32(&ran))
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if r.Operation("bg") != nil {
		t.Fatal("never-awaited step recorded an operation")
	}
	for _, e := range r.EventTypes() {
		if e == "StepStarted" {
			t.Fatal("never-awaited step recorded a StepStarted event")
		}
	}
}

// A WaitAsync the handler never awaits records no operation and no
// WaitStarted event, and the execution still succeeds.
func TestUnawaitedWait(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.WaitAsync(ctx, "bgwait", time.Hour)
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-wait status=%s bgwaitOpRecorded=%v events=%v",
		r.Status, r.Operation("bgwait") != nil, r.EventTypes())
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if r.Operation("bgwait") != nil {
		t.Fatal("never-awaited wait recorded an operation")
	}
	for _, e := range r.EventTypes() {
		if e == "WaitStarted" {
			t.Fatal("never-awaited wait recorded a WaitStarted event")
		}
	}
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

// An InvokeAsync future the handler never awaits records no operation and
// no ChainedInvokeStarted event, and the execution still succeeds.
func TestUnawaitedInvoke(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.InvokeAsync[string](ctx, "bginvoke", "target-function:$LATEST", "in")
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-invoke status=%s bginvokeRecorded=%v events=%v",
		r.Status, r.Operation("bginvoke") != nil, r.EventTypes())
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if r.Operation("bginvoke") != nil {
		t.Fatal("never-awaited invoke recorded an operation")
	}
}

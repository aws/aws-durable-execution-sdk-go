package durable

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// slowCompletion names the checkpoint update whose call the fake holds open
// until the test releases it. It stands for a completion record whose write
// is still in flight when the handler blocks: a batch item's SUCCEED, a
// batch parent's SUCCEED, or a WaitForCallback context's SUCCEED.
type slowCompletion struct {
	name   string
	action OperationAction
}

// runSuspendWithSlowCompletion runs a handler that launches a branch whose
// body runs a step blocked on a gate, then blocks the root goroutine on a
// Wait so the invocation commits to PENDING. launch starts the branch and
// returns a function that awaits its result; body is the step function the
// branch must run, and it blocks until the test releases it. The fake holds
// the checkpoint call carrying slow open until the test releases that too,
// so the completion record is in flight while the handler is blocked. The
// test asserts that no response arrives while the record is in flight, and
// returns what the fake received before the response.
//
// state is the operations the fake reports as already checkpointed, so a
// scenario can start from a replayed prefix.
func runSuspendWithSlowCompletion(t *testing.T, state []wireOperation, slow slowCompletion, launch func(ctx Context, body func(StepContext) (string, error)) func() (string, error)) drainOutcome {
	t.Helper()

	bodyStarted := make(chan struct{})
	gate := make(chan struct{})
	waitCheckpointed := make(chan struct{})
	slowReached := make(chan struct{})
	slowGate := make(chan struct{})
	var (
		mu        sync.Mutex
		batches   [][]OperationUpdate
		late      [][]OperationUpdate
		responded atomic.Bool
		waitOnce  sync.Once
		slowOnce  sync.Once
	)
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			for _, u := range in.Updates {
				if u.Type == OperationTypeContext && aws.ToString(u.Name) == slow.name && u.Action == slow.action {
					// The record's write is in flight until released.
					slowOnce.Do(func() { close(slowReached) })
					<-slowGate
				}
			}
			// A call is recorded when it completes: a call that was
			// still open when the response arrived counts as late.
			mu.Lock()
			if responded.Load() {
				late = append(late, in.Updates)
			} else {
				batches = append(batches, in.Updates)
			}
			mu.Unlock()
			for _, u := range in.Updates {
				if u.Type == OperationTypeWait && u.Action == OperationActionStart {
					waitOnce.Do(func() { close(waitCheckpointed) })
				}
			}
			return CheckpointOutput{CheckpointToken: "token-fake"}, nil
		},
	}

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		await := launch(ctx, func(_ StepContext) (string, error) {
			close(bodyStarted)
			<-gate
			return "completed", nil
		})
		// The branch has entered the step body.
		<-bodyStarted
		if err := Wait(ctx, "short-wait", time.Second); err != nil {
			return "", err
		}
		return await()
	}, withLambdaAPI(fake))

	type response struct {
		raw []byte
		err error
	}
	respCh := make(chan response, 1)
	go func() {
		raw, err := h(context.Background(), stepPayload(`""`, state...))
		mu.Lock()
		responded.Store(true)
		mu.Unlock()
		respCh <- response{raw: raw, err: err}
	}()

	<-waitCheckpointed
	// The handler goroutine is blocked. Let the step finish so the
	// branch proceeds to its completion record, whose write then stalls.
	close(gate)
	select {
	case <-slowReached:
	case resp := <-respCh:
		if resp.err != nil {
			t.Fatalf("Invoke error before %s %s was written: %v", slow.name, slow.action, resp.err)
		}
		t.Fatalf("invocation responded %s before %s %s was written", resp.raw, slow.name, slow.action)
	case <-time.After(orphanExitTiming):
		t.Fatalf("%s %s was never written", slow.name, slow.action)
	}
	select {
	case resp := <-respCh:
		if resp.err != nil {
			t.Fatalf("Invoke error while %s %s was in flight: %v", slow.name, slow.action, resp.err)
		}
		t.Fatalf("invocation responded %s while %s %s was in flight", resp.raw, slow.name, slow.action)
	case <-time.After(prematureResponseWindow):
	}

	close(slowGate)
	var resp response
	select {
	case resp = <-respCh:
	case <-time.After(orphanExitTiming):
		t.Fatal("invocation did not respond after the completion record was written")
	}
	if resp.err != nil {
		t.Fatalf("Invoke error: %v", resp.err)
	}

	mu.Lock()
	defer mu.Unlock()
	return drainOutcome{status: parseResponse(t, resp.raw).Status, batches: batches, late: late}
}

// requireBranchCompletionRecorded asserts the invocation responded PENDING
// and that every named context recorded its SUCCEED before the response.
func requireBranchCompletionRecorded(t *testing.T, out drainOutcome, names ...string) {
	t.Helper()
	if out.status != invocationPending {
		t.Fatalf("status = %q, want %q", out.status, invocationPending)
	}
	for _, name := range names {
		requireRecordedBeforeResponse(t, out, name, OperationActionSucceed)
	}
}

// TestSuspendWaitsForConcurrentMapItemCompletion covers a Map worker: an
// item's step has finished and the item's completion record is being
// written when the handler blocks. The invocation must not respond PENDING
// until the item, the batch, and the enclosing branch have recorded their
// completion. Otherwise the next invocation finds the item unfinished and
// runs its body again.
func TestSuspendWaitsForConcurrentMapItemCompletion(t *testing.T) {
	out := runSuspendWithSlowCompletion(t, nil, slowCompletion{name: "item-0", action: OperationActionSucceed},
		func(ctx Context, body func(StepContext) (string, error)) func() (string, error) {
			fut := Go(ctx, "mapper", func(c Context) (string, error) {
				res, err := Map(c, "items", []int{0, 1}, func(c Context, _ int, index int) (string, error) {
					if index == 0 {
						return Step(c, "inner-step", body)
					}
					return "quick", nil
				}, WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }))
				if err != nil {
					return "", err
				}
				return res.Results()[0], nil
			})
			return fut.Result
		})
	requireBranchCompletionRecorded(t, out, "inner-step", "item-0", "items", "mapper")
}

// TestSuspendWaitsForSequentialMapItemCompletion is the sequential form of
// the Map scenario: the item runs on the branch's own goroutine.
func TestSuspendWaitsForSequentialMapItemCompletion(t *testing.T) {
	out := runSuspendWithSlowCompletion(t, nil, slowCompletion{name: "item-0", action: OperationActionSucceed},
		func(ctx Context, body func(StepContext) (string, error)) func() (string, error) {
			fut := Go(ctx, "mapper", func(c Context) (string, error) {
				res, err := Map(c, "items", []int{0}, func(c Context, _ int, _ int) (string, error) {
					return Step(c, "inner-step", body)
				}, WithMaxConcurrency(1), WithItemNamer(func(int) string { return "item-0" }))
				if err != nil {
					return "", err
				}
				return res.Results()[0], nil
			})
			return fut.Result
		})
	requireBranchCompletionRecorded(t, out, "inner-step", "item-0", "items", "mapper")
}

// TestSuspendWaitsForParallelParentCompletion covers the batch parent: every
// branch of a Parallel has completed and the parent's completion record is
// being written when the handler blocks. Otherwise the next invocation
// finds the batch unfinished and runs it again.
func TestSuspendWaitsForParallelParentCompletion(t *testing.T) {
	out := runSuspendWithSlowCompletion(t, nil, slowCompletion{name: "fan-out", action: OperationActionSucceed},
		func(ctx Context, body func(StepContext) (string, error)) func() (string, error) {
			fut := Go(ctx, "runner", func(c Context) (string, error) {
				res, err := Parallel(c, "fan-out", []Branch[string]{
					{Name: "slow", Func: func(c Context) (string, error) { return Step(c, "inner-step", body) }},
					{Name: "quick", Func: func(Context) (string, error) { return "quick", nil }},
				})
				if err != nil {
					return "", err
				}
				return res.Results()[0], nil
			})
			return fut.Result
		})
	requireBranchCompletionRecorded(t, out, "inner-step", "slow", "fan-out", "runner")
}

// TestSuspendWaitsForCallbackContextCompletion covers WaitForCallback: the
// callback was resolved in an earlier invocation, the submitter step
// finishes now, and the context's completion record is being written when
// the handler blocks. Otherwise the next invocation finds the context
// unfinished and runs the submitter again.
func TestSuspendWaitsForCallbackContextCompletion(t *testing.T) {
	// The branch ("1") and the WaitForCallback context ("1-1") are
	// STARTED; the inner callback ("1-1-1") is SUCCEEDED; the submitter
	// step ("1-1-2") has not run, so it runs live and is the gated body.
	state := []wireOperation{
		{Id: hashID("1"), Name: "cb-branch", Type: string(OperationTypeContext), SubType: operationSubTypeRunInChildContext, Status: "STARTED"},
		{Id: hashID("1-1"), ParentId: hashID("1"), Name: "cb", Type: string(OperationTypeContext), SubType: operationSubTypeWaitForCallback, Status: "STARTED"},
		{Id: hashID("1-1-1"), ParentId: hashID("1-1"), Type: string(OperationTypeCallback), SubType: operationSubTypeCallback, Status: "SUCCEEDED",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1", Result: `"resolved"`}},
	}
	out := runSuspendWithSlowCompletion(t, state, slowCompletion{name: "cb", action: OperationActionSucceed},
		func(ctx Context, body func(StepContext) (string, error)) func() (string, error) {
			fut := Go(ctx, "cb-branch", func(c Context) (string, error) {
				return WaitForCallback[string](c, "cb", func(sc StepContext, _ string) error {
					_, err := body(sc)
					return err
				})
			})
			return fut.Result
		})
	requireBranchCompletionRecorded(t, out, "cb", "cb-branch")
	succeed := findUpdate(out.batches, "cb", OperationActionSucceed)
	if got := aws.ToString(succeed.Payload); got != `"resolved"` {
		t.Errorf("cb payload = %s, want %q", got, `"resolved"`)
	}
}

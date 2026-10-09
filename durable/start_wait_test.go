// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// holdClient is an ExecutionClient that records every Checkpoint call. It
// holds the first call that carries an update that match accepts: the call
// closes held and returns only after release is closed. Every other call
// returns at once.
type holdClient struct {
	match   func(durable.OperationUpdate) bool
	held    chan struct{}
	release chan struct{}

	mu     sync.Mutex
	calls  [][]durable.OperationUpdate
	isHeld bool
}

func newHoldClient(match func(durable.OperationUpdate) bool) *holdClient {
	return &holdClient{match: match, held: make(chan struct{}), release: make(chan struct{})}
}

func (c *holdClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *holdClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	c.calls = append(c.calls, in.Updates)
	n := len(c.calls)
	hold := false
	if !c.isHeld {
		for _, u := range in.Updates {
			if c.match(u) {
				hold = true
				c.isHeld = true
				break
			}
		}
	}
	c.mu.Unlock()
	if hold {
		close(c.held)
		<-c.release
	}
	return durable.CheckpointOutput{CheckpointToken: "tok-" + strconv.Itoa(n)}, nil
}

// updates returns every update the client received, in the order it
// received them.
func (c *holdClient) updates() []durable.OperationUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	var all []durable.OperationUpdate
	for _, call := range c.calls {
		all = append(all, call...)
	}
	return all
}

func isStart(u durable.OperationUpdate, typ durable.OperationType, subType string) bool {
	return u.Action == durable.OperationActionStart && u.Type == typ && aws.ToString(u.SubType) == subType
}

func holdInput(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// invokeAsync starts one invocation of h against c and returns a channel
// that receives the invocation's error.
func invokeAsync[O any](t *testing.T, h durable.Handler[struct{}, O], c durable.ExecutionClient) <-chan error {
	t.Helper()
	input := holdInput(t)
	done := make(chan error, 1)
	go func() {
		_, err := durable.Wrap(h, durable.WithExecutionClient(c))(context.Background(), input)
		done <- err
	}()
	return done
}

// assertStartFirst checks that no update of an operation was sent before
// that operation's START.
func assertStartFirst(t *testing.T, updates []durable.OperationUpdate) {
	t.Helper()
	started := map[string]bool{}
	for _, u := range updates {
		id := aws.ToString(u.Id)
		if u.Action == durable.OperationActionStart {
			started[id] = true
			continue
		}
		if u.Type != durable.OperationTypeExecution && !started[id] {
			t.Errorf("%s %s of operation %q was sent before its START", u.Type, u.Action, aws.ToString(u.Name))
		}
	}
}

// Under AtLeastOncePerRetry, a step body runs while the call that carries
// its START is in flight. On main the body waits for that call, so this
// test blocks until go test -timeout ends it.
func TestAtLeastOnceStepBodyRunsWhileStartIsInFlight(t *testing.T) {
	c := newHoldClient(func(u durable.OperationUpdate) bool {
		return isStart(u, durable.OperationTypeStep, durable.OperationSubTypeStep)
	})
	bodyRan := make(chan struct{})
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			close(bodyRan)
			return "ok", nil
		})
	}, c)

	<-c.held
	<-bodyRan
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("invocation: %v", err)
	}
	assertStartFirst(t, c.updates())
}

// Under AtMostOncePerRetry, a step body runs only after the call that
// carries its START has returned. This holds on main and must keep holding.
func TestAtMostOnceStepBodyRunsAfterStartIsRecorded(t *testing.T) {
	c := newHoldClient(func(u durable.OperationUpdate) bool {
		return isStart(u, durable.OperationTypeStep, durable.OperationSubTypeStep)
	})
	var released atomic.Bool
	var sawRelease atomic.Bool
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			sawRelease.Store(released.Load())
			return "ok", nil
		}, durable.WithSemantics(durable.AtMostOncePerRetry))
	}, c)

	<-c.held
	released.Store(true)
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("invocation: %v", err)
	}
	if !sawRelease.Load() {
		t.Error("step body ran before the call that carries its START returned")
	}
}

// startsWhileItemStartIsHeld runs a batch of items whose functions each
// signal that they started. The client holds the call that carries the
// first item START until every item function has started. On main the
// batch waits for that call before it starts the next item, so the test
// blocks until go test -timeout ends it.
func startsWhileItemStartIsHeld(t *testing.T, itemSubType string, run func(ctx durable.Context, items int, started func()) error) {
	const items = 20
	c := newHoldClient(func(u durable.OperationUpdate) bool {
		return isStart(u, durable.OperationTypeContext, itemSubType)
	})
	var wg sync.WaitGroup
	wg.Add(items)
	allStarted := make(chan struct{})
	go func() {
		wg.Wait()
		close(allStarted)
	}()
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return "", run(ctx, items, wg.Done)
	}, c)

	<-c.held
	<-allStarted
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("invocation: %v", err)
	}
	assertStartFirst(t, c.updates())
}

func TestMapStartsItemsWhileItemStartIsInFlight(t *testing.T) {
	startsWhileItemStartIsHeld(t, durable.OperationSubTypeMapIteration, func(ctx durable.Context, items int, started func()) error {
		_, err := durable.Map(ctx, "map", make([]int, items), func(_ durable.Context, _ int, i int) (int, error) {
			started()
			return i, nil
		})
		return err
	})
}

func TestParallelStartsBranchesWhileBranchStartIsInFlight(t *testing.T) {
	startsWhileItemStartIsHeld(t, durable.OperationSubTypeParallelBranch, func(ctx durable.Context, items int, started func()) error {
		branches := make([]durable.Branch[int], items)
		for i := range branches {
			branches[i] = durable.Branch[int]{Func: func(durable.Context) (int, error) {
				started()
				return i, nil
			}}
		}
		_, err := durable.Parallel(ctx, "parallel", branches)
		return err
	})
}

func TestGoStartsChildrenWhileChildStartIsInFlight(t *testing.T) {
	startsWhileItemStartIsHeld(t, durable.OperationSubTypeRunInChildContext, func(ctx durable.Context, items int, started func()) error {
		futs := make([]*durable.Future[int], items)
		for i := range futs {
			futs[i] = durable.Go(ctx, "go-"+strconv.Itoa(i), func(durable.Context) (int, error) {
				started()
				return i, nil
			})
		}
		_, err := durable.All(ctx, "all", futs)
		return err
	})
}

// returnsAfterHeldCall runs op inside a handler. The client holds the
// first call that carries an update that match accepts. The test checks
// that op returned only after the held call returned. This holds on main
// and must keep holding.
func returnsAfterHeldCall(t *testing.T, what string, match func(durable.OperationUpdate) bool, op func(durable.Context) (string, error)) {
	t.Helper()
	c := newHoldClient(match)
	var released, returnedAfterRelease atomic.Bool
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		v, err := op(ctx)
		returnedAfterRelease.Store(released.Load())
		return v, err
	}, c)

	<-c.held
	released.Store(true)
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("invocation: %v", err)
	}
	if !returnedAfterRelease.Load() {
		t.Errorf("%s returned before the call that carries its SUCCEED returned", what)
	}
	assertStartFirst(t, c.updates())
}

// A step under AtLeastOncePerRetry does not wait for its START, but it
// still waits for its SUCCEED. The SUCCEED is queued after the START, so
// when the wait returns, the START is recorded too.
func TestAtLeastOnceStepReturnsAfterSucceedIsRecorded(t *testing.T) {
	returnsAfterHeldCall(t, "Step", func(u durable.OperationUpdate) bool {
		return u.Type == durable.OperationTypeStep && u.Action == durable.OperationActionSucceed
	}, func(ctx durable.Context) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) { return "ok", nil })
	})
}

// A child context does not wait for its START, but it still waits for its
// SUCCEED before RunInChildContext returns.
func TestRunInChildContextReturnsAfterSucceedIsRecorded(t *testing.T) {
	returnsAfterHeldCall(t, "RunInChildContext", func(u durable.OperationUpdate) bool {
		return u.Type == durable.OperationTypeContext && u.Action == durable.OperationActionSucceed &&
			aws.ToString(u.Name) == "child"
	}, func(ctx durable.Context) (string, error) {
		return durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) { return "v", nil })
	})
}

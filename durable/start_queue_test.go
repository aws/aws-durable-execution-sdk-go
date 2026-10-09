// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// failClient is an ExecutionClient that records every Checkpoint call. The
// first call that carries an update that match accepts closes held, waits
// for release, and then fails with err. Every other call succeeds at once.
type failClient struct {
	match   func(durable.OperationUpdate) bool
	err     error
	held    chan struct{}
	release chan struct{}

	mu       sync.Mutex
	calls    [][]durable.OperationUpdate
	failedAt int
}

func newFailClient(match func(durable.OperationUpdate) bool, err error) *failClient {
	return &failClient{match: match, err: err, held: make(chan struct{}), release: make(chan struct{}), failedAt: -1}
}

func (c *failClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *failClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	c.calls = append(c.calls, in.Updates)
	n := len(c.calls)
	fail := false
	if c.failedAt < 0 {
		for _, u := range in.Updates {
			if c.match(u) {
				fail = true
				c.failedAt = n - 1
				break
			}
		}
	}
	c.mu.Unlock()
	if fail {
		close(c.held)
		<-c.release
		return durable.CheckpointOutput{}, c.err
	}
	return durable.CheckpointOutput{CheckpointToken: "tok-" + strconv.Itoa(n)}, nil
}

// A failed call that carries a START no caller waits for halts the
// checkpointer. The client receives no later call, and the invocation
// ends with the call's *CheckpointError even though the handler catches
// every error it sees and returns a value.
func TestUnawaitedStartFailureEndsInvocation(t *testing.T) {
	wantErr := errors.New("throttled")
	c := newFailClient(func(u durable.OperationUpdate) bool {
		return isStart(u, durable.OperationTypeStep, durable.OperationSubTypeStep) && aws.ToString(u.Name) == "s"
	}, wantErr)
	bodyRan := make(chan struct{})
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		// The handler catches every error: the first step's error and
		// the error of the step after it.
		_, _ = durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			close(bodyRan)
			return "ok", nil
		})
		_, _ = durable.Step(ctx, "after", func(durable.StepContext) (string, error) {
			return "ok", nil
		})
		return "caught", nil
	}, c)

	<-c.held
	<-bodyRan
	close(c.release)
	err := <-done

	var cpErr *durable.CheckpointError
	if !errors.As(err, &cpErr) || cpErr.Scope() != durable.ErrorScopeInvocation || !errors.Is(err, wantErr) {
		t.Fatalf("invocation error = %v, want an invocation-scoped *CheckpointError wrapping %v", err, wantErr)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failedAt != len(c.calls)-1 {
		t.Errorf("calls after the failed call: %+v, want none", c.calls[c.failedAt+1:])
	}
}

// When the handler returns while a START is queued and not yet sent, the
// SDK sends the START before the invocation responds. The client holds the
// call that carries the first child's START, so the second child's START
// stays queued until the handler has returned.
func TestStartQueuedAtHandlerReturnIsSent(t *testing.T) {
	c := newHoldClient(func(u durable.OperationUpdate) bool {
		return isStart(u, durable.OperationTypeContext, durable.OperationSubTypeRunInChildContext) &&
			aws.ToString(u.Name) == "first"
	})
	// The children block until the test ends, so they record nothing
	// after their START.
	stop := make(chan struct{})
	defer close(stop)
	returning := make(chan struct{})
	done := invokeAsync(t, func(ctx durable.Context, _ struct{}) (string, error) {
		defer close(returning)
		for _, name := range []string{"first", "second"} {
			durable.Go(ctx, name, func(durable.Context) (string, error) {
				<-stop
				return name, nil
			})
		}
		return "done", nil
	}, c)

	<-c.held
	<-returning
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("invocation: %v", err)
	}
	updates := c.updates()
	sent := false
	for _, u := range updates {
		if isStart(u, durable.OperationTypeContext, durable.OperationSubTypeRunInChildContext) && aws.ToString(u.Name) == "second" {
			sent = true
		}
	}
	if !sent {
		t.Errorf("updates = %+v, want the START of child %q", updates, "second")
	}
	assertStartFirst(t, updates)
}

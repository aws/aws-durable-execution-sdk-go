// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func isStepAction(action durable.OperationAction) func(durable.OperationUpdate) bool {
	return func(u durable.OperationUpdate) bool {
		return u.Type == durable.OperationTypeStep && u.Action == action
	}
}

// invokeWith starts one invocation of h against c and returns a channel
// that receives the invocation's response status, or "error: ..." when
// the invocation returned an error.
func invokeWith[O any](t *testing.T, h durable.Handler[struct{}, O], c durable.ExecutionClient, opts ...durable.HandlerOption) <-chan string {
	t.Helper()
	input := holdInput(t)
	opts = append([]durable.HandlerOption{durable.WithExecutionClient(c)}, opts...)
	done := make(chan string, 1)
	go func() {
		out, err := durable.Wrap(h, opts...)(context.Background(), input)
		if err != nil {
			done <- "error: " + err.Error()
			return
		}
		var resp struct{ Status string }
		if err := json.Unmarshal(out, &resp); err != nil {
			done <- "error: " + err.Error()
			return
		}
		done <- resp.Status
	}()
	return done
}

// waitHeld waits until c holds a call, and fails when it does not within
// a few seconds.
func waitHeld(t *testing.T, c *holdClient) {
	t.Helper()
	select {
	case <-c.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the client never received the call it holds")
	}
}

// assertNotDone fails when done receives a value within a short window.
func assertNotDone(t *testing.T, done <-chan string, what string) {
	t.Helper()
	select {
	case s := <-done:
		t.Fatalf("%s: invocation ended with %s", what, s)
	case <-time.After(50 * time.Millisecond):
	}
}

// Under AtLeastOnceAsync, Step returns while the call that carries its
// SUCCEED is in flight, and the invocation reports SUCCEEDED only after
// that call returns.
func TestAtLeastOnceAsyncReturnsBeforeSucceedIsRecorded(t *testing.T) {
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	returned := make(chan string, 1)
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		out, err := durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "ok", nil
		}, durable.WithSemantics(durable.AtLeastOnceAsync))
		returned <- out
		return out, err
	}, c)

	waitHeld(t, c)
	select {
	case out := <-returned:
		if out != "ok" {
			t.Fatalf("Step returned %q, want ok", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Step did not return while its SUCCEED was in flight")
	}
	assertNotDone(t, done, "SUCCEED in flight")
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
	assertStartFirst(t, c.updates())
}

// Under AtLeastOnceAsync, a FAIL is still awaited.
func TestAtLeastOnceAsyncWaitsForFail(t *testing.T) {
	returnsAfterHeldCall(t, "Step", isStepAction(durable.OperationActionFail), func(ctx durable.Context) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "", errors.New("boom")
		}, durable.WithSemantics(durable.AtLeastOnceAsync), durable.WithRetry(durable.NoRetry()))
	})
}

// Under AtLeastOnceAsync, a RETRY is still awaited: the step's next
// attempt is not scheduled before the call that carries the RETRY returns.
func TestAtLeastOnceAsyncWaitsForRetry(t *testing.T) {
	c := newHoldClient(isStepAction(durable.OperationActionRetry))
	var attempts atomic.Int32
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			attempts.Add(1)
			return "", errors.New("boom")
		}, durable.WithSemantics(durable.AtLeastOnceAsync))
	}, c)

	waitHeld(t, c)
	assertNotDone(t, done, "RETRY in flight")
	close(c.release)
	if s := <-done; s != "PENDING" {
		t.Fatalf("status = %s, want PENDING", s)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

// A child context's SUCCEED is queued after the SUCCEED of its async step.
func TestAtLeastOnceAsyncChildStepSucceedPrecedesContextSucceed(t *testing.T) {
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.RunInChildContext(ctx, "child", func(cctx durable.Context) (string, error) {
			return durable.Step(cctx, "s", func(durable.StepContext) (string, error) {
				return "ok", nil
			}, durable.WithSemantics(durable.AtLeastOnceAsync))
		})
	}, c)

	waitHeld(t, c)
	assertNotDone(t, done, "SUCCEED in flight")
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
	var order []string
	for _, u := range c.updates() {
		if u.Action == durable.OperationActionSucceed {
			order = append(order, string(u.Type))
		}
	}
	if len(order) != 2 || order[0] != string(durable.OperationTypeStep) || order[1] != string(durable.OperationTypeContext) {
		t.Fatalf("SUCCEED order = %v, want [STEP CONTEXT]", order)
	}
}

// succeedFailClient fails every Checkpoint call that carries a step
// SUCCEED. The failure arrives after a delay, so the handler returns while
// the call is in flight.
type succeedFailClient struct {
	mu sync.Mutex
	n  int
}

func (c *succeedFailClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *succeedFailClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	c.n++
	n := c.n
	c.mu.Unlock()
	for _, u := range in.Updates {
		if isStepAction(durable.OperationActionSucceed)(u) {
			time.Sleep(50 * time.Millisecond)
			return durable.CheckpointOutput{}, errors.New("checkpoint unavailable")
		}
	}
	return durable.CheckpointOutput{CheckpointToken: "tok-" + strconv.Itoa(n)}, nil
}

// A failed SUCCEED call under AtLeastOnceAsync ends the invocation with
// its error, so the invocation does not report SUCCEEDED and the step runs
// again on replay.
func TestAtLeastOnceAsyncFailedSucceedDoesNotReportSucceeded(t *testing.T) {
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "ok", nil
		}, durable.WithSemantics(durable.AtLeastOnceAsync))
	}, &succeedFailClient{})
	if s := <-done; s == "SUCCEEDED" {
		t.Fatal("invocation reported SUCCEEDED over a failed SUCCEED")
	}
}

// The end hook of an AtLeastOnceAsync step reports SUCCEEDED with the
// result and an end time while the SUCCEED is still in flight.
func TestAtLeastOnceAsyncPluginEndBeforeSucceedIsRecorded(t *testing.T) {
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	ends := make(chan durable.OperationHookInfo, 1)
	plugin := durable.PluginFactoryFunc(func(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
		return &durable.Plugin{OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.Type == string(durable.OperationTypeStep) {
				ends <- info
			}
		}}, nil
	})
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "ok", nil
		}, durable.WithSemantics(durable.AtLeastOnceAsync))
	}, c, durable.WithPlugins(plugin))

	waitHeld(t, c)
	select {
	case info := <-ends:
		if info.Status != durable.PluginOperationSucceeded || info.Result != `"ok"` || info.EndTimestamp.IsZero() || info.StartTimestamp.IsZero() {
			t.Fatalf("end = %+v, want SUCCEEDED with result and timestamps", info)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no end while the SUCCEED was in flight")
	}
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
}

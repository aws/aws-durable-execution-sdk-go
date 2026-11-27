// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// ctxHoldClient holds the first call that carries a matching update until
// release is closed. Like a real HTTP client, it fails a call whose
// context is cancelled.
type ctxHoldClient struct {
	mu      sync.Mutex
	match   func(durable.OperationUpdate) bool
	isHeld  bool
	held    chan struct{}
	release chan struct{}
	calls   [][]durable.OperationUpdate
	errs    []error
}

func newCtxHoldClient(match func(durable.OperationUpdate) bool) *ctxHoldClient {
	return &ctxHoldClient{match: match, held: make(chan struct{}), release: make(chan struct{})}
}

// releaseAfterBodyReturns releases the held call once bodyCtx, the context
// of a wrapped child body, is cancelled. The SDK cancels that context when
// the body returns, so the held call is released only after the body
// returned. Call it from inside the body. The timeout only guards against
// a hang.
func (c *ctxHoldClient) releaseAfterBodyReturns(t *testing.T, bodyCtx context.Context) {
	go func() {
		select {
		case <-bodyCtx.Done():
		case <-time.After(10 * time.Second):
			t.Error("the child body context was not cancelled")
		}
		close(c.release)
	}()
}

func (c *ctxHoldClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *ctxHoldClient) Checkpoint(ctx context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	n := len(c.calls) + 1
	hold := false
	if !c.isHeld && slices.ContainsFunc(in.Updates, c.match) {
		hold, c.isHeld = true, true
	}
	c.mu.Unlock()
	if hold {
		close(c.held)
		select {
		case <-c.release:
		case <-ctx.Done():
		}
	}
	err := ctx.Err()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.errs = append(c.errs, err)
		return durable.CheckpointOutput{}, err
	}
	c.calls = append(c.calls, in.Updates)
	return durable.CheckpointOutput{CheckpointToken: "tok-" + strconv.Itoa(n)}, nil
}

func (c *ctxHoldClient) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, call := range c.calls {
		for _, u := range call {
			out = append(out, aws.ToString(u.Name)+":"+string(u.Action))
		}
	}
	return out
}

// wrapChildContextPlugin passes a derived context to every child body, as a
// tracing plugin that opens a span does. The SDK cancels that context when
// the body returns.
func wrapChildContextPlugin() durable.HandlerOption {
	type spanKey struct{}
	return durable.WithPlugins(durable.PluginFactoryFunc(func(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
		return &durable.Plugin{
			WrapChildContextFn: func(ctx context.Context, _ durable.OperationHookInfo, fn func(context.Context) (any, error)) (any, error) {
				return fn(context.WithValue(ctx, spanKey{}, "span"))
			},
		}, nil
	}))
}

func updateNamed(name string, action durable.OperationAction) func(durable.OperationUpdate) bool {
	return func(u durable.OperationUpdate) bool {
		return aws.ToString(u.Name) == name && u.Action == action
	}
}

func asyncStepValue(ctx durable.Context, name, value string) (string, error) {
	return durable.Step(ctx, name, func(durable.StepContext) (string, error) { return value, nil },
		durable.WithSemantics(durable.AtLeastOnceAsync))
}

// A queued async SUCCEED in a wrapped child body is sent, in order, after
// the body returns.
func TestAtLeastOnceAsyncSucceedInWrappedChildIsSent(t *testing.T) {
	c := newCtxHoldClient(updateNamed("pre", durable.OperationActionSucceed))
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		if _, err := asyncStepValue(ctx, "pre", "p"); err != nil {
			return "", err
		}
		// The pre SUCCEED call is held, so the child's updates queue behind it
		// until the child body has returned.
		return durable.RunInChildContext(ctx, "child", func(cctx durable.Context) (string, error) {
			c.releaseAfterBodyReturns(t, cctx)
			return asyncStepValue(cctx, "a", "A")
		})
	}, c, wrapChildContextPlugin())

	if got := <-done; got != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED; recorded %v, errors %v", got, c.recorded(), c.errs)
	}
	rec := c.recorded()
	succeedA := slices.Index(rec, "a:SUCCEED")
	succeedChild := slices.Index(rec, "child:SUCCEED")
	if succeedA < 0 || succeedChild < succeedA {
		t.Fatalf("recorded %v: want a:SUCCEED before child:SUCCEED", rec)
	}
}

// The call that carries an async SUCCEED is not cancelled when the wrapped
// child body returns while the call is in flight.
func TestAtLeastOnceAsyncSucceedCallOutlivesWrappedChild(t *testing.T) {
	c := newCtxHoldClient(updateNamed("a", durable.OperationActionSucceed))
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.RunInChildContext(ctx, "child", func(cctx durable.Context) (string, error) {
			c.releaseAfterBodyReturns(t, cctx)
			out, err := asyncStepValue(cctx, "a", "A")
			<-c.held // the SUCCEED call is in flight when the body returns
			return out, err
		})
	}, c, wrapChildContextPlugin())

	if got := <-done; got != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED; errors %v", got, c.errs)
	}
	if len(c.errs) != 0 {
		t.Fatalf("checkpoint calls failed: %v", c.errs)
	}
}

// valueClient records, for each checkpoint call, the updates it carries and
// the value its context holds for key.
type valueClient struct {
	key   any
	mu    sync.Mutex
	calls []valueCall
}

type valueCall struct {
	updates []string
	value   any
}

func (c *valueClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *valueClient) Checkpoint(ctx context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var names []string
	for _, u := range in.Updates {
		names = append(names, aws.ToString(u.Name)+":"+string(u.Action))
	}
	c.calls = append(c.calls, valueCall{updates: names, value: ctx.Value(c.key)})
	return durable.CheckpointOutput{CheckpointToken: "tok-" + strconv.Itoa(len(c.calls))}, nil
}

// The call that carries a no-wait START or async SUCCEED sees the context
// values a WrapInvocation hook added.
func TestAtLeastOnceAsyncCallKeepsWrapInvocationValues(t *testing.T) {
	type traceKey struct{}
	c := &valueClient{key: traceKey{}}
	plugin := durable.WithPlugins(durable.PluginFactoryFunc(func(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
		return &durable.Plugin{
			WrapInvocation: func(ctx context.Context, _ durable.InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
				return fn(context.WithValue(ctx, traceKey{}, "trace"))
			},
		}, nil
	}))
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return asyncStepValue(ctx, "a", "A")
	}, c, plugin)

	if got := <-done; got != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", got)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var sawStart, sawSucceed bool
	for _, call := range c.calls {
		start := slices.Contains(call.updates, "a:START")
		succeed := slices.Contains(call.updates, "a:SUCCEED")
		if !start && !succeed {
			continue
		}
		sawStart = sawStart || start
		sawSucceed = sawSucceed || succeed
		if call.value != "trace" {
			t.Errorf("call %v: context value = %v, want trace", call.updates, call.value)
		}
	}
	if !sawStart || !sawSucceed {
		t.Fatalf("calls %v: want a:START and a:SUCCEED", c.calls)
	}
}

// The checkpoint Debug records of a no-wait request in a child context go
// to the handler the child installed with ConfigureLogging.
func TestAtLeastOnceAsyncDebugRecordsKeepChildLogScope(t *testing.T) {
	c := newCtxHoldClient(func(durable.OperationUpdate) bool { return false })
	sink := &attrSink{}
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.RunInChildContext(ctx, "child", func(cctx durable.Context) (string, error) {
			if err := durable.ConfigureLogging(cctx, durable.LogConfig{
				Handler: attrHandler{sink: sink, level: slog.LevelDebug},
			}); err != nil {
				return "", err
			}
			return asyncStepValue(cctx, "a", "A")
		})
	}, c, wrapChildContextPlugin())

	if got := <-done; got != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED; errors %v", got, c.errs)
	}
	var enqueued int
	for _, r := range sink.records() {
		if r.msg == "checkpoint enqueued" {
			enqueued++
		}
	}
	// The child records the START and the SUCCEED of a.
	if enqueued < 2 {
		t.Fatalf("child handler got %d checkpoint enqueued records, want at least 2; records %v", enqueued, sink.records())
	}
}

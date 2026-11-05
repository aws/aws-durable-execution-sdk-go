// Command plugin-lifecycle demonstrates registering a [durable.Plugin] via
// [durable.WithPlugins] and observing the full hook firing sequence during a
// single-step execution: OnInvocationStart → OnOperationStart →
// OnOperationAttemptStart → OnOperationAttemptEnd → OnOperationEnd →
// OnInvocationEnd.
//
// It also shows how a plugin hands per-invocation state to the handler.
// The factory creates one recorder per invocation. The WrapInvocation hook
// attaches that recorder to the context it passes to the handler, and the
// handler reads it back from its [durable.Context]. So two invocations
// running at the same time in one process never share a recorder.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// hookEvent records one hook invocation with its name and key metadata.
type hookEvent struct {
	Hook          string `json:"hook"`
	OperationName string `json:"operationName,omitempty"`
	Attempt       int    `json:"attempt,omitempty"`
}

// recorder collects hook events in invocation order. It is safe for
// concurrent use because the plugin dispatch contract permits hooks to fire
// from multiple goroutines when parallel operations are active.
type recorder struct {
	mu     sync.Mutex
	events []hookEvent
}

func (r *recorder) record(e hookEvent) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []hookEvent {
	r.mu.Lock()
	cp := make([]hookEvent, len(r.events))
	copy(cp, r.events)
	r.mu.Unlock()
	return cp
}

// recorderKey is the context key under which WrapInvocation passes the
// invocation's recorder to the handler.
type recorderKey struct{}

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. Each
// call creates a new recorder, so the hooks of one invocation never write
// into the events of another.
func newPlugin(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
	return pluginFor(&recorder{}), nil
}

// pluginFor returns the Plugin whose hooks record into rec. WrapInvocation
// runs the handler with rec attached to the context, so the handler can
// read the same recorder its plugin writes to.
func pluginFor(rec *recorder) *durable.Plugin {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, _ durable.InvocationHookInfo) {
			rec.record(hookEvent{Hook: "OnInvocationStart"})
		},
		WrapInvocation: func(ctx context.Context, _ durable.InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(ctx, recorderKey{}, rec))
		},
		OnInvocationEnd: func(_ context.Context, _ durable.InvocationEndHookInfo) {
			rec.record(hookEvent{Hook: "OnInvocationEnd"})
		},
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			rec.record(hookEvent{Hook: "OnOperationStart", OperationName: info.Name})
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			rec.record(hookEvent{Hook: "OnOperationEnd", OperationName: info.Name})
		},
		OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
			rec.record(hookEvent{Hook: "OnOperationAttemptStart", OperationName: info.Name, Attempt: info.Attempt})
		},
		OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
			rec.record(hookEvent{Hook: "OnOperationAttemptEnd", OperationName: info.Name, Attempt: info.Attempt})
		},
	}
}

// Output is the handler result containing the hooks observed before the
// handler returns. OnInvocationEnd fires after the handler completes, so it
// does not appear here.
type Output struct {
	Message string      `json:"message"`
	Hooks   []hookEvent `json:"hooks"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	rec, ok := ctx.Value(recorderKey{}).(*recorder)
	if !ok {
		return Output{}, errors.New("no recorder in context: register the plugin with durable.WithPlugins")
	}
	msg, err := durable.Step(ctx, "compute", func(_ durable.StepContext) (string, error) {
		return "plugin lifecycle complete", nil
	})
	if err != nil {
		return Output{}, fmt.Errorf("step failed: %w", err)
	}
	return Output{Message: msg, Hooks: rec.snapshot()}, nil
}

func main() { durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin))) }

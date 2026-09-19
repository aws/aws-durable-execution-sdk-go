// Command plugin-lifecycle demonstrates registering a [durable.Plugin] via
// [durable.WithPlugins] and observing the full hook firing sequence during a
// single-step execution: OnInvocationStart → OnOperationStart →
// OnOperationAttemptStart → OnOperationAttemptEnd → OnOperationEnd →
// OnInvocationEnd.
package main

import (
	"context"
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

// reset discards the events of earlier invocations. A warm Lambda
// container keeps package-level state between invocations, so without it
// the output would carry the hooks of every invocation the container has
// served.
func (r *recorder) reset() {
	r.mu.Lock()
	r.events = nil
	r.mu.Unlock()
}

func (r *recorder) snapshot() []hookEvent {
	r.mu.Lock()
	cp := make([]hookEvent, len(r.events))
	copy(cp, r.events)
	r.mu.Unlock()
	return cp
}

// rec is the package-level recorder shared between the plugin and handler.
var rec = &recorder{}

// plugin records every lifecycle hook invocation into rec.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, _ durable.InvocationHookInfo) {
		rec.reset()
		rec.record(hookEvent{Hook: "OnInvocationStart"})
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

// Output is the handler result containing the hooks observed before the
// handler returns. OnInvocationEnd fires after the handler completes, so it
// does not appear here.
type Output struct {
	Message string      `json:"message"`
	Hooks   []hookEvent `json:"hooks"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	msg, err := durable.Step(ctx, "compute", func(_ durable.StepContext) (string, error) {
		return "plugin lifecycle complete", nil
	})
	if err != nil {
		return Output{}, fmt.Errorf("step failed: %w", err)
	}
	return Output{Message: msg, Hooks: rec.snapshot()}, nil
}

func main() { durable.Start(handler, durable.WithPlugins(plugin)) }

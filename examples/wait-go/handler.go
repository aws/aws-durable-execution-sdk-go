// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/simple-step-go's
// own handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// CoolDownEvent is this example's input shape.
type CoolDownEvent struct {
	OrderID string `json:"orderId"`
	Seconds int    `json:"seconds"`
}

// CoolDownResult is this example's output shape.
type CoolDownResult struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

// BasicHandler demonstrates the simplest possible operations.Wait usage:
// pause execution for a fixed duration (no compute charges incurred while
// suspended), then continue. Mirrors the JS reference SDK's own
// wait/basic example ("Basic usage of context.wait() to pause execution").
func BasicHandler(event CoolDownEvent, dc types.DurableContext) (CoolDownResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID})

	if err := operations.Wait(dc, "cool-down", types.Duration{Seconds: 1}); err != nil {
		return CoolDownResult{}, fmt.Errorf("order %s: %w", event.OrderID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"orderId": event.OrderID})
	return CoolDownResult{OrderID: event.OrderID, Status: "cooled-down"}, nil
}

// ConfigurableHandler demonstrates a Wait whose duration comes from the
// event payload rather than being hardcoded - mirroring the JS reference
// SDK's own wait/configurable example ("Wait with a configurable duration
// passed via the event payload").
func ConfigurableHandler(event CoolDownEvent, dc types.DurableContext) (CoolDownResult, error) {
	seconds := event.Seconds
	if seconds <= 0 {
		seconds = 1
	}

	if err := operations.Wait(dc, "configurable-cool-down", types.Duration{Seconds: seconds}); err != nil {
		return CoolDownResult{}, fmt.Errorf("order %s: %w", event.OrderID, err)
	}

	return CoolDownResult{OrderID: event.OrderID, Status: "cooled-down"}, nil
}

// NamedHandler demonstrates giving a Wait a caller-chosen, descriptive
// name (rather than letting every Wait/Step in a handler collide on the
// same generic id) - mirroring the JS reference SDK's own wait/named
// example ("Using context.wait() with a custom name").
func NamedHandler(event CoolDownEvent, dc types.DurableContext) (CoolDownResult, error) {
	if err := operations.Wait(dc, "await-payment-settlement", types.Duration{Seconds: 1}); err != nil {
		return CoolDownResult{}, fmt.Errorf("order %s: %w", event.OrderID, err)
	}

	return CoolDownResult{OrderID: event.OrderID, Status: "settled"}, nil
}

// UnawaitedHandler demonstrates that a durable execution can complete
// successfully even when it never actually resumes past a scheduled Wait
// - the handler here calls operations.Wait but IGNORES its returned
// error (the SDK's suspend-or-complete race in
// durable.WithDurableExecution simply lets the containing goroutine keep
// running and return before the Wait itself would ever resolve, since
// nothing later in the handler depends on it). Mirrors the JS reference
// SDK's own wait/unawaited example ("Demonstrates scheduling a wait
// operation without awaiting it - function completes immediately while
// wait is scheduled").
//
// NOTE: unlike JS's fire-and-forget Promise semantics, Go's operations.Wait
// is a plain, synchronous, blocking function call - there is no
// language-level way to "schedule but not await" it the way JS's
// un-awaited Promise does. This handler demonstrates the closest Go
// equivalent: running the Wait in its own goroutine and returning a
// result without ever synchronizing on that goroutine's completion. This
// is included for parity with the JS reference SDK's own example
// catalog, but is NOT a recommended pattern for production durable
// handlers - a goroutine that outlives the handler's own return has no
// guaranteed opportunity to run to completion at all once the invocation
// itself ends.
func UnawaitedHandler(event CoolDownEvent, dc types.DurableContext) (CoolDownResult, error) {
	go func() {
		_ = operations.Wait(dc, "background-cool-down", types.Duration{Seconds: 30})
	}()

	return CoolDownResult{OrderID: event.OrderID, Status: "scheduled"}, nil
}

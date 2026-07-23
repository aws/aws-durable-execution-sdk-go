// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TimeoutEvent is this example's input shape.
type TimeoutEvent struct {
	RequestID string `json:"requestId"`
}

// TimeoutResult is this example's output shape.
type TimeoutResult struct {
	RequestID string `json:"requestId"`
	Success   bool   `json:"success"`
	TimedOut  bool   `json:"timedOut"`
	Error     string `json:"error,omitempty"`
}

// handler demonstrates operations.WithWaitForCallbackTimeout: the
// submitter succeeds immediately, but no external system EVER completes
// the callback - after the configured timeout elapses, the backend
// unilaterally fails the callback with a *operations.CallbackFailedError
// whose Timeout field is true. Mirrors the JS reference SDK's own
// wait-for-callback/timeout example ("Demonstrates waitForCallback
// timeout scenarios").
//
// Unlike the JS reference SDK (which exposes three distinct instanceof-able
// error types - CallbackError/CallbackSubmitterError/CallbackTimeoutError -
// see wait-for-callback/error-instance-* examples, not yet ported to this
// Go SDK for exactly this reason: no equivalent distinct types exist
// here), this Go SDK surfaces every callback failure through the SAME
// concrete *operations.CallbackFailedError type, distinguishing a timeout
// from an explicit external failure via that single type's own Timeout
// bool field - checked here via the standard library's errors.As, since
// CallbackFailedError may be wrapped by intermediate layers (e.g. a
// ChildContextFailedError, since WaitForCallback is implemented as
// RunInChildContext(dc, id, fn) - see WaitForCallback's own doc).
//
// NOTE: the actual TIMEOUT path this handler is built to demonstrate
// (the submitter succeeds, but nothing ever calls
// SendDurableExecutionCallbackSuccess/Failure, so the backend itself
// unilaterally times the callback out) can only be exercised against a
// REAL deployment - testing.LocalTestRunner's fake in-memory client has
// no mechanism to simulate a backend-driven timeout (only explicit
// SendCallbackSuccess/SendCallbackFailure, both of which map to
// OperationStatusFailed, never OperationStatusTimedOut - see this
// example's own handler_test.go for the full explanation and the
// distinct, locally-testable scenario it exercises instead).
func handler(event TimeoutEvent, dc types.DurableContext) (TimeoutResult, error) {
	result, err := operations.WaitForCallback[string](dc, "never-completes-callback",
		func(sc types.StepContext, callbackID string) error {
			// Submitter succeeds immediately - the callback is
			// registered, but no external system will ever resolve it
			// in this example.
			return nil
		},
		operations.WithWaitForCallbackTimeout[string](types.Duration{Seconds: 1}),
	)
	if err != nil {
		var callbackErr *operations.CallbackFailedError
		timedOut := errors.As(err, &callbackErr) && callbackErr.Timeout
		return TimeoutResult{RequestID: event.RequestID, Success: false, TimedOut: timedOut, Error: err.Error()}, nil
	}

	return TimeoutResult{RequestID: event.RequestID, Success: true, Error: result}, nil
}

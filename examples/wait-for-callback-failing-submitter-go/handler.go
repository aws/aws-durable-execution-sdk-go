// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// SubmitterAttemptEvent is this example's input shape.
type SubmitterAttemptEvent struct {
	RequestID string `json:"requestId"`
}

// SubmitterAttemptResult is this example's output shape.
type SubmitterAttemptResult struct {
	RequestID string `json:"requestId"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
}

// handler demonstrates operations.WithWaitForCallbackSubmitterRetryStrategy:
// the SUBMITTER function itself (not the callback's own timeout) can
// fail, and - like any Step - be retried according to a caller-supplied
// retry policy before the whole WaitForCallback operation gives up.
// Mirrors the JS reference SDK's own wait-for-callback/failing-submitter
// example ("Demonstrates waitForCallback with submitter function that
// fails"), which configures a 3-attempt submitter retry policy.
//
// This example's submitter ALWAYS fails, so the retry policy exhausts
// and the whole operation ultimately fails - the handler catches that
// error and reports it in the result rather than letting it propagate,
// matching the JS example's own try/catch structure.
func handler(event SubmitterAttemptEvent, dc types.DurableContext) (SubmitterAttemptResult, error) {
	result, err := operations.WaitForCallback[string](dc, "failing-submitter-callback",
		func(sc types.StepContext, callbackID string) error {
			return fmt.Errorf("submitter failed on attempt %d", sc.Attempt())
		},
		operations.WithWaitForCallbackSubmitterRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			if attempt >= 3 {
				return types.RetryDecision{ShouldRetry: false}
			}
			return types.RetryDecision{ShouldRetry: true, Delay: &types.Duration{Seconds: 1}}
		}),
	)
	if err != nil {
		return SubmitterAttemptResult{RequestID: event.RequestID, Success: false, Error: err.Error()}, nil
	}

	return SubmitterAttemptResult{RequestID: event.RequestID, Success: true, Error: result}, nil
}

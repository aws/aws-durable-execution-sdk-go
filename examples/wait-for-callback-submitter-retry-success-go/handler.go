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

// RetrySuccessEvent is this example's input shape.
type RetrySuccessEvent struct {
	RequestID        string `json:"requestId"`
	FailUntilAttempt int    `json:"failUntilAttempt"`
}

// RetrySuccessResult is this example's output shape.
type RetrySuccessResult struct {
	RequestID string `json:"requestId"`
	Result    string `json:"result"`
	Success   bool   `json:"success"`
}

// handler demonstrates operations.WithWaitForCallbackSubmitterRetryStrategy
// on the SUCCESS path: the submitter fails on its first few attempts, then
// succeeds once FailUntilAttempt is reached - unlike
// examples/wait-for-callback-failing-submitter-go (whose submitter ALWAYS
// fails, exhausting every retry), demonstrating that a transient
// submitter failure recovers correctly mid-retry. Mirrors the JS
// reference SDK's own wait-for-callback/submitter-retry-success example
// ("Demonstrates waitForCallback with submitter retry strategy using
// exponential backoff").
func handler(event RetrySuccessEvent, dc types.DurableContext) (RetrySuccessResult, error) {
	failUntil := event.FailUntilAttempt
	if failUntil <= 0 {
		failUntil = 1
	}

	result, err := operations.WaitForCallback[string](dc, "retry-submitter-callback",
		func(sc types.StepContext, callbackID string) error {
			if sc.Attempt() < failUntil {
				return fmt.Errorf("simulated submitter failure on attempt %d", sc.Attempt())
			}
			sc.Logger().Info("submitter succeeded", map[string]any{"attempt": sc.Attempt(), "callbackId": callbackID})
			return nil
		},
		operations.WithWaitForCallbackSubmitterRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			if attempt >= 4 {
				return types.RetryDecision{ShouldRetry: false}
			}
			// Exponential backoff: 1s, 2s, 4s between retries.
			delaySeconds := 1
			for i := 1; i < attempt; i++ {
				delaySeconds *= 2
			}
			return types.RetryDecision{ShouldRetry: true, Delay: &types.Duration{Seconds: delaySeconds}}
		}),
	)
	if err != nil {
		return RetrySuccessResult{}, err
	}

	return RetrySuccessResult{RequestID: event.RequestID, Result: result, Success: true}, nil
}

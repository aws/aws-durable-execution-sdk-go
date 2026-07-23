// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// MultiInvocationEvent is this example's input shape.
type MultiInvocationEvent struct {
	RequestID string `json:"requestId"`
}

// MultiInvocationResult is this example's output shape.
type MultiInvocationResult struct {
	RequestID       string `json:"requestId"`
	FirstCallback   string `json:"firstCallback"`
	SecondCallback  string `json:"secondCallback"`
	StepProcessed   bool   `json:"stepProcessed"`
	InvocationCount string `json:"invocationCount"`
}

// handler demonstrates that operation checkpointing/replay tracking
// works correctly across MANY suspend/resume cycles within a single
// handler - two separate Wait operations and two separate WaitForCallback
// operations, interleaved with a Step, each suspending the invocation
// independently. Mirrors the JS reference SDK's own
// wait-for-callback/multiple-invocations example ("Demonstrates multiple
// invocations tracking with waitForCallback operations across different
// invocations").
func handler(event MultiInvocationEvent, dc types.DurableContext) (MultiInvocationResult, error) {
	if err := operations.Wait(dc, "wait-invocation-1", types.Duration{Seconds: 1}); err != nil {
		return MultiInvocationResult{}, err
	}

	firstCallback, err := operations.WaitForCallback[string](dc, "first-callback",
		func(sc types.StepContext, callbackID string) error { return nil },
	)
	if err != nil {
		return MultiInvocationResult{}, err
	}

	stepResult, err := operations.Step(dc, "process-callback-data", func(sc types.StepContext) (bool, error) {
		return true, nil
	})
	if err != nil {
		return MultiInvocationResult{}, err
	}

	if err := operations.Wait(dc, "wait-invocation-2", types.Duration{Seconds: 1}); err != nil {
		return MultiInvocationResult{}, err
	}

	secondCallback, err := operations.WaitForCallback[string](dc, "second-callback",
		func(sc types.StepContext, callbackID string) error { return nil },
	)
	if err != nil {
		return MultiInvocationResult{}, err
	}

	return MultiInvocationResult{
		RequestID:       event.RequestID,
		FirstCallback:   firstCallback,
		SecondCallback:  secondCallback,
		StepProcessed:   stepResult,
		InvocationCount: "multiple",
	}, nil
}

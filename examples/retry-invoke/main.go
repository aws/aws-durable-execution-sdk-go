// Command retry-invoke demonstrates retrying a durable Invoke operation
// end-to-end. Each retry invokes the target function (itself, in this
// example) with an incremented attempt counter. The target succeeds once
// the attempt number reaches the configured threshold.
//
// This is the Go equivalent of the JS withRetry(ctx, name, (ctx,attempt)=>ctx.invoke(...))
// pattern. In Go, the retry loop is explicit and idiomatic.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input is the event for the orchestrator entry point.
type Input struct {
	// TargetFunction is the Lambda function ARN to invoke.
	TargetFunction string `json:"targetFunction"`
	// FailUntilAttempt controls when the target starts succeeding.
	FailUntilAttempt int `json:"failUntilAttempt"`
	// MaxAttempts caps the retry loop.
	MaxAttempts int `json:"maxAttempts"`
}

// TargetInput is what the orchestrator sends to the target function.
type TargetInput struct {
	Attempt          int `json:"attempt"`
	FailUntilAttempt int `json:"failUntilAttempt"`
}

// Result summarizes the invocation outcome.
type Result struct {
	Response json.RawMessage `json:"response"`
	Attempts int             `json:"attempts"`
}

func handler(ctx durable.Context, event Input) (Result, error) {
	targetFunction := event.TargetFunction
	if targetFunction == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		targetFunction = prefix + "go-retry-invoke-target:$LATEST"
	}

	maxAttempts := event.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	failUntil := event.FailUntilAttempt
	if failUntil == 0 {
		failUntil = 3
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		name := fmt.Sprintf("invoke-%d", attempt)
		result, err := durable.Invoke[json.RawMessage](ctx, name, targetFunction,
			TargetInput{Attempt: attempt, FailUntilAttempt: failUntil})
		if err == nil {
			return Result{Response: result, Attempts: attempt}, nil
		}
		lastErr = err

		// Wait before retrying (required minimum 1s for checkpoint).
		if attempt < maxAttempts {
			waitName := fmt.Sprintf("retry-wait-%d", attempt)
			if wErr := durable.Wait(ctx, waitName, 1*time.Second); wErr != nil {
				return Result{}, wErr
			}
		}
	}
	return Result{}, fmt.Errorf("invoke exhausted after %d attempts: %w", maxAttempts, lastErr)
}

func main() { durable.Start(handler) }

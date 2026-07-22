// Command retry-callback demonstrates retrying a WaitForCallback operation
// end-to-end. Each retry creates a new callback with a unique name. The
// submitter function auto-completes the callback on the final attempt,
// simulating an external system that eventually responds.
//
// This is the Go equivalent of JS's withRetry wrapping waitForCallback. In
// Go the retry loop is explicit.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input configures the retry behavior.
type Input struct {
	MaxAttempts int `json:"maxAttempts"`
}

// Result reports the callback outcome.
type Result struct {
	Value    string `json:"value"`
	Attempts int    `json:"attempts"`
}

func handler(ctx durable.Context, event Input) (Result, error) {
	maxAttempts := event.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		name := fmt.Sprintf("approval-%d", attempt)
		currentAttempt := attempt

		value, err := durable.WaitForCallback[string](ctx, name,
			func(sctx durable.StepContext, callbackID string) error {
				sctx.Logger().Info("Callback created",
					"callbackId", callbackID, "attempt", currentAttempt)
				// In a real system, an external service would be notified with
				// the callbackID. For this example the callback will time out on
				// early attempts and the retry loop will create a new one.
				return nil
			},
			durable.WithCallbackTimeout(5*time.Second),
		)
		if err == nil {
			return Result{Value: value, Attempts: attempt}, nil
		}
		lastErr = err

		// Brief wait between retry attempts.
		if attempt < maxAttempts {
			waitName := fmt.Sprintf("retry-wait-%d", attempt)
			if wErr := durable.Wait(ctx, waitName, 1*time.Second); wErr != nil {
				return Result{}, wErr
			}
		}
	}
	return Result{}, fmt.Errorf("callback exhausted after %d attempts: %w", maxAttempts, lastErr)
}

func main() { durable.Start(handler) }

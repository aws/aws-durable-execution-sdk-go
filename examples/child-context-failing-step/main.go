// Command child-context-failing-step demonstrates
// [durable.RunInChildContext] with a step that exhausts its retries,
// followed by a wait that succeeds — proving the execution continues
// past a caught child-context failure.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result confirms the execution succeeded past the failure.
type Result struct {
	Success bool `json:"success"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.RunInChildContext(ctx, "child-with-failure",
		func(child durable.Context) (any, error) {
			_, stepErr := durable.Step(child, "failing-step",
				func(_ durable.StepContext) (any, error) {
					return nil, fmt.Errorf("step failed in child context")
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{MaxAttempts: 3})))
			return nil, stepErr
		})

	if err != nil {
		// Only swallow ChildContextError; rethrow anything else.
		var childErr *durable.ChildContextError
		if !errors.As(err, &childErr) {
			return Result{}, err
		}
	}

	// Continue past the failure with a short wait.
	if err := durable.Wait(ctx, "wait-after-failure", 1*time.Second); err != nil {
		return Result{}, err
	}

	return Result{Success: true}, nil
}

func main() { durable.Start(handler) }

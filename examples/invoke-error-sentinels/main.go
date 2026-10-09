// Command invoke-error-sentinels demonstrates telling apart the ways an
// invoked function can end without a result. When the invoked execution
// does not succeed, [durable.Invoke] returns an [*durable.InvokeError].
// For the three terminal statuses the service assigns from outside the
// invoked function, that error unwraps to a sentinel:
//
//   - [durable.ErrInvokeTimedOut]: the invoked execution ran past its
//     execution timeout.
//   - [durable.ErrExecutionStopped]: the invoked execution was stopped
//     with the StopDurableExecution API.
//   - [durable.ErrExecutionCancelled]: the invoked execution was
//     cancelled.
//
// The handler matches them with errors.Is, which holds on the first
// invocation and on replay because the error is rebuilt from the recorded
// status. In those three cases the invoked function produced no answer,
// so the handler returns an outcome naming the status instead of failing.
// An invoked function that fails with an error of its own matches none of
// the sentinels: the handler returns that error and the execution fails.
package main

import (
	"errors"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and the order to process.
type Input struct {
	TargetFunction string `json:"targetFunction"`
	OrderID        string `json:"orderId"`
}

// StageRequest is sent to the target function.
type StageRequest struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
}

// StageResult is returned by the target function.
type StageResult struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
	Status  string `json:"status"`
}

// Output reports how the invoke ended. Outcome is "completed" when the
// target returned a result, and "timed-out", "stopped", or "cancelled"
// when its execution ended without one. Result is set only when Outcome
// is "completed".
type Output struct {
	Outcome string       `json:"outcome"`
	Result  *StageResult `json:"result,omitempty"`
}

func handler(ctx durable.Context, event Input) (Output, error) {
	targetFunction := event.TargetFunction
	if targetFunction == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		targetFunction = prefix + "go-invoke-simple-target:$LATEST"
	}

	orderID := event.OrderID
	if orderID == "" {
		orderID = "ORD-DEFAULT"
	}

	result, err := durable.Invoke[StageResult](ctx, "quote", targetFunction,
		StageRequest{OrderID: orderID, Stage: "quote"})
	switch {
	case err == nil:
		return Output{Outcome: "completed", Result: &result}, nil
	case errors.Is(err, durable.ErrInvokeTimedOut):
		return Output{Outcome: "timed-out"}, nil
	case errors.Is(err, durable.ErrExecutionStopped):
		return Output{Outcome: "stopped"}, nil
	case errors.Is(err, durable.ErrExecutionCancelled):
		return Output{Outcome: "cancelled"}, nil
	default:
		// The invoked function failed with its own error, or the
		// invocation is suspending while the invoke runs. Return it
		// unchanged.
		return Output{}, err
	}
}

func main() { durable.Start(handler) }

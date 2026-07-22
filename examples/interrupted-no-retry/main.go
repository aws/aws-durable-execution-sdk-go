// Command interrupted-no-retry demonstrates step interruption handling with
// AT_MOST_ONCE_PER_RETRY semantics and no retry. When a step body runs
// longer than the Lambda function timeout, the SDK detects the interruption
// on the next invocation and reports it as a StepError wrapping a
// StepInterruptedError.
//
// This example requires a short Lambda function timeout (configured in the
// SAM template) so the step is reliably interrupted mid-execution. The
// overall durable execution timeout is longer, allowing the framework to
// replay and report the failure.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input optionally overrides the step sleep duration.
type Input struct {
	StepDurationMs int `json:"stepDurationMs"`
}

// Result reports the outcome of the interrupted step.
type Result struct {
	Status    string `json:"status"`
	ErrorName string `json:"errorName,omitempty"`
	Message   string `json:"message,omitempty"`
	CauseName string `json:"causeName,omitempty"`
}

func handler(ctx durable.Context, event Input) (Result, error) {
	sleepMs := event.StepDurationMs
	if sleepMs == 0 {
		sleepMs = 30000 // must exceed per-function Lambda Timeout
	}

	result, err := durable.Step(ctx, "long-running-step",
		func(_ durable.StepContext) (string, error) {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			return "step-completed", nil
		},
		durable.WithSemantics(durable.AtMostOncePerRetry),
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		var stepErr *durable.StepError
		if errors.As(err, &stepErr) {
			r := Result{
				Status:    "failed",
				ErrorName: "StepError",
				Message:   stepErr.Error(),
			}
			var interrupted *durable.StepInterruptedError
			if errors.As(err, &interrupted) {
				r.CauseName = "StepInterruptedError"
			}
			return r, nil
		}
		return Result{Status: "failed", Message: err.Error()}, nil
	}
	return Result{Status: "succeeded", Message: result}, nil
}

func main() { durable.Start(handler) }

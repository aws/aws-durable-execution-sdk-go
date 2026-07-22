// Command retry-invoke-target is the target function invoked by retry-invoke.
// It fails until the attempt number reaches failUntilAttempt, then succeeds.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TargetInput matches what the orchestrator sends.
type TargetInput struct {
	Attempt          int `json:"attempt"`
	FailUntilAttempt int `json:"failUntilAttempt"`
}

// TargetResult is the success response.
type TargetResult struct {
	Message string `json:"message"`
	Attempt int    `json:"attempt"`
}

func handler(_ durable.Context, event TargetInput) (TargetResult, error) {
	if event.Attempt < event.FailUntilAttempt {
		return TargetResult{}, fmt.Errorf("deliberate failure on attempt %d", event.Attempt)
	}
	return TargetResult{
		Message: fmt.Sprintf("success on attempt %d", event.Attempt),
		Attempt: event.Attempt,
	}, nil
}

func main() { durable.Start(handler) }

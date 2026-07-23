// Requirement 1-11: Step with retry (fails then succeeds).
//
// From test-requirements/step/1-11.yaml:
//
//	description: Step with retry (fails then succeeds)
//	handler: |
//	  A step that fails on the first attempt and succeeds on the second,
//	  with a retry strategy configured.
//	invocations: |
//	  - Handler invokes
//	    `context.step(unreliable_func, config={ retryStrategy: ... })`,
//	    step throws an error, SDK checkpoints StepFailed with RetryDetails
//	    (CurrentAttempt=1, NextAttemptDelaySeconds=1), invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because retry delay elapsed, SDK replays past
//	    checkpoints, re-executes the step, step succeeds, SDK checkpoints
//	    StepSucceeded with RetryDetails (CurrentAttempt=2), execution
//	    succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: Operation succeeded
//
// sc.Attempt() (see step.go's dcontext.NewStepContext, and
// examples/retry-go/handler.go's identical callFlakyDependency pattern)
// is the SDK's own checkpointed, replay-safe attempt counter - it is
// reconstructed from the checkpointed StepDetails.Attempt on every
// invocation, so branching on it (rather than a package-level Go
// variable, which would NOT survive a fresh process/invocation) is the
// correct, real way to make a step's body behave differently on its
// first vs. second attempt. A utils.Presets.FixedDelay(1s, maxAttempts)
// strategy produces exactly the NextAttemptDelaySeconds=1 the YAML's
// ExpectedExecutionHistory RetryDetails specifies.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("1-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_11Handler, config(client))
	})
}

func step1_11Handler(event any, dc types.DurableContext) (string, error) {
	strategy := utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 5)
	return operations.Step(dc, "unreliable_func", func(sc types.StepContext) (string, error) {
		if sc.Attempt() < 2 {
			return "", errors.New("intentional transient failure for conformance requirement 1-11")
		}
		return "Operation succeeded", nil
	}, operations.WithStepRetryStrategy[string](strategy))
}

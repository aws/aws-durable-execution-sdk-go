// Requirement 3-7: Child context with step retry (fails then succeeds).
//
// From test-requirements/child/3-7.yaml:
//
//	description: Child context with step retry (fails then succeeds)
//	handler: |
//	  A child context containing a step that fails on the first attempt
//	  and succeeds on the second, with a retry strategy configured on the
//	  step.
//	  The step returns the input string on the successful attempt.
//	  The child context returns the step result.
//	invocations: |
//	  - Handler invokes a child context with a step that has a retry
//	    strategy configured. ContextStarted, StepStarted, step fails,
//	    StepFailed with RetryDetails, invocation completes, execution
//	    suspends.
//	  - Replay 1: Re-invoked because retry delay elapsed. StepStarted,
//	    step succeeds, StepSucceeded with RetryDetails, ContextSucceeded,
//	    execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// Identical retry mechanics to step_1_11.go's own "fails then succeeds"
// step (sc.Attempt()'s checkpointed, replay-safe counter branches the
// body), but the step is nested inside a RunInChildContext call rather
// than at the top level - exercising the same suspend/resume-across-a-
// retry-delay behavior the briefing describes as fully backend-driven
// once the retry checkpoint is issued, now nested one level under a
// child context's own step-ID namespace.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("3-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_7Handler, config(client))
	})
}

func child3_7Handler(event string, dc types.DurableContext) (string, error) {
	strategy := utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 5)
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "unreliable_step", func(sc types.StepContext) (string, error) {
			if sc.Attempt() < 2 {
				return "", errors.New("intentional transient failure for conformance requirement 3-7")
			}
			return event, nil
		}, operations.WithStepRetryStrategy[string](strategy))
	})
}

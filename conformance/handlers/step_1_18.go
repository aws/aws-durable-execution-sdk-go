// Requirement 1-18: Step with AtMostOncePerRetry semantics (with retry,
// succeeds on second attempt).
//
// From test-requirements/step/1-18.yaml:
//
//	description: Step with AtMostOncePerRetry semantics (with retry,
//	  succeeds on second attempt)
//	handler: |
//	  A step configured with AtMostOncePerRetry semantics and a retry
//	  strategy. The step crashes on the first attempt, then succeeds on
//	  the second retry attempt. The step prints the input string to raw
//	  stdout each time it executes.
//	invocations: |
//	  - SDK checkpoints StepStarted, step begins execution, prints input to
//	    stdout, Lambda crashes.
//	  - Replay 1: Re-invoked because Lambda crashed (Runtime.ExitError), SDK
//	    sees StepStarted with no completion, raises StepInterruptedError,
//	    retry configured, SDK checkpoints StepFailed with retry scheduled,
//	    invocation completes, execution suspends.
//	  - Replay 2: Re-invoked because retry delay elapsed, step re-executes
//	    (new retry attempt), prints input to stdout, step succeeds, SDK
//	    checkpoints StepSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: succeeded on second attempt
//	ExpectedExecutionHistory:
//	  ... StepFailed RetryDetails.CurrentAttempt: 1, NextAttemptDelaySeconds: '*'
//	  ... StepSucceeded RetryDetails.CurrentAttempt: 2
//	ExpectedLogs:
//	  - pattern: ${INPUT_1}
//	    count: 2
//
// Same real-crash mechanism as 1-17 (raw os.Stdout print, then a genuine
// os.Exit(1) killing the process - see that file's doc for the full
// Runtime.ExitError mechanics), but this step's own attempt-1 body
// crashes unconditionally while attempt 2 (sc.Attempt() == 2, the SAME
// checkpointed, replay-safe attempt counter examples/retry-go/handler.go
// and this package's other retry requirements use) prints again and
// returns successfully instead of crashing - exercising
// AtMostOncePerRetry's OTHER branch in runStep: an interruption error
// that IS retried (a retry strategy is configured here, unlike 1-17),
// producing a real StepFailed-with-RetryDetails checkpoint after the
// crash, then a genuine second, fresh invocation (the backend
// re-invokes once NextAttemptDelaySeconds elapses) that re-enters fn for
// attempt 2 and succeeds - hence "prints exactly twice" in ExpectedLogs,
// once per real process execution of this step body.
package handlers

import (
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("1-18", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_18Handler, config(client))
	})
}

func step1_18Handler(event string, dc types.DurableContext) (string, error) {
	strategy := utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 3)
	return operations.Step(dc, "at_most_once_flaky_step", func(sc types.StepContext) (string, error) {
		fmt.Println(event)
		if sc.Attempt() < 2 {
			os.Exit(1)
			return "", errors.New("unreachable") // unreachable
		}
		return "succeeded on second attempt", nil
	},
		operations.WithStepSemantics[string](types.StepSemanticsAtMostOncePerRetry),
		operations.WithStepRetryStrategy[string](strategy),
	)
}

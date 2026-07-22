// Requirement 1-12: Retry exhaustion (max attempts).
//
// From test-requirements/step/1-12.yaml:
//
//	description: Retry exhaustion (max attempts)
//	handler: |
//	  A step that always fails, with a retry strategy that allows 4 total
//	  attempts (1 initial + 3 retries).
//	invocations: |
//	  - 4 total attempts, each failing, delay=1s between each of the first
//	    3, no NextAttemptDelaySeconds on the 4th (exhausted), execution
//	    fails.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// utils.Presets.FixedDelay(1s, 4) retries up to 4 total attempts (its own
// `attempt >= maxAttempts` check - see utils/retry.go - means attempts
// 1..3 retry with a 1s delay and attempt 4 does not, matching the YAML's
// CurrentAttempt sequence 1,2,3,4 with NextAttemptDelaySeconds present on
// 1-3 and absent on 4) with a constant 1s delay between each, exactly as
// the YAML's RetryDetails sequence specifies. The step body always fails
// unconditionally - no sc.Attempt() branching needed since success never
// happens in this scenario.
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
	Register("1-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_12Handler, config(client))
	})
}

func step1_12Handler(event any, dc types.DurableContext) (string, error) {
	strategy := utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 4)
	return operations.Step(dc, "always_fail", func(sc types.StepContext) (string, error) {
		return "", errors.New("intentional permanent failure for conformance requirement 1-12")
	}, operations.WithStepRetryStrategy[string](strategy))
}

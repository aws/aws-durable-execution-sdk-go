// Requirement 6-7: Wait-for-condition check function throws (uncaught
// failure).
//
// From test-requirements/wait_for_condition/6-7.yaml:
//
//	description: Wait-for-condition where the check function raises an
//	  exception on the first attempt — the operation fails and the error
//	  propagates, failing the execution
//	handler: |
//	  Handler runs a single wait_for_condition operation whose check
//	  function raises an exception on its first invocation.
//	  wait_for_condition has no internal retry for check-function errors,
//	  so the SDK records a terminal StepFailed (SubType WaitForCondition)
//	  and re-raises. The handler does not catch the error, so the
//	  execution fails.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    the first check raises an exception, SDK checkpoints the
//	    terminal StepFailed (SubType WaitForCondition) carrying the
//	    error, the error propagates uncaught, and the execution fails.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// No explicit retry strategy is configured, so this uses the operation's
// zero-value default, utils.Presets.NoRetry() (see wait_for_condition.go's
// cfg initialization) - ShouldRetry is false on the very first attempt,
// regardless of whether the triggering error is a genuine checkFn error
// or the errConditionNotMet sentinel. checkFn returns a non-nil error on
// its first (and, given NoRetry, only) call, driving pollAndCheckpoint's
// !decision.ShouldRetry branch directly: a single terminal StepFailed
// checkpoint with no RETRY/continue step at all, matching this
// requirement's single-StepFailed ExpectedExecutionHistory. The handler
// does not observe the returned error - it propagates straight out as
// the whole execution's error, matching ExpectedResult.ExecutionStatus:
// FAILED.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("6-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_7Handler, config(client))
	})
}

func waitForCondition6_7Handler(event any, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		return operations.ConditionResult[int]{}, errors.New("intentional check failure for conformance requirement 6-7")
	}, 0)
}

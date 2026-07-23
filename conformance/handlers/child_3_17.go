// Requirement 3-17: Child context with print only (verify no
// re-execution on replay).
//
// From test-requirements/child/3-17.yaml:
//
//	description: Child context with print only (verify no re-execution on
//	  replay)
//	handler: |
//	  A child context that only prints to raw stdout (no durable operation
//	  inside), followed by a wait that causes a replay. Verifies the print
//	  is only executed once and not re-executed during replay.
//	  The child context returns the input string.
//	invocations: |
//	  - Handler invokes a child context that only prints to raw stdout and
//	    returns the input (no durable operation inside), followed by a
//	    wait. SDK checkpoints ContextStarted, child prints input to stdout
//	    and returns, SDK checkpoints ContextSucceeded, WaitStarted,
//	    invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because wait completed. SDK skips the
//	    completed child context and returns its cached result without
//	    re-executing the print. WaitSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//	ExpectedLogs:
//	  # Child executes once (first invocation), not re-executed on replay
//	  - pattern: ${INPUT_1}
//	    count: 1
//
// The child context's fn has no nested durable operation at all (no
// Step, no Wait) - it only prints to raw stdout and returns the input
// directly, so its own CONTEXT/START-to-SUCCEED checkpoint pair is the
// ONLY replay-skip barrier protecting the print from re-executing. The
// Wait AFTER the child context (a top-level sibling, structurally like
// 3-9's) is what actually causes the suspend/resume: on the replay
// invocation, RunInChildContext's Succeeded branch (invoke.go) returns
// the cached result WITHOUT calling fn again, so the print genuinely
// runs exactly once across both invocations - this is the SDK's real
// replay-skip guarantee under test, not anything this handler enforces
// itself (unlike 3-11's YAML-documented-but-unimplemented ReplayChildren
// mode, this scenario's result is small, so no
// ResultTooLargeError/oversized-result concern applies, and the ordinary
// child-context replay-skip path - already exercised correctly by 3-1
// and 3-9 - is what's actually under test here).
package handlers

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-17", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_17Handler, config(client))
	})
}

func child3_17Handler(event string, dc types.DurableContext) (string, error) {
	result, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		// Raw stdout, deliberately NOT sc.Logger() (there is no
		// types.StepContext here to log through anyway - this child
		// body has no nested step) - ExpectedLogs validates raw
		// standard output, matching step_1_17/1_18's identical
		// convention, and this print must execute EXACTLY once across
		// both invocations, which only holds if RunInChildContext's own
		// Succeeded replay-skip branch (not any bookkeeping in this
		// handler) is what prevents fn from being re-entered on replay.
		fmt.Println(event)
		return event, nil
	})
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait_after_child", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return result, nil
}

// Requirement 7-10: Wait-for-callback mixed with wait and step.
//
// From test-requirements/wait_for_callback/7-10.yaml:
//
//	description: Wait-for-callback combined with a preceding durable
//	  wait and a top-level step, exercising interleaving of operation
//	  types in one handler
//	handler: |
//	  Handler first runs a 1-second durable wait, then a top-level step
//	  that returns fixed data, then a single wait_for_callback operation
//	  using the input as the operation name. The submitter completes and
//	  the operation suspends; the external system completes the callback
//	  with a success payload, which the handler returns.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints WaitStarted (Duration 1), the
//	    invocation completes and execution suspends for the timer.
//	  - Replay 1: The wait completes (WaitSucceeded). The handler runs a
//	    top-level step (StepStarted/StepSucceeded, no ParentId). It then
//	    starts the wait_for_callback operation: ContextStarted (SubType
//	    WaitForCallback), CallbackStarted (ParentId pointing to the
//	    operation Id), submitter StepStarted/StepSucceeded (ParentId
//	    pointing to the operation Id); the invocation completes and
//	    execution suspends.
//	  - Replay 2: External system completed the callback. SDK checkpoints
//	    CallbackSucceeded and ContextSucceeded for the operation, and
//	    execution succeeds.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD}
//	    Delay: 1
//
// Plain sequential composition at the TOP level (no child context): a
// 1-second operations.Wait, then a top-level operations.Step returning
// fixed data (its result is unused by the rest of the handler, matching
// the YAML's own "runs a top-level step that returns fixed data" with no
// further mention of that data being consumed), then
// operations.WaitForCallback whose result the handler returns directly.
// Because the top-level Step is NOT nested inside any child context, its
// own checkpointed ParentID is empty, matching this requirement's own
// ExpectedExecutionHistory StepStarted/StepSucceeded entries which have
// no ParentId key at all (unlike the submitter step nested inside the
// wait_for_callback operation, which does).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_10Handler, config(client))
	})
}

func waitForCallback7_10Handler(event string, dc types.DurableContext) (string, error) {
	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	if _, err := operations.Step(dc, "step", func(sc types.StepContext) (string, error) {
		return "fixed-data", nil
	}); err != nil {
		return "", err
	}

	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
}

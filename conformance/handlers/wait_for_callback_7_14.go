// Requirement 7-14: Wait-for-callback timeout caught (recovers).
//
// From test-requirements/wait_for_callback/7-14.yaml:
//
//	description: Wait-for-callback that times out on its base timeout;
//	  the handler catches the timeout error and returns a fixed
//	  recovery message so the execution succeeds
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name, configured with a 3-second timeout, wrapped
//	  in error handling. The submitter completes and the operation
//	  suspends. No external system completes the callback, so it times
//	  out. The handler catches the raised timeout error and returns the
//	  fixed string "timed-out-handled", so the execution succeeds.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted
//	    with Timeout=3, ParentId pointing to the operation Id), runs the
//	    submitter step (StepStarted/StepSucceeded, ParentId pointing to
//	    the operation Id), the invocation completes and execution
//	    suspends.
//	  - Replay 1: The timeout elapses with no external completion. SDK
//	    checkpoints CallbackTimedOut and ContextFailed, raises the
//	    timeout error which the handler catches; the handler returns
//	    "timed-out-handled" and execution succeeds.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: timed-out-handled
//	# No CallbackActions - deliberately: no external system ever
//	# responds, matching 7-5's own base-timeout scenario.
//
// Same operations.WithWaitForCallbackTimeout(3s) setup as 7-5, but the
// resulting *operations.CallbackFailedError (Timeout=true) is observed
// ("caught") and intentionally not propagated - the handler returns the
// fixed fallback string "timed-out-handled" instead, matching the
// try/catch-recovers idiom already established by 7-6's own analogous
// external-failure-caught handler and wait_for_condition_6_8.go's
// original check-failure-caught handler.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_14Handler, config(client))
	})
}

func waitForCallback7_14Handler(event string, dc types.DurableContext) (string, error) {
	_, cbErr := operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	}, operations.WithWaitForCallbackTimeout[string](types.Duration{Seconds: 3}))
	// "Caught": cbErr is observed here and intentionally not propagated
	// - this is the try/catch the YAML describes.
	_ = cbErr

	return "timed-out-handled", nil
}

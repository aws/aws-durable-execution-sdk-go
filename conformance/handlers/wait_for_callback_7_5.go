// Requirement 7-5: Wait-for-callback timeout (no external completion).
//
// From test-requirements/wait_for_callback/7-5.yaml:
//
//	description: Wait-for-callback that times out because the external
//	  system never completes the callback within the configured timeout
//	handler: |
//	  Handler runs a single wait_for_callback operation configured with
//	  a short timeout of 3 seconds, using the input as the operation
//	  name. The submitter completes and the operation suspends. No
//	  external system ever completes the callback, so after the timeout
//	  elapses the callback times out, the operation fails, and the
//	  handler does not catch it so the execution fails.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId pointing to the operation
//	    Id), the invocation completes and execution suspends.
//	  - Replay 1: The callback timeout elapses with no external
//	    completion. SDK checkpoints CallbackTimedOut, raises the timeout
//	    error to the handler which does not catch it, checkpoints
//	    ContextFailed, and execution fails.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	# No CallbackActions - deliberately: no external system ever
//	# responds, so the backend's own timeout enforcement is what
//	# resolves the callback.
//	ExpectedExecutionHistory: ... CallbackStartedDetails.Timeout: 3 ...
//	  CallbackTimedOutDetails.Error.Payload.ErrorType: Callback.Timeout
//	  ... ExecutionFailed.
//
// Uses operations.WithWaitForCallbackTimeout(3s) - see that option's own
// doc for the real "not yet implemented" gap this session found and
// fixed (WaitForCallback previously accepted this option but never
// enforced it; it is now wired through to CreateCallback's own
// WithCallbackTimeout, which checkpoints CallbackOptions.TimeoutSeconds
// on the inner CALLBACK/CALLBACK START - the exact field this
// requirement's own ExpectedExecutionHistory.CallbackStartedDetails.
// Timeout: 3 asserts). The handler does not catch the resulting
// *operations.CallbackFailedError (Timeout=true), so it propagates
// uncaught and the execution genuinely fails.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_5Handler, config(client))
	})
}

func waitForCallback7_5Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	}, operations.WithWaitForCallbackTimeout[string](types.Duration{Seconds: 3}))
}

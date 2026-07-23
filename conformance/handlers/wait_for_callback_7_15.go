// Requirement 7-15: Wait-for-callback success with empty (null)
// payload.
//
// From test-requirements/wait_for_callback/7-15.yaml:
//
//	description: Wait-for-callback where the external system completes
//	  the callback with success but no payload, exercising the
//	  empty/null result path
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name. The submitter completes and the operation
//	  suspends. The external system completes the callback with a
//	  success but provides no result payload, so the operation result is
//	  null/empty; the handler returns it.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId pointing to the operation
//	    Id), the invocation completes and execution suspends.
//	  - Replay 1: External system completed the callback with an empty
//	    success (no payload). SDK checkpoints CallbackSucceeded with no
//	    result payload, then ContextSucceeded, and execution succeeds
//	    with a null/empty result.
//	AsyncInvoke: true
//	# The exact serialized encoding of a null/empty result diverges
//	# across SDKs, so the terminal payloads are wildcarded; ExpectedResult
//	# asserts the SUCCEEDED status.
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Delay: 1
//
// # Real SDK gap found and fixed this session
//
// Before this session, operations.deserializeCallbackResult
// unconditionally returned a generic "succeeded but no result recorded"
// error whenever a succeeded CALLBACK operation's CallbackDetails.Result
// was nil - conflating "malformed/corrupt checkpoint" with "the external
// system legitimately sent a success with no payload," a real and
// reachable distinction (see that function's own updated doc in
// callback.go for the full comparison against Step's own, non-analogous
// nil-Result guard, which can never actually fire on a real success).
// Fixed by having deserializeCallbackResult return T's zero value (nil
// for the *string this handler uses, matching step_1_5.go's own
// "Undefined/null result" precedent) rather than an error when Result is
// absent. Verified via a real deployment: this handler's own
// WaitForCallback call - typed *string, matching the pointer-for-null
// idiom step_1_5.go already established - now resolves SUCCEEDED with a
// nil *string result once the external system's payload-less success
// action fires, instead of the operation spuriously failing with
// "succeeded but no result recorded."
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_15Handler, config(client))
	})
}

func waitForCallback7_15Handler(event string, dc types.DurableContext) (*string, error) {
	return operations.WaitForCallback[*string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
}

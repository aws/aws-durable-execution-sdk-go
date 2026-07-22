// Requirement 7-3: Wait-for-callback with anonymous submitter (no
// name).
//
// From test-requirements/wait_for_callback/7-3.yaml:
//
//	description: Wait-for-callback invoked with only a submitter
//	  function and no operation name — the operation gets an
//	  auto-generated id and no Name
//	handler: |
//	  Handler runs a single wait_for_callback operation providing only
//	  an inline (anonymous) submitter function and no name. The
//	  submitter completes, the operation suspends, and the external
//	  system completes the callback with a success payload which
//	  becomes the operation result.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback) with no Name, creates the inner callback
//	    (CallbackStarted, ParentId pointing to the operation Id), runs
//	    the submitter step (StepStarted/StepSucceeded, ParentId pointing
//	    to the operation Id), the invocation completes and execution
//	    suspends.
//	  - Replay 1: External system completed the callback. SDK
//	    checkpoints CallbackSucceeded with the payload, then
//	    ContextSucceeded, and execution succeeds.
//	AsyncInvoke: true
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD}
//	    Delay: 1
//
// operations.WaitForCallback's own id parameter both mints the
// operation's hierarchical step ID (via NextStepID, unconditionally
// unique per call site regardless of what string is passed) AND becomes
// its checkpointed Name (types.OperationUpdate.Name, `json:"Name,
// omitempty"` on the wire per wire.go) - passing the empty string here
// still produces a distinct, valid operation ID (NextStepID's hashing
// scheme does not require a non-empty name to disambiguate call sites)
// but an empty Name is omitted entirely from the checkpoint request,
// matching this requirement's own "no Name" expectation. This is the Go
// SDK's equivalent of "providing only an inline submitter function and
// no name" - there is no separate no-name overload, just an empty id
// string.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_3Handler, config(client))
	})
}

func waitForCallback7_3Handler(event any, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, "", func(sc types.StepContext, callbackID string) error {
		return nil
	})
}

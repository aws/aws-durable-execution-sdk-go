// Requirement 7-11: Wait-for-callback with structured (JSON) result
// deserialization.
//
// From test-requirements/wait_for_callback/7-11.yaml:
//
//	description: Wait-for-callback whose result is deserialized from a
//	  JSON object rather than the default raw string, exercising the
//	  deserialization path
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name and configures it to deserialize the
//	  callback payload as a JSON object (instead of the default raw
//	  string). The submitter completes and the operation suspends. The
//	  external system completes the callback with the JSON object
//	  {"status": "approved"}. The handler deserializes it and returns
//	  the value of the "status" field ("approved").
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId pointing to the operation
//	    Id), the invocation completes and execution suspends.
//	  - Replay 1: External system completed the callback with a JSON
//	    object payload. SDK checkpoints CallbackSucceeded carrying the
//	    JSON payload, then ContextSucceeded; the handler deserializes the
//	    payload and returns the "status" field, and execution succeeds
//	    returning "approved".
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: approved
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload:
//	      status: approved
//	    Delay: 1
//
// operations.WaitForCallback is instantiated with a concrete
// approvalResult struct (rather than string, as every other requirement
// in this suite uses) so the default JSON serdes deserializes the
// callback's {"status": "approved"} payload directly into
// approvalResult.Status via deserializeCallbackResult's own
// JSON-remarshal fallback path (callback.go) - no custom
// WithWaitForCallbackSerdes needed; the default serdes already handles
// arbitrary JSON-deserializable result types, string is simply the type
// every other requirement in this suite happens to use. The handler then
// returns the Status field alone, matching ExpectedResult.Result:
// approved.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_11Handler, config(client))
	})
}

type waitForCallback7_11ApprovalResult struct {
	Status string `json:"status"`
}

func waitForCallback7_11Handler(event string, dc types.DurableContext) (string, error) {
	result, err := operations.WaitForCallback[waitForCallback7_11ApprovalResult](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
	if err != nil {
		return "", err
	}
	return result.Status, nil
}

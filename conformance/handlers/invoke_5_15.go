// Requirement 5-15: Invoke with custom payload serdes.
//
// From test-requirements/invoke/5-15.yaml:
//
//	description: Invoke with custom payload serdes — custom serializer for the outgoing
//	  payload
//	handler: |
//	  A handler that invokes a target function with a custom payload serializer configured.
//	  The custom serdes transforms the payload before sending (e.g., uppercases a string).
//	  The target function receives the transformed payload and succeeds.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName, payload, { payloadSerdes:
//	    customSerdes })`, SDK serializes payload with custom serdes, checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK checkpoints
//	    ChainedInvokeSucceeded, execution succeeds.
//	Input:
//	  data: hello
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// operations.WithInvokeSerdes sets ONE types.Serdes used for BOTH the
// outgoing input and the incoming result (see invokeConfig.serdes's own
// single field in invoke.go) - this requirement only needs the OUTGOING
// direction customized, so invoke515UppercaseInputSerdes below overrides
// Serialize to uppercase the payload's own JSON string representation
// before sending, while its Deserialize is left as a byte-for-byte
// passthrough of the default JSON behavior (utils.JSONSerdes.Deserialize)
// for the echoed result coming back - i.e. only ONE of the two
// types.Serdes methods actually diverges from the default, which is all
// this specific requirement's own "transforms the payload before
// sending" scenario needs (contrast invoke_5_16.go, which customizes the
// OTHER direction only).
package handlers

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

type invoke515Event struct {
	Data string `json:"data"`
}

// invoke515UppercaseInputSerdes uppercases the outgoing payload's raw
// string value before JSON-encoding it, leaving deserialization of the
// (echoed-back, already-uppercased) result to the default JSON behavior
// unchanged - see this file's own top-level doc for why only Serialize
// needs to diverge here.
type invoke515UppercaseInputSerdes struct {
	utils.JSONSerdes
}

func (invoke515UppercaseInputSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	if s, ok := value.(string); ok {
		value = strings.ToUpper(s)
	}
	return utils.JSONSerdes{}.Serialize(value, entityID, executionARN)
}

func init() {
	Register("5-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_15Handler, config(client))
	})
}

func invoke5_15Handler(event invoke515Event, dc types.DurableContext) (string, error) {
	return operations.Invoke[string, string](
		dc, "invoke", echoTargetFunctionARN(), event.Data,
		operations.WithInvokeSerdes[string, string](invoke515UppercaseInputSerdes{}),
	)
}

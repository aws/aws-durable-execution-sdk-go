// Requirement 5-16: Invoke with custom result serdes.
//
// From test-requirements/invoke/5-16.yaml:
//
//	description: Invoke with custom result serdes — custom deserializer for the returned
//	  result
//	handler: |
//	  A handler that invokes a target function with a custom result deserializer configured.
//	  The custom serdes transforms the result after receiving (e.g., uppercases the result
//	  string).
//	  The handler returns the transformed result.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName, payload, { resultSerdes:
//	    customSerdes })`, SDK checkpoints ChainedInvokeStarted, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK checkpoints
//	    ChainedInvokeSucceeded, deserializes result with custom serdes, execution succeeds.
//	Input: hello
//	ExpectedResult:
//	  Result: '"HELLO"'
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... ChainedInvokeStartedDetails.Input.Payload: '"hello"'
//	  ... ChainedInvokeSucceededDetails.Result.Payload: '"hello"'
//
// Confirms echo-target's own real, deployed behavior stays a PLAIN,
// unmodified echo for this requirement - ChainedInvokeStartedDetails.
// Input.Payload and ChainedInvokeSucceededDetails.Result.Payload are BOTH
// asserted as the literal, lowercase '"hello"' JSON string: the target
// receives "hello" and returns "hello" verbatim, completely unaware of
// any uppercasing. The uppercasing happens entirely on the CALLER's own
// side, via invoke516UppercaseResultSerdes's Deserialize override below -
// the mirror image of invoke_5_15.go's Serialize-only override (that one
// customizes the OUTGOING direction; this one customizes the INCOMING
// direction, leaving Serialize as the unmodified default so the
// checkpointed Input.Payload stays exactly '"hello"').
package handlers

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// invoke516UppercaseResultSerdes leaves Serialize as the unmodified
// default JSON behavior (so the checkpointed outgoing Input.Payload
// exactly matches this requirement's own '"hello"' expectation) and
// overrides ONLY Deserialize, uppercasing the echoed-back RAW payload
// string as-is - NOT json-decoding it first. The requirement's own
// ExpectedResult.Result is '"HELLO"' (a 7-character string WITH the
// quote characters, after the validator's single json.loads of the
// top-level result): the custom deserializer receives the raw
// checkpointed wire bytes '"hello"' (JSON quotes included) and
// uppercases them verbatim to '"HELLO"' (quotes still included, now as
// literal string content), which the handler's own default top-level
// serdes then JSON-encodes as '"\"HELLO\""' - json.loads of that is
// exactly the expected quoted string. An earlier version of this serdes
// json-unmarshalled the payload BEFORE uppercasing (producing the bare
// 'HELLO', no quotes), which failed this requirement - a real handler
// bug, confirmed against the requirement's own quoted expectation, not
// a suite bug.
type invoke516UppercaseResultSerdes struct {
	utils.JSONSerdes
}

func (invoke516UppercaseResultSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	return strings.ToUpper(pointer), nil
}

func init() {
	Register("5-16", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_16Handler, config(client))
	})
}

func invoke5_16Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Invoke[string, string](
		dc, "invoke", echoTargetFunctionARN(), event,
		operations.WithInvokeSerdes[string, string](invoke516UppercaseResultSerdes{}),
	)
}

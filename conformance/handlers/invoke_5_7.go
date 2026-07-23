// Requirement 5-7: Invoke large payload (payload near size limit).
//
// From test-requirements/invoke/5-7.yaml:
//
//	description: Invoke large payload — payload near the size limit
//	handler: |
//	  A handler that invokes a target function with a large payload (near the Lambda payload
//	  size limit).
//	  The target function receives the large payload, processes it, and returns a result.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// "Near the size limit" here means this SDK's OWN checkpoint-payload
// limit (checkResultSize/resultTooLargeThresholdBytes in
// pkg/durable/operations/errors.go, 750 * 1024 bytes) - not Lambda's own
// much larger 6 MB synchronous-invoke payload limit - since that is the
// limit operations.Invoke itself actually enforces client-side before
// ever checkpointing anything (see invoke.go's own checkResultSize call
// on the serialized input). This handler generates a fixed, deterministic
// (not random - replay-safety requires the SAME bytes be reconstructed
// identically on every replay, see this SDK's own established
// convention e.g. child_3_11's ReplayChildren doc) 512 KiB string,
// comfortably close to but safely under that 750 KiB threshold, and
// sends it as the invoke input; echo-target returns it unchanged.
package handlers

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// invoke57LargePayloadBytes is comfortably under
// resultTooLargeThresholdBytes (750 KiB, pkg/durable/operations/errors.go)
// while still being "large" in the sense this requirement's own
// description means - close enough to the limit to genuinely exercise
// checkpointing/transmitting a large payload, but with enough margin that
// JSON-string-escaping overhead (this payload is a single large string,
// checkpointed as a JSON string literal - see checkResultSize's own doc
// on why it measures the SERIALIZED, not raw, byte length) can never push
// the actual serialized size over the real threshold.
const invoke57LargePayloadBytes = 512 * 1024

func init() {
	Register("5-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_7Handler, config(client))
	})
}

func invoke5_7Handler(_ any, dc types.DurableContext) (string, error) {
	// A fixed repeating character, not e.g. crypto/rand output - must be
	// byte-for-byte identical on every replay (see this file's own doc
	// comment on replay-safety) and echo-target's response is asserted
	// only via a wildcard '*' Payload match anyway (test-requirements/
	// invoke/5-7.yaml's own ExpectedExecutionHistory), so content beyond
	// "large and deterministic" is irrelevant to what this requirement
	// actually verifies.
	largePayload := strings.Repeat("x", invoke57LargePayloadBytes)
	return operations.Invoke[string, string](dc, "invoke", echoTargetFunctionARN(), largePayload)
}

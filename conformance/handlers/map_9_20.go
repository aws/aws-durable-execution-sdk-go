// Requirement 9-20: Map with an operation-level serdes - deserialize on
// replay (wait after the map).
//
// From test-requirements/map/9-20.yaml:
//
//	description: A durable wait after a map with a custom operation-level
//	  serializer forces a replay; the checkpointed map result is
//	  deserialized through that serializer and the execution resumes
//	  with the correct result
//	handler: |
//	  Handler invokes the map operation over two string items ["x", "y"]
//	  with the same custom OPERATION-LEVEL serializer as 9-19 (emits
//	  "OPSERDE:X,Y" on serialize, reverses it on deserialize). The map
//	  result is ["X", "Y"] and its ContextSucceeded payload is
//	  "OPSERDE:X,Y". After the map, the handler issues a durable wait (1
//	  second), which suspends the whole execution. On replay (after the
//	  wait elapses) the SDK reconstructs the completed map result by
//	  DESERIALIZING the checkpointed payload through the custom
//	  serializer, resumes, and the handler returns the ordered results.
//	  Max-concurrency is 1 for a deterministic history.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [X, Y]
//
// The deserialize-on-replay half of 9-19 directly above (see that
// requirement's own doc for the full mechanism/JS-SDK-evidence
// writeup): uses the exact same operations.WithMapResultSerdes[TIn,
// TOut](opLevelSerdes{}) option and shared opLevelSerdes type (see
// map_9_19.go), plus a durable Wait afterward to force the real backend
// to replay past the already-SUCCEEDED Map context on a later
// invocation - proving deserializeBatchResult's own new custom-serdes
// Deserialize path genuinely runs (and short-circuits re-running the
// item loop), not just that Serialize's own output looks right in
// isolation.
package handlers

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("9-20", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_20Handler, config(client))
	})
}

func map9_20Handler(event any, dc types.DurableContext) ([]string, error) {
	batch, err := operations.Map(dc, "op-serde-replay", []string{"x", "y"},
		func(child types.DurableContext, item string, index int) (string, error) {
			return strings.ToUpper(item), nil
		},
		operations.WithMapMaxConcurrency[string, string](1),
		operations.WithMapResultSerdes[string, string](opLevelSerdes{}),
	)
	if err != nil {
		return nil, err
	}

	if err := operations.Wait(dc, "pause", types.Duration{Seconds: 1}); err != nil {
		return nil, err
	}

	return batch.GetResults(), nil
}

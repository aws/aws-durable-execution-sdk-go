// Requirement 9-19: Map with an operation-level (whole-result) serdes -
// serialize on a fresh operation.
//
// From test-requirements/map/9-19.yaml:
//
//	description: Map configured with a custom operation-level serializer
//	  serializes the entire map result through that serializer; the Map
//	  context result payload is the serializer's output
//	handler: |
//	  Handler invokes the map operation over two string items ["x", "y"]
//	  with a custom OPERATION-LEVEL serializer (serdes / serDes) wired
//	  into the map config - this serializes the whole map result (the
//	  batch/MapResult), not individual items. Each iteration returns the
//	  uppercased item using the default per-item serializer, so the map
//	  result is ["X", "Y"]. The operation-level serializer is a real,
//	  non-identity implementation that emits the deterministic string
//	  "OPSERDE:" followed by the comma-joined results
//	  ("OPSERDE:X,Y"), and deserializes by reversing that encoding.
//	  Max-concurrency is 1 for a deterministic history. The Map
//	  ContextSucceeded result payload is asserted directly to prove the
//	  operation-level serializer ran on the fresh operation; per-iteration
//	  payloads use the default serializer and are not asserted here. The
//	  handler returns the ordered results ["X", "Y"].
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [X, Y]
//	ExpectedExecutionHistory: ... ContextSucceeded (SubType Map) whose
//	  result payload is exactly "OPSERDE:X,Y"
//
// PREVIOUSLY declared NotImplemented on the conclusion that this Go
// SDK's finishBatch unconditionally serializes the outer BatchResult
// with plain encoding/json, with no hook for a caller-supplied
// whole-result serializer. Re-examined per explicit user request and
// found to be a real, buildable feature gap, not a permanent limitation:
// the JS reference SDK has a genuine, public, DISTINCT MapConfig.serdes
// field (packages/aws-durable-execution-sdk-js/src/types/batch.ts:
// "Serialization/deserialization configuration for parent context",
// explicitly separate from itemSerdes's own "for each item" doc) - and
// its own actual mechanism (map-handler.ts forwards config.serdes
// straight into executeConcurrently -> runInChildContext's OWN
// options.serdes) confirms this is NOT a Map/Parallel-specific
// mechanism at all - it's the exact same generic per-context serdes hook
// RunInChildContext/WithChildSerdes ALREADY implements one level down in
// this very Go SDK. Implemented as operations.WithMapResultSerdes[TIn,
// TOut](types.Serdes) (pkg/durable/operations/batch.go), which
// finishBatch/deserializeBatchResult now dispatch to (by concrete type,
// distinguishing the untouched-default utils.JSONSerdes case from a
// genuinely custom one) instead of the old, unconditional
// batchResultWire[TOut]/MarshalJSON path - see WithMapResultSerdes's own
// doc, and finishBatch's own updated doc, for the full mechanism and the
// two accepted Deserialize return shapes.
package handlers

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// opLevelSerdes is 9-19/9-20's own shared custom, non-identity,
// whole-result serdes - exactly matching both requirements' own YAML
// description: "emits the deterministic string 'OPSERDE:' followed by
// the comma-joined results", "deserializes by reversing that encoding".
type opLevelSerdes struct{}

func (opLevelSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	batch, ok := value.(operations.BatchResult[string])
	if !ok {
		return "", fmt.Errorf("opLevelSerdes.Serialize: expected operations.BatchResult[string], got %T", value)
	}
	return "OPSERDE:" + strings.Join(batch.GetResults(), ","), nil
}

func (opLevelSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	trimmed := strings.TrimPrefix(pointer, "OPSERDE:")
	if trimmed == "" {
		return []string{}, nil
	}
	return strings.Split(trimmed, ","), nil
}

func init() {
	Register("9-19", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_19Handler, config(client))
	})
}

func map9_19Handler(event any, dc types.DurableContext) ([]string, error) {
	batch, err := operations.Map(dc, "op-serde", []string{"x", "y"},
		func(child types.DurableContext, item string, index int) (string, error) {
			return strings.ToUpper(item), nil
		},
		operations.WithMapMaxConcurrency[string, string](1),
		operations.WithMapResultSerdes[string, string](opLevelSerdes{}),
	)
	if err != nil {
		return nil, err
	}
	return batch.GetResults(), nil
}

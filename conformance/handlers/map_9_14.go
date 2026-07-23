// Requirement 9-14: Map with a custom per-item serdes.
//
// From test-requirements/map/9-14.yaml:
//
//	description: Map configured with a custom per-item serializer
//	  round-trips each iteration result through that serializer and
//	  returns the correct ordered results
//	handler: |
//	  Handler invokes the map operation over two string items with a
//	  custom per-item serializer (item_serdes / itemSerdes) wired into
//	  the map config. Each iteration returns the uppercased item
//	  directly; the SDK serializes and deserializes each iteration
//	  result through the custom serializer as it is checkpointed and
//	  replayed. Max-concurrency is 1 for a deterministic history. ...
//	  the checkpointed iteration payload is the serializer's output
//	  ("wrapped:X", "wrapped:Y") ...
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [X, Y]
//
// operations.WithMapSerdes applies to EVERY item's own result (batch.go's
// Map passes cfg.serdes straight through to each runBatchItem call,
// per-item, not just the outer BatchResult - see finishBatch's own doc
// for why the OUTER result is always plain JSON regardless of this
// serdes) - exactly matching this requirement's "item_serdes" naming and
// its per-item "wrapped:X"/"wrapped:Y" assertion on each MapIteration's
// own ContextSucceededDetails.Result.Payload. Unlike Parallel8N15's
// {"wrapped": v} JSON envelope, this requirement's own wire format is
// the plain string "wrapped:<VALUE>" (asserted literally in the YAML,
// not via a JSON-shape wildcard), so map914Serdes serializes/
// deserializes that exact prefix form instead of a JSON envelope.
package handlers

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// map914Serdes serializes a string value v as the literal "wrapped:v"
// and deserializes that prefix form back to v - a real, non-identity
// round-trip, matching this requirement's own literal-payload assertion
// ("wrapped:X"/"wrapped:Y") rather than a JSON envelope.
type map914Serdes struct{}

func (map914Serdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("map914Serdes: expected string, got %T", value)
	}
	return "wrapped:" + s, nil
}

func (map914Serdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	s, ok := strings.CutPrefix(pointer, "wrapped:")
	if !ok {
		return nil, fmt.Errorf("map914Serdes: malformed payload %q: missing wrapped: prefix", pointer)
	}
	return s, nil
}

func init() {
	Register("9-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_14Handler, config(client))
	})
}

func map9_14Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []string{"x", "y"}

	batch, err := operations.Map(dc, "serdes", items,
		func(child types.DurableContext, item string, index int) (string, error) {
			return strings.ToUpper(item), nil
		},
		operations.WithMapMaxConcurrency[string, string](1),
		operations.WithMapSerdes[string, string](map914Serdes{}),
	)
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}

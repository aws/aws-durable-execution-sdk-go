// Requirement 8-15: Parallel with a custom per-branch serde
// (itemSerdes) round-trips results.
//
// From test-requirements/parallel/8-15.yaml:
//
//	description: Parallel with a custom itemSerdes that wraps each branch
//	  result on serialize and unwraps on deserialize, verifying the serde
//	  is honored and round-trips
//	handler: |
//	  Handler invokes the parallel operation with two branches and
//	  max-concurrency 1, passing a custom per-branch serde (itemSerdes).
//	  Each branch returns a string directly ("x" for branch 0, "y" for
//	  branch 1). The custom serde is symmetric: it serializes a value v
//	  as the envelope {"wrapped": v} and deserializes that envelope back
//	  to v. Each branch result is therefore checkpointed in the wrapped
//	  form but read back as the original value. The handler returns the
//	  ordered array of successful branch results, ["x", "y"].
//	ExpectedExecutionHistory:
//	  ...ContextSucceededDetails.Result.Payload: ${/wrapped/}  (both
//	  branches)
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [x, y]
//
// operations.WithParallelSerdes applies to EVERY branch's own result
// (batch.go's Parallel passes cfg.serdes straight through to each
// runBatchItem call, per-branch, not just the outer BatchResult) -
// exactly matching this requirement's "itemSerdes" naming and its
// per-branch (not per-batch) ${/wrapped/} assertion on each
// ParallelBranch's own ContextSucceededDetails.Result.Payload.
// parallel815Serdes.Serialize/Deserialize round-trip a bare string value
// through a {"wrapped": ...} JSON envelope.
package handlers

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// parallel815Envelope is the {"wrapped": v} wire shape parallel815Serdes
// serializes every branch result into and deserializes back out of.
type parallel815Envelope struct {
	Wrapped string `json:"wrapped"`
}

type parallel815Serdes struct{}

func (parallel815Serdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("parallel815Serdes: expected string, got %T", value)
	}
	b, err := json.Marshal(parallel815Envelope{Wrapped: s})
	if err != nil {
		return "", fmt.Errorf("parallel815Serdes: serialize: %w", err)
	}
	return string(b), nil
}

func (parallel815Serdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var env parallel815Envelope
	if err := json.Unmarshal([]byte(pointer), &env); err != nil {
		return nil, fmt.Errorf("parallel815Serdes: deserialize: %w", err)
	}
	return env.Wrapped, nil
}

func init() {
	Register("8-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_15Handler, config(client))
	})
}

func parallel8_15Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "x", nil },
		func(child types.DurableContext) (string, error) { return "y", nil },
	}

	batch, err := operations.Parallel(dc, "serde", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelSerdes[string](parallel815Serdes{}),
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

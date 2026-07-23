// Requirement 8-4: Parallel with heterogeneous branch return types.
//
// From test-requirements/parallel/8-4.yaml:
//
//	description: Parallel whose branches return different types (string,
//	  number, object)
//	handler: |
//	  Handler invokes the parallel operation with three branches that
//	  return values of different types directly: branch 0 returns the
//	  string "hello", branch 1 returns the number 42, and branch 2
//	  returns the object {k: "v"}. Max-concurrency is 1 for deterministic
//	  ordering. The handler returns the ordered array of successful
//	  branch results, in branch index order. This verifies each branch's
//	  result is serialized and deserialized with its own type preserved.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    - hello
//	    - 42
//	    - k: v
//
// operations.Parallel[TOut] is generic over a SINGLE TOut shared by every
// branch (unlike the reference SDKs' dynamically-typed branches array),
// so heterogeneous per-branch return types are expressed here as
// TOut=any: each branch returns its own concrete Go value (a string, a
// float64, or a map[string]string) boxed in the any interface, and the
// default JSON serdes round-trips each through encoding/json (numbers
// always deserialize back as float64, matching the YAML's plain "42").
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_4Handler, config(client))
	})
}

func parallel8_4Handler(event any, dc types.DurableContext) ([]any, error) {
	branches := []func(types.DurableContext) (any, error){
		func(child types.DurableContext) (any, error) { return "hello", nil },
		func(child types.DurableContext) (any, error) { return 42, nil },
		func(child types.DurableContext) (any, error) { return map[string]string{"k": "v"}, nil },
	}

	batch, err := operations.Parallel(dc, "hetero", branches, operations.WithParallelMaxConcurrency[any](1))
	if err != nil {
		return nil, err
	}

	results := make([]any, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}

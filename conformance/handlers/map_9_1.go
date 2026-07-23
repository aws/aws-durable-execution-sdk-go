// Requirement 9-1: Map basic (one step per item, all succeed).
//
// From test-requirements/map/9-1.yaml:
//
//	description: Map basic applies a function to each item, each item
//	  runs a single step, all succeed
//	handler: |
//	  Handler invokes the map operation with a fixed name "map" over a
//	  list of items. Each item runs inside its own MapIteration child
//	  context and executes a single step that returns a greeting string
//	  ("Hello, <item>!"). Max-concurrency is set to 1 so items are
//	  processed sequentially in index order, producing a deterministic
//	  execution history. The handler returns the ordered array of
//	  successful item results, in item index order.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ["Hello, World!", "Hello, Kiro!"]
//
// operations.WithMapMaxConcurrency(1) forces items to run strictly
// sequentially in index order (see batch.go's runBatch: the concurrency
// semaphore is sized 1, and step IDs are pre-claimed in index order on
// the parent goroutine before any item goroutine is spawned), giving a
// fully deterministic ExecutionHistory ordering to assert against - each
// item runs a Step (SubType Step) inside its own MapIteration child
// context, mirroring Parallel8N1's identical structure one level down
// (see that file's own doc) with SubType MapIteration/Step in place of
// ParallelBranch/Step.
package handlers

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_1Handler, config(client))
	})
}

func map9_1Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []string{"World", "Kiro"}

	batch, err := operations.Map(dc, "map", items,
		func(child types.DurableContext, item string, index int) (string, error) {
			return operations.Step(child, "greet", func(sc types.StepContext) (string, error) {
				return fmt.Sprintf("Hello, %s!", item), nil
			})
		},
		operations.WithMapMaxConcurrency[string, string](1),
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

// Requirement 9-15: Map suspends inside an iteration and replay skips
// the completed iteration.
//
// From test-requirements/map/9-15.yaml:
//
//	description: A wait inside one map iteration suspends the whole
//	  execution mid-map; on replay the already-succeeded iteration is
//	  skipped and the suspended iteration resumes to completion
//	handler: |
//	  Handler invokes the map operation over two items with
//	  max-concurrency=1 so iterations run sequentially. Iteration 0 runs
//	  a single step returning "r0" and succeeds. Iteration 1 issues a
//	  durable wait (1 second) and then runs a step returning "r1". When
//	  iteration 1 hits the wait the whole execution suspends. On replay
//	  (after the wait elapses) the SDK skips the already-succeeded
//	  iteration 0, resumes iteration 1 after the wait, runs its step,
//	  completes the map, and the execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [r0, r1]
//
// operations.Wait (see runBatchItem's own doc: fn's own nested
// operations are independently replay-safe under the item's step-ID
// prefix, exactly like RunInChildContext's single child context) is the
// mechanism that suspends the whole execution when iteration 1 hits it
// - runBatch's goroutine for iteration 1 receives errSuspended back from
// fn and propagates it unchanged (see runBatch's own comment on its
// errSuspended branch), which surfaces as Map itself returning
// errSuspended to the handler, which durable.WithDurableExecution's own
// runtime plumbing recognizes as "suspend this invocation" rather than a
// real failure - no special handling needed in this handler beyond
// letting Map's own error propagate.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_15Handler, config(client))
	})
}

func map9_15Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []int{0, 1}

	batch, err := operations.Map(dc, "suspend", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			if item == 1 {
				if err := operations.Wait(child, "pause", types.Duration{Seconds: 1}); err != nil {
					return "", err
				}
			}
			return operations.Step(child, "produce", func(sc types.StepContext) (string, error) {
				if item == 0 {
					return "r0", nil
				}
				return "r1", nil
			})
		},
		operations.WithMapMaxConcurrency[int, string](1),
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

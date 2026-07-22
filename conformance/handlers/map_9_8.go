// Requirement 9-8: Map tolerated-failure-count within tolerance (all
// items complete).
//
// From test-requirements/map/9-8.yaml:
//
//	description: Map with a tolerated-failure-count tolerating one
//	  failure completes every item
//	handler: |
//	  Handler invokes the map operation over three items with a
//	  completion config of tolerated-failure-count=1. Max-concurrency is
//	  1 so items run sequentially: item 0 succeeds, item 1 raises an
//	  error and fails, item 2 succeeds. Because the failure count (1)
//	  does not exceed the tolerated failure count (1), the operation
//	  does not stop early and all items complete. The handler returns a
//	  projection {completionReason, status, successCount, failureCount,
//	  totalCount}. Completion reason is ALL_COMPLETED, but status is
//	  FAILED because at least one item failed.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 2
//	    failureCount: 1
//	    totalCount: 3
//
// Unlike 9-5 (tolerance exceeded, Map returns a *BatchFailedError), here
// ToleratedFailureCount=1 with exactly 1 failure means
// batchCompletion.overallSucceeded's thresholdExceeded check (failed >
// 1, i.e. 1 > 1) is false, so finishBatch's SUCCESS branch runs and Map
// returns a populated, non-error BatchResult - the projection is built
// directly from batch.Items, with "status: FAILED" reflecting that at
// least one item failed even though the BATCH itself (as a Go call)
// succeeded. Mirrors Parallel8N9 (parallel_8_9.go) exactly, one level
// down.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("9-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_8Handler, config(client))
	})
}

func map9_8Handler(event any, dc types.DurableContext) (mapProjection, error) {
	tolerated := 1
	items := []int{0, 1, 2}

	batch, err := operations.Map(dc, "tolerated", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			if item == 1 {
				return "", errMapItemFailed
			}
			return "ok", nil
		},
		operations.WithMapMaxConcurrency[int, string](1),
		operations.WithMapCompletionConfig[int, string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return mapProjection{}, err
	}

	return newMapProjection(batch), nil
}

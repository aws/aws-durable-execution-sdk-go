// Requirement 9-10: Map tolerated-failure-percentage exceeded (stops
// early).
//
// From test-requirements/map/9-10.yaml:
//
//	description: Map stops early once the failure percentage exceeds
//	  the tolerated-failure-percentage
//	handler: |
//	  Handler invokes the map operation over four items with a
//	  completion config of tolerated-failure-percentage=25. The
//	  tolerated failure percentage is computed against the total item
//	  count (4). Max-concurrency is 1 so items run sequentially: item 0
//	  fails (1/4 = 25%, not exceeding 25), item 1 also fails (2/4 = 50%,
//	  now exceeding 25), so the operation stops early and items 2 and 3
//	  are never started. The handler returns a projection
//	  {completionReason, successCount, failureCount, totalCount}.
//	  Completion reason is FAILURE_TOLERANCE_EXCEEDED.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    successCount: 0
//	    failureCount: 2
//	    totalCount: 2
//
// Same root cause and shape as 9-9, using ToleratedFailurePercentage
// instead of ToleratedFailureCount: batchCompletion.thresholdExceeded
// computes failed/total*100 against b.total (fixed at len(items)=4 for
// the whole batch, per batch.go's Map: `completion := batchCompletion{
// cfg: cfg.completionConfig, total: len(items)}` - NOT against however
// many items have actually started so far), so after item 1's failure
// the percentage is 2/4=50% > 25, tripping the same early-exit as 9-9.
//
// # Update: now reads the projection directly off BatchResult's own real
// # fields/methods (see 9-9's own doc for the full completion-policy-
// # contract fix)
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("9-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_10Handler, config(client))
	})
}

func map9_10Handler(event any, dc types.DurableContext) (mapToleranceProjection, error) {
	pct := 25.0
	items := []int{0, 1, 2, 3}

	batch, err := operations.Map(dc, "tolerated-pct", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			if item == 0 || item == 1 {
				return "", errMapItemFailed
			}
			return "unreached", nil
		},
		operations.WithMapMaxConcurrency[int, string](1),
		operations.WithMapCompletionConfig[int, string](types.CompletionConfig{ToleratedFailurePercentage: &pct}),
	)
	if err != nil {
		return mapToleranceProjection{}, err
	}

	return newMapToleranceProjection(batch), nil
}

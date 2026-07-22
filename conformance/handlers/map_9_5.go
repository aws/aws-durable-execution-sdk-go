// Requirement 9-5: Map fail-fast via tolerated-failure-count=0 stops
// after first failure.
//
// From test-requirements/map/9-5.yaml:
//
//	description: Map configured with a fail-fast completion config
//	  (tolerated-failure-count=0) stops on the first item failure and
//	  does not start remaining items
//	handler: |
//	  Handler invokes the map operation over three items with a
//	  fail-fast completion config (tolerated-failure-count=0).
//	  Max-concurrency is 1 so items run sequentially: item 0 returns
//	  "ok" and succeeds, item 1 raises an error and fails, at which point
//	  fail-fast stops the operation and item 2 is never started. The map
//	  operation itself does not raise; it returns a batch result. The
//	  handler returns a projection {completionReason, status,
//	  successCount, failureCount, totalCount}. Because a failure
//	  occurred, status is FAILED and completion reason is
//	  FAILURE_TOLERANCE_EXCEEDED.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    status: FAILED
//	    successCount: 1
//	    failureCount: 1
//	    totalCount: 2
//
// # Update: the completion-policy-contract mismatch this handler used to
// # document is now FIXED at the SDK level
//
// operations.Map now ALWAYS returns a valid BatchResult with a nil error
// once its items finish, exactly matching this requirement's own YAML
// prose ("The map operation itself does not raise; it returns a batch
// result") - see batch.go's own completion-policy-contract fix doc. This
// handler now reads the projection's fields directly off BatchResult's
// own real Status()/CompletionReason/SucceededCount()/FailureCount()/
// TotalCount() methods.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// mapProjection is the shared {completionReason, status, successCount,
// failureCount, totalCount}-shaped result several requirements in this
// suite (9-5, 9-9, 9-10, 9-18) return, derived directly from
// operations.BatchResult's own real fields/methods - mirrors
// parallelProjection (parallel_8_6.go) exactly, one level down (Map
// instead of Parallel).
type mapProjection struct {
	CompletionReason string `json:"completionReason"`
	Status           string `json:"status"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
}

func newMapProjection[T any](batch operations.BatchResult[T]) mapProjection {
	return mapProjection{
		CompletionReason: batch.CompletionReason,
		Status:           batch.Status(),
		SuccessCount:     batch.SucceededCount(),
		FailureCount:     batch.FailureCount(),
		TotalCount:       batch.TotalCount(),
	}
}

// errMapItemFailed is a plain sentinel used across several of this
// suite's handlers whenever an item's own scenario just needs to fail
// without any specific error content asserted - mirrors
// errParallelBranchFailed (parallel_8_6.go) exactly.
var errMapItemFailed = errors.New("item failed")

func init() {
	Register("9-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_5Handler, config(client))
	})
}

func map9_5Handler(event any, dc types.DurableContext) (mapProjection, error) {
	tolerated := 0
	items := []int{0, 1, 2}

	batch, err := operations.Map(dc, "failfast", items,
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
		// A genuine, unrelated error - Map no longer returns an error
		// for a policy-not-met batch (see operations.BatchResult's own
		// doc), so this is unexpected for this scenario.
		return mapProjection{}, err
	}

	return newMapProjection(batch), nil
}

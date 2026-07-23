// Requirement 9-18: Suspension after a map that completed with a
// failure (replay skips the completed map).
//
// From test-requirements/map/9-18.yaml:
//
//	description: A durable wait placed after a map that ran every item
//	  but recorded a failure suspends the execution; on replay the
//	  completed map (including the failed iteration) is skipped and the
//	  execution resumes to success
//	handler: |
//	  Handler invokes the map operation over two items with a
//	  tolerated-failure-count of 1, so a single failure is tolerated and
//	  every item runs: item 0 succeeds, item 1 raises and is recorded as
//	  failed. The map returns a batch result whose status is FAILED (a
//	  failure occurred) with completion reason ALL_COMPLETED; the
//	  handler does NOT rethrow. After the map, the handler issues a
//	  durable wait (1 second), which suspends the whole execution. On
//	  replay (after the wait elapses) the SDK skips the already-
//	  completed map (including the failed iteration), checkpoints the
//	  wait success, and the execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 1
//	    failureCount: 1
//	    totalCount: 2
//
// ToleratedFailureCount=1 with exactly 1 failure means
// batchCompletion.overallSucceeded's thresholdExceeded check (1 > 1) is
// false, so Map returns a non-error BatchResult here (mirroring 9-8's
// identical "within tolerance" shape) - there is no completion-policy-
// contract mismatch to route around in this particular requirement
// (unlike 9-5/9-9/9-10/9-6). After building the projection, the handler
// issues a durable Wait that suspends the whole execution; on replay,
// Map's own outer CONTEXT/MAP replay-skip (Status Succeeded) returns the
// already-checkpointed BatchResult directly without re-running the
// already-failed iteration 1, then Wait's own checkpoint replay-skips
// too.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-18", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_18Handler, config(client))
	})
}

func map9_18Handler(event any, dc types.DurableContext) (mapProjection, error) {
	tolerated := 1
	items := []string{"ok", "fail"}

	batch, err := operations.Map(dc, "fail-then-wait", items,
		func(child types.DurableContext, item string, index int) (string, error) {
			if item == "fail" {
				return "", errMapItemFailed
			}
			return item, nil
		},
		operations.WithMapMaxConcurrency[string, string](1),
		operations.WithMapCompletionConfig[string, string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return mapProjection{}, err
	}

	projection := newMapProjection(batch)

	if err := operations.Wait(dc, "pause", types.Duration{Seconds: 1}); err != nil {
		return mapProjection{}, err
	}

	return projection, nil
}

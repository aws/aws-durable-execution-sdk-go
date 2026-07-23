// Requirement 9-17: Suspension after a successful map (replay skips the
// completed map).
//
// From test-requirements/map/9-17.yaml:
//
//	description: A durable wait placed after a fully successful map
//	  suspends the execution; on replay the completed map is skipped and
//	  the execution resumes to success
//	handler: |
//	  Handler invokes the map operation over two items that each return
//	  the uppercased item directly; all iterations succeed. After the
//	  map completes, the handler issues a durable wait (1 second), which
//	  suspends the whole execution. On replay (after the wait elapses)
//	  the SDK skips the already-completed map and its iterations,
//	  checkpoints the wait success, and the execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [A, B]
//
// On the first invocation, Map runs to completion (checkpointing
// ContextSucceeded, SubType Map) and then operations.Wait suspends the
// whole execution. On replay, Map's own outer CONTEXT/MAP replay-skip
// (see that function's doc: "if existing, found :=
// c.ExecManager().GetOperation(mapID); found" with Status Succeeded)
// returns the already-checkpointed BatchResult directly without
// re-running any iteration, then Wait's own checkpoint has already
// succeeded so it replay-skips too, and the handler returns immediately.
package handlers

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-17", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_17Handler, config(client))
	})
}

func map9_17Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []string{"a", "b"}

	batch, err := operations.Map(dc, "then-wait", items,
		func(child types.DurableContext, item string, index int) (string, error) {
			return strings.ToUpper(item), nil
		},
		operations.WithMapMaxConcurrency[string, string](1),
	)
	if err != nil {
		return nil, err
	}

	if err := operations.Wait(dc, "pause", types.Duration{Seconds: 1}); err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}

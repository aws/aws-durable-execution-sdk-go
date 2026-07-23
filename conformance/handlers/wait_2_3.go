// Requirement 2-3: Multiple sequential waits.
//
// From test-requirements/wait/2-3.yaml:
//
//	description: Multiple sequential waits
//	handler: |
//	  Two sequential wait operations, each suspending and resuming the
//	  execution.
//	invocations: |
//	  - Handler invokes `context.wait(Duration.from_seconds(2),
//	    name="wait-1")`, SDK checkpoints WaitStarted, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because wait-1 completed, SDK checkpoints
//	    WaitSucceeded for wait-1, handler invokes
//	    `context.wait(Duration.from_seconds(2), name="wait-2")`, SDK
//	    checkpoints WaitStarted for wait-2, invocation completes, execution
//	    suspends.
//	  - Replay 2: Re-invoked because wait-2 completed, SDK checkpoints
//	    WaitSucceeded for wait-2, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completedWaits: 2
//
// Two sequential, distinctly-named Wait calls. Each operations.Wait call
// is independently checkpointed/replay-skipped by its own step ID (see
// wait.go's Succeeded replay-skip branch), so simply issuing both calls
// in order - wait-1 then wait-2 - produces exactly the two-suspend/
// two-resume cycle this requirement's invocations describe, with no
// extra bookkeeping needed in this handler: wait-1 replay-skips on the
// invocation that resumes it, then wait-2 executes fresh.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// wait2_3Result mirrors the YAML's ExpectedResult.Result shape
// ({completedWaits: 2}).
type wait2_3Result struct {
	CompletedWaits int `json:"completedWaits"`
}

func init() {
	Register("2-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(wait2_3Handler, config(client))
	})
}

func wait2_3Handler(event any, dc types.DurableContext) (wait2_3Result, error) {
	if err := operations.Wait(dc, "wait-1", types.Duration{Seconds: 2}); err != nil {
		return wait2_3Result{}, err
	}
	if err := operations.Wait(dc, "wait-2", types.Duration{Seconds: 2}); err != nil {
		return wait2_3Result{}, err
	}
	return wait2_3Result{CompletedWaits: 2}, nil
}

// Requirement 2-5: Wait with long duration (1 hour).
//
// From test-requirements/wait/2-5.yaml:
//
//	description: Wait with long duration (1 hour)
//	async: true
//	handler: |
//	  A wait operation with a 1-hour duration, verifying correct
//	  conversion from hours to seconds.
//	invocations: |
//	  - Handler invokes `context.wait(Duration.from_hours(1))`, SDK
//	    converts to 3600 seconds and checkpoints WaitStarted with
//	    Duration=3600, invocation completes, execution suspends.
//	AsyncInvoke: true
//	Input:
//	ExpectedExecutionHistory:
//	  ... WaitStarted WaitStartedDetails.Duration: 3600
//
// Same shape as 2-4 (AsyncInvoke: true - only the first invocation's
// checkpointed history through the suspend is validated) but exercising
// the Hours field of types.Duration instead of Minutes, producing the
// checkpointed Duration=3600 the YAML expects via the same
// Days*86400+Hours*3600+Minutes*60+Seconds arithmetic in wait.go.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("2-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(wait2_5Handler, config(client))
	})
}

func wait2_5Handler(event any, dc types.DurableContext) (*string, error) {
	if err := operations.Wait(dc, "wait_one_hour", types.Duration{Hours: 1}); err != nil {
		return nil, err
	}
	return nil, nil
}

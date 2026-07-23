// Requirement 2-2: Wait with name.
//
// From test-requirements/wait/2-2.yaml:
//
//	description: Wait with name
//	handler: |
//	  A wait operation with an explicit `name` parameter.
//	invocations: |
//	  - Handler invokes
//	    `context.wait(Duration.from_seconds(N), name="custom_wait_name")`,
//	    SDK checkpoints WaitStarted with the duration and name, invocation
//	    completes, execution suspends.
//	  - Replay 1: Re-invoked because wait completed, SDK checkpoints
//	    WaitSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... WaitStarted Name: custom_wait_name, WaitStartedDetails.Duration: 2
//	  ... WaitSucceeded Name: custom_wait_name, WaitSucceededDetails.Duration: 2
//
// operations.Wait's second parameter (id) IS the wait's name in this Go
// SDK - identical to how Step's id parameter doubles as its checkpointed
// Name (see step_1_2.go's identical note for Step). "An explicit name
// parameter" is satisfied simply by passing "custom_wait_name" as that
// id argument.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("2-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(wait2_2Handler, config(client))
	})
}

func wait2_2Handler(event any, dc types.DurableContext) (*string, error) {
	if err := operations.Wait(dc, "custom_wait_name", types.Duration{Seconds: 2}); err != nil {
		return nil, err
	}
	return nil, nil
}

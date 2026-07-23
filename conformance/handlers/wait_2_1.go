// Requirement 2-1: Wait basic.
//
// From test-requirements/wait/2-1.yaml:
//
//	description: Wait basic
//	handler: |
//	  A single wait operation with a specified duration.
//	invocations: |
//	  - Handler invokes `context.wait(Duration.from_seconds(N))`, SDK
//	    checkpoints WaitStarted with the duration, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because wait completed, SDK checkpoints
//	    WaitSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... WaitStarted SubType: Wait, WaitStartedDetails.Duration: 2
//	  ... WaitSucceeded SubType: Wait, WaitSucceededDetails.Duration: 2
//
// The simplest possible operations.Wait call: a single, unnamed 2-second
// wait with no other operation before or after it. operations.Wait itself
// only ever sends a WAIT/START checkpoint (see wait.go's doc) - the
// terminal WaitSucceeded checkpoint this requirement's
// ExpectedExecutionHistory expects is entirely backend-driven once the
// duration elapses, per this session's own briefing; nothing further is
// needed in this handler beyond the single Wait call itself.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("2-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(wait2_1Handler, config(client))
	})
}

func wait2_1Handler(event any, dc types.DurableContext) (*string, error) {
	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 2}); err != nil {
		return nil, err
	}
	return nil, nil
}

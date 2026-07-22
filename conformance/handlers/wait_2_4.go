// Requirement 2-4: Wait with different duration units.
//
// From test-requirements/wait/2-4.yaml:
//
//	description: Wait with different duration units
//	async: true
//	handler: |
//	  A wait operation using minutes as the duration unit, verifying
//	  correct conversion to seconds.
//	invocations: |
//	  - Handler invokes `context.wait(Duration.from_minutes(1))`, SDK
//	    converts to 60 seconds and checkpoints WaitStarted with
//	    Duration=60, invocation completes, execution suspends.
//	AsyncInvoke: true
//	Input:
//	ExpectedExecutionHistory:
//	  ... WaitStarted WaitStartedDetails.Duration: 60
//
// AsyncInvoke: true means the conformance runner only validates the
// FIRST invocation's checkpointed history (up through the suspend) and
// never drives this wait to completion - see this task's own briefing on
// multi-replay-cycle scenarios' async handling being entirely the
// runner's concern, not this handler's. types.Duration{Minutes: 1} is
// this SDK's own replay-safe duration type (see wait.go's toGoDuration:
// waitSeconds := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds),
// so passing Minutes: 1 rather than Seconds: 60 directly is what
// actually exercises the "using minutes as the duration unit" conversion
// this requirement describes - operations.Wait itself computes
// WaitOptions.WaitSeconds from the Duration struct's fields, giving the
// checkpointed Duration=60 the YAML expects.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("2-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(wait2_4Handler, config(client))
	})
}

func wait2_4Handler(event any, dc types.DurableContext) (*string, error) {
	if err := operations.Wait(dc, "wait_one_minute", types.Duration{Minutes: 1}); err != nil {
		return nil, err
	}
	return nil, nil
}

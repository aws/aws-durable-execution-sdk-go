// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// JobEvent is this example's input shape.
type JobEvent struct {
	JobID string `json:"jobId"`
}

// JobResult is this example's output shape.
type JobResult struct {
	JobID       string `json:"jobId"`
	FinalStatus string `json:"finalStatus"`
	PollCount   int    `json:"pollCount"`
}

// jobPollState is checkpointed after each poll - the state
// WaitForCondition threads between checkFn calls.
type jobPollState struct {
	PollCount int    `json:"pollCount"`
	Status    string `json:"status"`
}

// handler demonstrates operations.WaitForCondition: polling an external
// system (here, simulated - "completes" after 3 polls) until it reports a
// terminal status. Each poll is checkpointed, and (per the confirmed
// real-backend flowchart, internal SDK Operation Diagrams design doc,
// "WaitForCondition") the poll loop is structurally identical to Step's
// own retry loop - just triggered by "condition not yet met" rather than
// an error.
func handler(event JobEvent, dc types.DurableContext) (JobResult, error) {
	dc.Logger().Info("handler started", map[string]any{"jobId": event.JobID})

	final, err := operations.WaitForCondition(dc, "poll-job-status",
		func(sc types.StepContext, state jobPollState) (operations.ConditionResult[jobPollState], error) {
			// In production: call the external job's status API. Here we
			// simulate a job that reports "running" for its first 2 polls
			// and "completed" on the 3rd, so the example demonstrates
			// real polling rather than succeeding on the first check.
			next := jobPollState{PollCount: state.PollCount + 1}
			if next.PollCount >= 3 {
				next.Status = "completed"
			} else {
				next.Status = "running"
			}
			sc.Logger().Info("polled job status", map[string]any{"jobId": event.JobID, "pollCount": next.PollCount, "status": next.Status})

			return operations.ConditionResult[jobPollState]{
				State:        next,
				ConditionMet: next.Status == "completed",
			}, nil
		},
		jobPollState{},
		// Poll every 5 seconds (in production), up to 10 times, before
		// giving up - a fixed-delay strategy rather than exponential
		// backoff, since polling intervals for job-status checks are
		// typically constant rather than escalating.
		operations.WithConditionRetryStrategy[jobPollState](utils.Presets.FixedDelay(types.Duration{Seconds: 5}, 10)),
	)
	if err != nil {
		return JobResult{}, fmt.Errorf("job %s: %w", event.JobID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"finalStatus": final.Status, "pollCount": final.PollCount})
	return JobResult{JobID: event.JobID, FinalStatus: final.Status, PollCount: final.PollCount}, nil
}

// Command execution-start-time demonstrates [durable.ExecutionStartTime],
// the checkpointed start time of the execution. Every invocation of one
// execution reads the same value, so a handler can derive a time from it
// between durable operations without making replay non-deterministic.
//
// The handler derives a reply deadline from the start time, records it in
// a step, and waits. The wait ends the invocation, and the execution
// resumes in a new invocation that runs the handler from the beginning:
// it derives the deadline again and replays the step, which returns the
// deadline recorded by the first invocation. The two are equal because
// ExecutionStartTime returned the same value both times. A deadline
// derived from time.Now outside a step would differ between the two
// invocations; to use the wall clock, read it inside a step so that it is
// checkpointed once.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// replyWindow is how long after the execution starts a reply is due.
const replyWindow = 24 * time.Hour

// Output reports the start time, the deadline derived from it in the
// invocation that returns, and whether the deadline recorded by the first
// invocation matches it.
type Output struct {
	StartedAt              time.Time `json:"startedAt"`
	Deadline               time.Time `json:"deadline"`
	MatchesFirstInvocation bool      `json:"matchesFirstInvocation"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	startedAt := durable.ExecutionStartTime(ctx)
	deadline := startedAt.Add(replyWindow)

	// The step runs in the first invocation only. Later invocations replay
	// its checkpointed result: the deadline as the first invocation
	// derived it.
	recorded, err := durable.Step(ctx, "record-deadline", func(_ durable.StepContext) (time.Time, error) {
		return deadline, nil
	})
	if err != nil {
		return Output{}, err
	}

	// The wait suspends the execution, so the rest of the handler runs in
	// a second invocation.
	if err := durable.Wait(ctx, "cool-off", 1*time.Second); err != nil {
		return Output{}, err
	}

	return Output{
		StartedAt:              startedAt,
		Deadline:               deadline,
		MatchesFirstInvocation: recorded.Equal(deadline),
	}, nil
}

func main() { durable.Start(handler) }

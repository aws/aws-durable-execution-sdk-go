// Command parallel-min-successful-threshold demonstrates the MinSuccessful
// completion policy of [durable.Parallel] with branches that complete at
// staggered times. Two branches finish within tens of milliseconds; the
// other three take two to four seconds. With MinSuccessful set to 2 the
// batch completes as soon as the two quick branches succeed. The three
// branches still running are abandoned: their results are not awaited and
// each is reported with status STARTED, counted in TotalCount but neither
// as a success nor as a failure. The batch still lets the work already in
// flight finish before it returns, so no branch outlives the invocation.
// An abandoned branch whose step is already running finishes that step;
// one that has not yet reached its step never starts it. Either way the
// abandoned branch contexts are never marked complete.
//
// The output names the branches in each group, so the early completion is
// visible in the result and not only in the counts.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output reports how the batch completed.
type Output struct {
	SuccessCount     int      `json:"successCount"`
	StartedCount     int      `json:"startedCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	Succeeded        []string `json:"succeeded"`
	Abandoned        []string `json:"abandoned"`
}

// branch describes one branch: its name and how long its work takes.
type branch struct {
	name    string
	latency time.Duration
}

// branches are ordered fastest to slowest.
//
// A branch counts as succeeded when the checkpoint of its step's outcome is
// acknowledged. Checkpoints are batched: a call that is retried (up to
// three attempts, 100 ms and 200 ms apart) holds back every checkpoint
// queued behind it, and the branches acknowledged by one call reach the
// completion decision in any order. So the gap between the second and the
// third branch must exceed the longest stall one call can cause. Two
// seconds does, with the same margin the future-race example relies on.
var branches = []branch{
	{"fast", 10 * time.Millisecond},
	{"quick", 50 * time.Millisecond},
	{"slow", 2 * time.Second},
	{"slower", 3 * time.Second},
	{"straggler", 4 * time.Second},
}

func handler(ctx durable.Context, _ any) (Output, error) {
	specs := make([]durable.Branch[string], len(branches))
	for i, b := range branches {
		specs[i] = durable.Branch[string]{
			Name: b.name,
			Func: func(ctx durable.Context) (string, error) {
				return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
					// Simulate work that takes this branch's latency.
					time.Sleep(b.latency)
					return b.name + " done", nil
				})
			},
		}
	}

	results, err := durable.Parallel(ctx, "staggered-branches", specs,
		durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
	)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		SuccessCount:     results.SuccessCount(),
		StartedCount:     results.StartedCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		Succeeded:        []string{},
		Abandoned:        []string{},
	}
	for _, item := range results.Succeeded() {
		out.Succeeded = append(out.Succeeded, item.Name)
	}
	for _, item := range results.Started() {
		out.Abandoned = append(out.Abandoned, item.Name)
	}
	return out, nil
}

func main() { durable.Start(handler) }

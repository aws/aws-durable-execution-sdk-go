// Command parallel-should-complete demonstrates a Parallel operation with a
// custom completion callback. The batch completes when branch A (index 0)
// succeeds, or when branches B and C (indexes 1 and 2) both succeed. The
// rule keys off the branch index, which is stable, because the branches
// are unnamed.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Output struct {
	SuccessCount     int      `json:"successCount"`
	StartedCount     int      `json:"startedCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	Results          []string `json:"results"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	// The branches sleep instead of running a Step so their finish order
	// is observable: branch A is slow, so the (B AND C) arm of the rule is
	// what completes the batch, and A is still in flight at that moment.
	// The batch then abandons A: it is reported STARTED in the result and
	// its child context stays STARTED in the checkpoint log, even though
	// its body finishes before Parallel returns.
	branch := func(delay time.Duration, result string) durable.Branch[string] {
		return durable.Branch[string]{Func: func(durable.Context) (string, error) {
			time.Sleep(delay)
			return result, nil
		}}
	}

	results, err := durable.Parallel(ctx, "quorum-branches", []durable.Branch[string]{
		branch(400*time.Millisecond, "Branch A done"), // index 0 = branch A (slow)
		branch(100*time.Millisecond, "Branch B done"), // index 1 = branch B (fast)
		branch(200*time.Millisecond, "Branch C done"), // index 2 = branch C
	}, durable.WithCompletion(durable.CompletionConfig{
		// Complete when branch A succeeds, OR branches B and C both succeed.
		ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
			ok := func(i int) bool { return p.Items[i].Status == durable.BatchItemSucceeded }
			if ok(0) || (ok(1) && ok(2)) {
				return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
			}
			return durable.ContinueBatch()
		},
	}))
	if err != nil {
		return Output{}, err
	}

	if err := durable.Wait(ctx, "wait", 1*time.Second); err != nil {
		return Output{}, err
	}

	return Output{
		SuccessCount:     results.SuccessCount(),
		StartedCount:     results.StartedCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		Results:          results.Results(),
	}, nil
}

func main() { durable.Start(handler) }

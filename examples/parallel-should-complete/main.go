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
	// The branches sleep instead of running a Step. Branch A sleeps the
	// longest, so its body finishes last. A branch counts as succeeded
	// only when the checkpoint call that records its SUCCEED returns, and
	// those calls can return in a different order than the bodies finish.
	// So either arm of the rule can complete the batch:
	//
	//  1. B and C are recorded first. The (B AND C) arm completes the
	//     batch while A is in flight. The batch abandons A: it is reported
	//     STARTED in the result, and its child context stays STARTED in
	//     the checkpoint log, even though its body finishes before
	//     Parallel returns.
	//  2. A is recorded before B or C. The A arm completes the batch, and
	//     whichever of B and C is not yet recorded is abandoned in the
	//     same way.
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

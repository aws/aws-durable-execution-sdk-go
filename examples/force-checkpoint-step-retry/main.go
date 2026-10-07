// Command force-checkpoint-step-retry demonstrates status polling when a
// long-running step in one parallel branch keeps the invocation running
// while another branch retries a failing step. The SDK polls the retrying
// step's status, and each attempt runs in the same invocation once it is
// due, so the retrying step succeeds on its third attempt before the
// long-running step finishes.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) (string, error) {
	results, err := durable.Parallel(ctx, "force-cp-block", []durable.Branch[any]{
		// Branch 1: Long-running step that blocks invocation termination.
		{Name: "long-running", Func: func(branchCtx durable.Context) (any, error) {
			return durable.Step(branchCtx, "long-running-step", func(_ durable.StepContext) (string, error) {
				time.Sleep(10 * time.Second)
				return "long-complete", nil
			})
		}},
		// Branch 2: Retrying step that needs force checkpoint to persist
		// retry progress.
		{Name: "retrying", Func: func(branchCtx durable.Context) (any, error) {
			attemptCount := 0
			return durable.Step(branchCtx, "retrying-step", func(_ durable.StepContext) (string, error) {
				// The counter lives in the branch, so it restarts at 0 on
				// every invocation. All three attempts run in the first
				// invocation, so the third succeeds. A run whose retries
				// span invocations exhausts them instead, which is the
				// failure the batch tolerates below.
				attemptCount++ //durable:ignore durableclosure -- the counter shows whether the attempts ran in one invocation
				if attemptCount < 3 {
					return "", fmt.Errorf("attempt %d failed", attemptCount)
				}
				return "retry-complete", nil
			}, durable.WithRetry(func(a durable.RetryAttempt) durable.RetryDecision {
				if a.Attempt >= 5 {
					return durable.RetryDecision{Retry: false}
				}
				return durable.RetryDecision{Retry: true, Delay: 1 * time.Second}
			}))
		}},
	},
		// Tolerate a failed branch so the long-running branch still runs to
		// completion; the default completion policy would stop the batch at
		// the first failure.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(1)}))
	// Failures within the tolerance do not fail the batch: err is nil and
	// the result reports the failed items. Any non-nil err propagates.
	if err != nil {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
}

func main() { durable.Start(handler) }

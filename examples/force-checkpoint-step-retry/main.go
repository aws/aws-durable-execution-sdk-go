// Command force-checkpoint-step-retry demonstrates force-checkpoint polling
// when a long-running step in one parallel branch blocks invocation
// termination while another branch retries a failing step. The runtime's
// force-checkpoint mechanism ensures retry progress is checkpointed even
// though the long-running branch hasn't yielded.
package main

import (
	"encoding/json"
	"errors"
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
				// every invocation; the step therefore exhausts its
				// retries, which is the failure the batch tolerates below.
				attemptCount++ //durable:ignore durableclosure -- the reset on each invocation is the behaviour this example exercises
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
	// A failed branch is reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
}

func main() { durable.Start(handler) }

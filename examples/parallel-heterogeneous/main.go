// Command parallel-heterogeneous demonstrates a Parallel operation where
// each branch performs a different operation type (Step, Wait, Invoke),
// proving that mixed-operation branches execute and return correctly.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// Output collects the parallel branch results and metadata.
type Output struct {
	Results          []string `json:"results"`
	CompletionReason string   `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	results, err := durable.Parallel(ctx, "heterogeneous-branches", []durable.Branch[string]{
		{Name: "compute", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "compute-value", func(_ durable.StepContext) (string, error) {
				return "computed: 7*6=42", nil
			})
		}},
		{Name: "timer", Func: func(ctx durable.Context) (string, error) {
			_ = durable.Wait(ctx, "short-timer", 1*time.Second)
			return "waited: 1s elapsed", nil
		}},
		{Name: "invoke", Func: func(ctx durable.Context) (string, error) {
			result, err := durable.Invoke[string](ctx, "child-function", "target-handler", map[string]string{"key": "value"})
			if err != nil {
				return "", fmt.Errorf("invoke failed: %w", err)
			}
			return result, nil
		}},
	},
		// Tolerate a failed branch so every branch runs; the default
		// completion policy would stop the batch at the first failure.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(3)}))
	// Failed branches are reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	return Output{
		Results:          results.Results(),
		CompletionReason: results.Reason.String(),
	}, nil
}

func main() { durable.Start(handler) }

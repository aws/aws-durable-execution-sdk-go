// Command parallel-min-successful demonstrates a Parallel operation with
// MinSuccessful completion config that completes early once enough
// branches succeed.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Output struct {
	SuccessCount     int      `json:"successCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	Results          []string `json:"results"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	branches := make([]durable.Branch[string], 4)
	for i := range branches {
		idx := i + 1
		branches[i] = durable.Branch[string]{
			Name: fmt.Sprintf("branch-%d", idx),
			Func: func(ctx durable.Context) (string, error) {
				return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
					time.Sleep(time.Duration(100*idx) * time.Millisecond)
					return fmt.Sprintf("Branch %d result", idx), nil
				})
			},
		}
	}

	results, err := durable.Parallel(ctx, "min-successful-branches", branches,
		durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
	)
	if err != nil {
		return Output{}, err
	}

	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return Output{
		SuccessCount:     results.SuccessCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		Results:          results.Results(),
	}, nil
}

func main() { durable.Start(handler) }

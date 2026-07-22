// Command future-any demonstrates durable.Any: return the first future to
// succeed. If all futures fail, Any returns a *CombinatorError wrapping all
// individual errors.
//
// Input: {"shouldFail": true} makes all steps fail, demonstrating the
// aggregate error path.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input controls whether all steps should fail.
type Input struct {
	ShouldFail bool `json:"shouldFail"`
}

// Result reports either the winning value or the aggregate error.
type Result struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

func handler(ctx durable.Context, event Input) (Result, error) {
	f1 := durable.StepAsync(ctx, "always-fails", func(_ durable.StepContext) (string, error) {
		return "", errors.New("failure 1")
	}, durable.WithRetry(durable.NoRetry()))

	f2 := durable.StepAsync(ctx, "maybe-succeeds-1", func(_ durable.StepContext) (string, error) {
		if event.ShouldFail {
			return "", errors.New("failure 2")
		}
		return "first success", nil
	}, durable.WithRetry(durable.NoRetry()))

	f3 := durable.StepAsync(ctx, "maybe-succeeds-2", func(_ durable.StepContext) (string, error) {
		if event.ShouldFail {
			return "", errors.New("failure 3")
		}
		return "second success", nil
	}, durable.WithRetry(durable.NoRetry()))

	val, err := durable.Any(ctx, "any", []*durable.Future[string]{f1, f2, f3})
	if err != nil {
		var combErr *durable.CombinatorError
		if errors.As(err, &combErr) {
			return Result{
				Status: "all-failed",
				Error:  fmt.Sprintf("all %d futures failed", len(combErr.Errors)),
			}, nil
		}
		return Result{}, err
	}

	return Result{Status: "succeeded", Value: val}, nil
}

func main() { durable.Start(handler) }

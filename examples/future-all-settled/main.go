// Command future-all-settled demonstrates durable.AllSettled: wait for all
// futures to settle regardless of success or failure, returning each outcome
// in input order.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports the aggregate settled outcomes.
type Result struct {
	Outcomes []string `json:"outcomes"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	f1 := durable.StepAsync(ctx, "success-1", func(_ durable.StepContext) (string, error) {
		return "success", nil
	})
	f2 := durable.StepAsync(ctx, "failure", func(_ durable.StepContext) (string, error) {
		return "", errors.New("failure")
	}, durable.WithRetry(durable.NoRetry()))
	f3 := durable.StepAsync(ctx, "success-2", func(_ durable.StepContext) (string, error) {
		return "another success", nil
	})

	settled, err := durable.AllSettled(ctx, "settled", []*durable.Future[string]{f1, f2, f3})
	if err != nil {
		return Result{}, err
	}

	outcomes := make([]string, len(settled))
	for i, s := range settled {
		if s.Err != nil {
			outcomes[i] = fmt.Sprintf("rejected: %s", s.Err.Error())
		} else {
			outcomes[i] = fmt.Sprintf("fulfilled: %s", s.Value)
		}
	}

	return Result{Outcomes: outcomes}, nil
}

func main() { durable.Start(handler) }

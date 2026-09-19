// Command parallel-invalid-max-concurrency demonstrates the validation of
// [durable.WithMaxConcurrency]. The option accepts only a positive value;
// zero or a negative value is a configuration error. [durable.Parallel]
// reports it before any branch starts, so no branch is checkpointed, and
// the handler returns the error, so the execution ends FAILED rather than
// running with the option silently ignored.
//
// Expected terminal state: FAILED.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	// Invalid: max concurrency must be positive. Omit the option to run
	// every branch at once.
	results, err := durable.Parallel(ctx, "parallel", []durable.Branch[string]{
		{Name: "unreachable", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "unreachable", nil
			})
		}},
	}, durable.WithMaxConcurrency(0))
	if err != nil {
		return nil, err
	}
	return results.Results(), nil
}

func main() { durable.Start(handler) }

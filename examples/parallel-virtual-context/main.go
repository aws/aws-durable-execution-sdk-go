// Command parallel-virtual-context demonstrates a Parallel operation with
// flat nesting (virtual contexts). Under flat nesting, per-branch context
// operations are suppressed — leaf operations checkpoint directly under the
// parent batch context without ParallelBranch wrappers.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// Output holds the handler result.
type Output struct {
	Results      []string `json:"results"`
	TotalCount   int      `json:"totalCount"`
	SuccessCount int      `json:"successCount"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	results, err := durable.Parallel(ctx, "parallel-tasks-virtual", []durable.Branch[string]{
		{Name: "fetch-data", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "fetch", func(_ durable.StepContext) (string, error) {
				return "fetched", nil
			})
		}},
		{Name: "process-data", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "process", func(_ durable.StepContext) (string, error) {
				return "processed", nil
			})
		}},
		{Name: "validate-data", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "validate", func(_ durable.StepContext) (string, error) {
				return "validated", nil
			})
		}},
	}, durable.WithNesting(durable.NestingFlat))
	if err != nil {
		return Output{}, err
	}

	return Output{
		Results:      results.Results(),
		TotalCount:   results.TotalCount(),
		SuccessCount: results.SuccessCount(),
	}, nil
}

func main() { durable.Start(handler) }

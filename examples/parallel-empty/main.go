// Command parallel-empty demonstrates a Parallel operation with zero
// branches, showing the zero-value BatchResult behavior.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Results        []any  `json:"results"`
	Errors         []any  `json:"errors"`
	SuccessCount   int    `json:"successCount"`
	FailureCount   int    `json:"failureCount"`
	TotalCount     int    `json:"totalCount"`
	Status         string `json:"status"`
	CompletionNote string `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	results, err := durable.Parallel(ctx, "empty-parallel", []durable.Branch[any]{})
	if err != nil {
		return Result{}, err
	}

	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return Result{
		Results:        make([]any, 0),
		Errors:         make([]any, 0),
		SuccessCount:   results.SuccessCount(),
		FailureCount:   results.FailureCount(),
		TotalCount:     results.TotalCount(),
		Status:         results.Status(),
		CompletionNote: results.Reason.String(),
	}, nil
}

func main() { durable.Start(handler) }

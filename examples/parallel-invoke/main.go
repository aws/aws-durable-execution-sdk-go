// Command parallel-invoke demonstrates parallel branches that each
// invoke a child durable function, verifying correct suspension
// between branch completions.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Input struct {
	Branches []BranchInput `json:"branches"`
}

type BranchInput struct {
	FunctionName string `json:"functionName"`
	Payload      any    `json:"payload,omitempty"`
}

type Output struct {
	SuccessCount int `json:"successCount"`
}

func handler(ctx durable.Context, event Input) (Output, error) {
	// Default to invoke-simple-target if no branches provided.
	if len(event.Branches) == 0 {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		targetFn := prefix + "go-invoke-simple-target:$LATEST"
		event.Branches = []BranchInput{
			{FunctionName: targetFn, Payload: map[string]string{"msg": "branch-0"}},
			{FunctionName: targetFn, Payload: map[string]string{"msg": "branch-1"}},
			{FunctionName: targetFn, Payload: map[string]string{"msg": "branch-2"}},
		}
	}

	branches := make([]durable.Branch[any], len(event.Branches))
	for i, b := range event.Branches {
		branchInput := b
		idx := i
		branches[i] = durable.Branch[any]{
			Name: fmt.Sprintf("branch-%d", idx),
			Func: func(ctx durable.Context) (any, error) {
				return durable.Invoke[any](ctx, fmt.Sprintf("invoke-%d", idx),
					branchInput.FunctionName, branchInput.Payload)
			},
		}
	}

	// Tolerate every branch failure so all branches run; the default
	// completion policy would stop the batch at the first failure.
	results, err := durable.Parallel(ctx, "parallel-invokes", branches,
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(len(branches))}))
	// Failed branches are reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	return Output{SuccessCount: results.SuccessCount()}, nil
}

func main() { durable.Start(handler) }

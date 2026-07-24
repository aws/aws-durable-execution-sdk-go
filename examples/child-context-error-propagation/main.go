// Command child-context-error-propagation demonstrates that errors
// propagate correctly through nested [durable.RunInChildContext] calls.
// The innermost child throws an error that must be retrievable as a
// [*durable.ChildContextError] at the outermost level, preserving the
// error chain across multiple context boundaries.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result captures whether the error chain was preserved.
type Result struct {
	Found   bool   `json:"found"`
	ErrMsg  string `json:"errMsg,omitempty"`
	Depth   int    `json:"depth"`
	IsOpErr bool   `json:"isOpErr"`
	IsChild bool   `json:"isChild"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	err := func() error {
		_, outerErr := durable.RunInChildContext(ctx, "outer-child",
			func(outer durable.Context) (any, error) {
				_, innerErr := durable.RunInChildContext(outer, "inner-child",
					func(inner durable.Context) (any, error) {
						_, stepErr := durable.Step(inner, "throw-error",
							func(_ durable.StepContext) (any, error) {
								return nil, fmt.Errorf("intentional nested failure")
							},
							durable.WithRetry(durable.NoRetry()))
						return nil, stepErr
					})
				return nil, innerErr
			})
		return outerErr
	}()

	if err == nil {
		return Result{Found: false}, nil
	}

	// Walk the error chain to verify structure.
	var childErr *durable.ChildContextError
	var opErr *durable.OperationError
	depth := 0
	current := err
	for current != nil {
		var ce *durable.ChildContextError
		if errors.As(current, &ce) {
			depth++
		}
		current = errors.Unwrap(current)
	}

	return Result{
		Found:   true,
		ErrMsg:  err.Error(),
		Depth:   depth,
		IsOpErr: errors.As(err, &opErr),
		IsChild: errors.As(err, &childErr),
	}, nil
}

func main() { durable.Start(handler) }

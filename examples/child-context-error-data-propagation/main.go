// Command child-context-error-data-propagation demonstrates that ErrorData
// attached to an error with [durable.WithErrorData] is preserved when the
// error crosses two nested [durable.RunInChildContext] boundaries. The
// innermost step fails with a sentinel payload; the outermost
// ChildContextError exposes that payload unchanged, on the first
// invocation and on replay alike.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// sentinel is the ErrorData payload attached to the innermost failure.
const sentinel = `{"reason":"operator-cancelled"}`

// Result reports whether the payload survived the nested boundaries.
type Result struct {
	Found     bool   `json:"found"`
	ErrorData string `json:"errorData,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.RunInChildContext(ctx, "outer-child",
		func(outer durable.Context) (any, error) {
			return durable.RunInChildContext(outer, "inner-child",
				func(inner durable.Context) (any, error) {
					return durable.Step(inner, "throw-with-error-data",
						func(_ durable.StepContext) (any, error) {
							return nil, durable.WithErrorData(fmt.Errorf("cb failed"), sentinel)
						},
						// Fail fast: skip retries so the example completes
						// without waiting out the default retry window.
						durable.WithRetry(durable.NoRetry()))
				})
		})
	if err == nil {
		return Result{Found: false}, nil
	}

	// Every typed operation error exposes ErrorData; the payload propagates
	// through the ChildContextError of each nesting level.
	var opErr *durable.OperationError
	if !errors.As(err, &opErr) {
		return Result{}, err
	}
	return Result{
		Found:     opErr.ErrorData != "",
		ErrorData: opErr.ErrorData,
		ErrorType: opErr.ErrorType,
	}, nil
}

func main() { durable.Start(handler) }

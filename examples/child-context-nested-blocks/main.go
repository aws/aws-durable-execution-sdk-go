// Command child-context-nested-blocks demonstrates multi-level nesting of
// [durable.RunInChildContext]: a parent context contains a child context,
// which itself contains a grandchild context with a leaf Step. Results
// bubble up through each nesting level, and every level is independently
// checkpointed.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output captures the final result assembled from nested contexts.
type Output struct {
	GrandchildValue string `json:"grandchildValue"`
	ChildValue      string `json:"childValue"`
	ParentValue     string `json:"parentValue"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	childResult, err := durable.RunInChildContext(ctx, "parent-block",
		func(child durable.Context) (string, error) {
			grandchildResult, err := durable.RunInChildContext(child, "child-block",
				func(grandchild durable.Context) (string, error) {
					return durable.Step(grandchild, "leaf-step",
						func(_ durable.StepContext) (string, error) {
							return "deep-value", nil
						})
				})
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("child-wraps(%s)", grandchildResult), nil
		})
	if err != nil {
		return Output{}, err
	}

	return Output{
		GrandchildValue: "deep-value",
		ChildValue:      childResult,
		ParentValue:     fmt.Sprintf("parent-wraps(%s)", childResult),
	}, nil
}

func main() { durable.Start(handler) }

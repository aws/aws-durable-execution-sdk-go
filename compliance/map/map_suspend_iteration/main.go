// Command map_suspend_iteration implements conformance requirement 9-15: A
// wait inside one map iteration suspends; on replay the succeeded iteration
// is skipped and the suspended iteration resumes.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"r0", "r1"}
	result, err := durable.Map(ctx, "suspend", items, func(childCtx durable.Context, item string, index int) (string, error) {
		if index == 0 {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) {
				return item, nil
			})
		}
		// Item 1: wait then step.
		if err := durable.Wait(childCtx, "", 1*time.Second); err != nil {
			return "", err
		}
		return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) {
			return item, nil
		})
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}

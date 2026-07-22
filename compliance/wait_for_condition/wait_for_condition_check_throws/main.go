// Command wait_for_condition_check_throws implements conformance requirement
// 6-7: check function raises an error on the first attempt, uncaught.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, _ any) (any, error) {
		return nil, errors.New("check function failed")
	}, durable.ConditionConfig[any]{
		InitialState: nil,
		WaitStrategy: func(_ any, _ int) durable.WaitDecision {
			return durable.WaitDecision{Continue: false}
		},
	})
}

func main() {
	durable.Start(handler)
}

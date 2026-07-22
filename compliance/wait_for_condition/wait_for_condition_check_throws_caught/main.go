// Command wait_for_condition_check_throws_caught implements conformance
// requirement 6-8: check throws but handler catches and returns "recovered".
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	_, err := durable.WaitForCondition(ctx, "", func(_ durable.StepContext, _ any) (any, error) {
		return nil, errors.New("check function failed")
	}, durable.ConditionConfig[any]{
		InitialState: nil,
		WaitStrategy: func(_ any, _ int) durable.WaitDecision {
			return durable.WaitDecision{Continue: false}
		},
	})
	if err != nil {
		return "recovered", nil
	}
	return "", nil
}

func main() {
	durable.Start(handler)
}

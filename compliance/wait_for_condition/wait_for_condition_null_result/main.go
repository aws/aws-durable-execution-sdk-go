// Command wait_for_condition_null_result implements conformance requirement
// 6-10: check returns null, strategy stops immediately, result is null.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (any, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, _ any) (any, error) {
		return nil, nil
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

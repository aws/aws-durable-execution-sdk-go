// Command wait_for_condition_immediate implements conformance requirement
// 6-2: condition is already satisfied on the first check (state >= 5).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, initialState int) (int, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state, nil
	}, durable.ConditionConfig[int]{
		InitialState: initialState,
		WaitStrategy: func(state int, _ int) durable.WaitDecision {
			if state >= 5 {
				return durable.WaitDecision{Continue: false}
			}
			return durable.WaitDecision{Continue: true, Delay: time.Second}
		},
	})
}

func main() {
	durable.Start(handler)
}

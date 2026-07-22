// Command wait_for_condition_multiple_sequential implements conformance
// requirement 6-13: two sequential wait_for_condition ops, first result
// seeds the second.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func makeStrategy(threshold int) func(int, int) durable.WaitDecision {
	return func(state int, _ int) durable.WaitDecision {
		if state >= threshold {
			return durable.WaitDecision{Continue: false}
		}
		return durable.WaitDecision{Continue: true, Delay: time.Second}
	}
}

func handler(ctx durable.Context, _ any) (int, error) {
	first, err := durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state + 1, nil
	}, durable.ConditionConfig[int]{
		InitialState: 0,
		WaitStrategy: makeStrategy(2),
	})
	if err != nil {
		return 0, err
	}

	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state + 1, nil
	}, durable.ConditionConfig[int]{
		InitialState: first,
		WaitStrategy: makeStrategy(4),
	})
}

func main() {
	durable.Start(handler)
}

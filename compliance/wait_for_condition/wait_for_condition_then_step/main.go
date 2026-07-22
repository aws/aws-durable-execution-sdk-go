// Command wait_for_condition_then_step implements conformance requirement
// 6-12: poll result feeds a subsequent step that multiplies by 10.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, threshold int) (int, error) {
	pollResult, err := durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state + 1, nil
	}, durable.ConditionConfig[int]{
		InitialState: 0,
		WaitStrategy: func(state int, _ int) durable.WaitDecision {
			if state >= threshold {
				return durable.WaitDecision{Continue: false}
			}
			return durable.WaitDecision{Continue: true, Delay: time.Second}
		},
	})
	if err != nil {
		return 0, err
	}

	return durable.Step(ctx, "", func(_ durable.StepContext) (int, error) {
		return pollResult * 10, nil
	})
}

func main() {
	durable.Start(handler)
}

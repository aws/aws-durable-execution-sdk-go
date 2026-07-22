// Command wait_for_condition_with_name implements conformance requirement
// 6-3: wait-for-condition with an explicit operation name "poll-status".
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, threshold int) (int, error) {
	return durable.WaitForCondition(ctx, "poll-status", func(_ durable.StepContext, state int) (int, error) {
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
}

func main() {
	durable.Start(handler)
}

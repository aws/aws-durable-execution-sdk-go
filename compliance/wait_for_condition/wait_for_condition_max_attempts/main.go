// Command wait_for_condition_max_attempts implements conformance requirement
// 6-6: condition never met, strategy exhausts 3 attempts then fails.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (int, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state + 1, nil
	}, durable.ConditionConfig[int]{
		InitialState: 0,
		WaitStrategy: func(_ int, attempt int) durable.WaitDecision {
			if attempt >= 3 {
				return durable.WaitDecision{
					Continue: false,
					Err:      errors.New("max attempts exceeded"),
				}
			}
			return durable.WaitDecision{Continue: true, Delay: time.Second}
		},
	})
}

func main() {
	durable.Start(handler)
}

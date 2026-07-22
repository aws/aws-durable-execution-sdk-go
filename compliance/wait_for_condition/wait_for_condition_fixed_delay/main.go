// Command wait_for_condition_fixed_delay implements conformance requirement
// 6-5: fixed 2-second delay between polls, no jitter.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, threshold int) (int, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state int) (int, error) {
		return state + 1, nil
	}, durable.ConditionConfig[int]{
		InitialState: 0,
		WaitStrategy: func(state int, attempt int) durable.WaitDecision {
			if state >= threshold {
				return durable.WaitDecision{Continue: false}
			}
			// Safety: max 100 attempts to avoid infinite polling.
			if attempt >= 100 {
				return durable.WaitDecision{Continue: false, Err: errors.New("max attempts exceeded")}
			}
			return durable.WaitDecision{Continue: true, Delay: 2 * time.Second}
		},
	})
}

func main() {
	durable.Start(handler)
}

// Command wait-for-condition demonstrates [durable.WaitForCondition]:
// a polling loop that checkpoints state between attempts and suspends
// for a configured delay until the condition is met or attempts exhaust.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (int, error) {
	// Poll until state reaches 3. Each cycle increments the counter,
	// suspends for 1 second, then re-invokes. The wait strategy caps
	// at 5 attempts to ensure deterministic termination.
	result, err := durable.WaitForCondition(ctx, "counter-poll",
		func(_ durable.StepContext, state int) (int, error) {
			return state + 1, nil
		},
		durable.ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, attempt int) durable.WaitDecision {
				if state != attempt {
					return durable.WaitDecision{
						Err: fmt.Errorf("state %d does not match attempt %d", state, attempt),
					}
				}
				if state >= 3 {
					// Condition met — stop polling.
					return durable.WaitDecision{Continue: false}
				}
				// Keep polling with a 1-second delay.
				return durable.WaitDecision{Continue: true, Delay: 1 * time.Second}
			},
		},
	)
	if err != nil {
		return 0, err
	}
	return result, nil
}

func main() { durable.Start(handler) }

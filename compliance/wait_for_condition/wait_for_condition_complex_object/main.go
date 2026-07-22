// Command wait_for_condition_complex_object implements conformance
// requirement 6-9: structured object state {status, attempts}.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type pollState struct {
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}

func handler(ctx durable.Context, _ any) (pollState, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state pollState) (pollState, error) {
		state.Attempts++
		if state.Attempts >= 2 {
			state.Status = "DONE"
		}
		return state, nil
	}, durable.ConditionConfig[pollState]{
		InitialState: pollState{Status: "PENDING", Attempts: 0},
		WaitStrategy: func(state pollState, _ int) durable.WaitDecision {
			if state.Status == "DONE" {
				return durable.WaitDecision{Continue: false}
			}
			return durable.WaitDecision{Continue: true, Delay: time.Second}
		},
	})
}

func main() {
	durable.Start(handler)
}

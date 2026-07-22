// Command force-checkpoint-callback demonstrates force-checkpoint polling
// when a long-running step in one parallel branch blocks invocation
// termination while another branch performs sequential callbacks. The
// runtime's force-checkpoint mechanism ensures callbacks are checkpointed
// even though the long-running branch hasn't yielded.
package main

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	results, err := durable.Parallel(ctx, "force-cp-block", []durable.Branch[any]{
		// Branch 1: Long-running step that blocks invocation termination.
		{Name: "long-running", Func: func(branchCtx durable.Context) (any, error) {
			return durable.Step(branchCtx, "long-running-step", func(_ durable.StepContext) (string, error) {
				time.Sleep(10 * time.Second)
				return "long-complete", nil
			})
		}},
		// Branch 2: Sequential callbacks that need force checkpoint.
		{Name: "callbacks", Func: func(branchCtx durable.Context) (any, error) {
			cb1, err := durable.CreateCallback[string](branchCtx, "callback-1")
			if err != nil {
				return nil, err
			}
			if _, err := cb1.Result(); err != nil {
				return nil, err
			}

			cb2, err := durable.CreateCallback[string](branchCtx, "callback-2")
			if err != nil {
				return nil, err
			}
			if _, err := cb2.Result(); err != nil {
				return nil, err
			}

			cb3, err := durable.CreateCallback[string](branchCtx, "callback-3")
			if err != nil {
				return nil, err
			}
			if _, err := cb3.Result(); err != nil {
				return nil, err
			}

			return "callbacks-complete", nil
		}},
	})
	if err != nil {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
}

func main() { durable.Start(handler) }

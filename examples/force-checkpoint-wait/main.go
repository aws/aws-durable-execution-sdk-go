// Command force-checkpoint-wait demonstrates force-checkpoint polling when
// a long-running step in one parallel branch blocks invocation termination
// while another branch performs multiple sequential waits. The runtime's
// force-checkpoint mechanism ensures waits are checkpointed even though the
// long-running branch hasn't yielded.
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
		// Branch 2: Sequential waits that need force checkpoint.
		{Name: "waits", Func: func(branchCtx durable.Context) (any, error) {
			_ = durable.Wait(branchCtx, "wait-1", 1*time.Second)
			_ = durable.Wait(branchCtx, "wait-2", 1*time.Second)
			_ = durable.Wait(branchCtx, "wait-3", 1*time.Second)
			_ = durable.Wait(branchCtx, "wait-4", 1*time.Second)
			_ = durable.Wait(branchCtx, "wait-5", 1*time.Second)
			return "waits-complete", nil
		}},
	})
	if err != nil {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
}

func main() { durable.Start(handler) }

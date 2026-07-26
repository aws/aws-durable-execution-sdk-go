// Command force-checkpoint-invoke demonstrates force-checkpoint polling
// when a long-running step in one parallel branch blocks invocation
// termination while another branch performs sequential invokes. The
// runtime's force-checkpoint mechanism ensures invoke results are
// checkpointed even though the long-running branch hasn't yielded.
package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type event struct {
	FunctionNames []string `json:"functionNames"`
}

func handler(ctx durable.Context, ev event) (string, error) {
	functionNames := ev.FunctionNames
	if len(functionNames) == 0 {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		target := prefix + "go-invoke-simple-target:$LATEST"
		functionNames = []string{target, target, target}
	}
	results, err := durable.Parallel(ctx, "force-cp-block", []durable.Branch[any]{
		// Branch 1: Long-running step that blocks invocation termination.
		{Name: "long-running", Func: func(branchCtx durable.Context) (any, error) {
			return durable.Step(branchCtx, "long-running-step", func(_ durable.StepContext) (string, error) {
				time.Sleep(20 * time.Second)
				return "long-complete", nil
			})
		}},
		// Branch 2: Sequential invokes that need force checkpoint.
		{Name: "invokes", Func: func(branchCtx durable.Context) (any, error) {
			type payload struct {
				Input string `json:"input"`
			}
			_, err := durable.Invoke[json.RawMessage](branchCtx, "invoke-1", functionNames[0], payload{Input: "data-1"})
			if err != nil {
				return nil, err
			}
			_, err = durable.Invoke[json.RawMessage](branchCtx, "invoke-2", functionNames[1], payload{Input: "data-2"})
			if err != nil {
				return nil, err
			}
			_, err = durable.Invoke[json.RawMessage](branchCtx, "invoke-3", functionNames[2], payload{Input: "data-3"})
			if err != nil {
				return nil, err
			}
			return "invokes-complete", nil
		}},
	})
	if err != nil {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
}

func main() { durable.Start(handler) }

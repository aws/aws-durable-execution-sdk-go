// Command block-example composes several operations inside nested child
// contexts. It corresponds to the reference example block-example ("advanced
// child context example").
//
// A parent child context runs a [durable.Step], then a nested child context
// that itself pauses with [durable.Wait] before returning. The parent
// assembles both results into one struct, which is checkpointed as the
// parent's result. The wait forces a replay, so the parent's result is
// restored from its checkpoint without re-entering either block.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// blockResult is the value the parent block returns. It is checkpointed as
// the parent context's result and becomes the handler result.
type blockResult struct {
	NestedStep  string `json:"nestedStep"`
	NestedBlock string `json:"nestedBlock"`
}

func handler(ctx durable.Context, _ any) (blockResult, error) {
	ctx.Logger().Info("handler started")

	result, err := durable.RunInChildContext(ctx, "parent-block",
		func(child durable.Context) (blockResult, error) {
			child.Logger().Info("inside parent block")

			nestedStep, err := durable.Step(child, "nested-step",
				func(sc durable.StepContext) (string, error) {
					sc.Logger().Info("inside nested step")
					return "nested step result", nil
				})
			if err != nil {
				return blockResult{}, err
			}

			nestedBlock, err := durable.RunInChildContext(child, "nested-block",
				func(grandchild durable.Context) (string, error) {
					grandchild.Logger().Info("inside nested block")
					if err := durable.Wait(grandchild, "", 1*time.Second); err != nil {
						return "", err
					}
					return "nested block result", nil
				})
			if err != nil {
				return blockResult{}, err
			}

			return blockResult{NestedStep: nestedStep, NestedBlock: nestedBlock}, nil
		})
	if err != nil {
		return blockResult{}, err
	}

	ctx.Logger().Info("block completed", "nestedStep", result.NestedStep, "nestedBlock", result.NestedBlock)
	return result, nil
}

func main() { durable.Start(handler) }

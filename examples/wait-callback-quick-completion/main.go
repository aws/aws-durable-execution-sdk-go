// Command wait-callback-quick-completion demonstrates a WaitForCallback
// where the submitter completes the callback so quickly that the execution
// may resolve within the same invocation cycle. This tests the "quick
// completion" path where the callback is already settled when Result() is
// called.
package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Value   string `json:"value"`
	Success bool   `json:"success"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	value, err := durable.WaitForCallback[string](ctx, "quick-callback",
		func(sctx durable.StepContext, callbackID string) error {
			// Immediately complete the callback — no delay.
			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)

			result, _ := json.Marshal("instant")
			_, err = client.SendDurableExecutionCallbackSuccess(
				sctx,
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     result,
				})
			return err
		},
	)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value, Success: true}, nil
}

func main() { durable.Start(handler) }

// Command wait-callback-basic demonstrates a WaitForCallback that
// self-completes via the submitter function. The submitter calls
// SendDurableExecutionCallbackSuccess directly, so the callback resolves
// without needing an external system.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Value string `json:"value"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	value, err := durable.WaitForCallback[string](ctx, "my-callback",
		func(sctx durable.StepContext, callbackID string) error {
			sctx.Logger().Info("Submitter sending callback success",
				"callbackId", callbackID)

			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)

			result, _ := json.Marshal("hello from callback")
			_, err = client.SendDurableExecutionCallbackSuccess(
				sctx,
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     result,
				})
			return err
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func main() { durable.Start(handler) }

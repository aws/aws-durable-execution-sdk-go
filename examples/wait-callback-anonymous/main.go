// Command wait-callback-anonymous demonstrates WaitForCallback with an
// anonymous (inline) submitter. The callback completes via self-send in
// the submitter, mirroring the JS anonymous submitter pattern.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Value     string `json:"value"`
	Completed bool   `json:"completed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	value, err := durable.WaitForCallback[string](ctx, "anonymous-callback",
		func(sctx durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)
			result, _ := json.Marshal("anonymous-result")
			_, err = client.SendDurableExecutionCallbackSuccess(
				context.Background(),
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
	return Result{Value: value, Completed: true}, nil
}

func main() { durable.Start(handler) }

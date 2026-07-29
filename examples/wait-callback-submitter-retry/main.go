// Command wait-callback-submitter-retry demonstrates WaitForCallback with
// a submitter retry strategy. Uses MustNewRetryStrategy to configure
// exponential backoff for up to 4 attempts.
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
	Value   string `json:"value"`
	Success bool   `json:"success"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	value, err := durable.WaitForCallback[string](ctx, "retry-submitter-callback",
		func(sctx durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)
			result, _ := json.Marshal("retry-success")
			_, err = client.SendDurableExecutionCallbackSuccess(
				context.Background(),
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     result,
				})
			return err
		},
		durable.WithCallbackTimeout(30*time.Second),
		durable.WithSubmitterRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  4,
			InitialDelay: 1 * time.Second,
			MaxDelay:     8 * time.Second,
		})),
	)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value, Success: true}, nil
}

func main() { durable.Start(handler) }

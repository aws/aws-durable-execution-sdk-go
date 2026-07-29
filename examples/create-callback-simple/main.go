// Command create-callback-simple demonstrates the basic CreateCallback
// pattern. A callback is created, and a durable Step sends the success
// response (ensuring idempotent replay). The callback result is then
// awaited.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Void is a placeholder for steps that return no meaningful value.
type Void = struct{}

type Result struct {
	Value string `json:"value"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[string](ctx, "my-callback",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Send the callback success in a durable Step so it runs exactly once.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-callback", func(sctx durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(sctx)
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)
		payload, _ := json.Marshal("hello from external")
		_, err = client.SendDurableExecutionCallbackSuccess(sctx,
			&lambdasvc.SendDurableExecutionCallbackSuccessInput{
				CallbackId: &callbackID,
				Result:     payload,
			})
		return Void{}, err
	})
	if err != nil {
		return Result{}, fmt.Errorf("send callback: %w", err)
	}

	value, err := cb.Result()
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func main() { durable.Start(handler) }

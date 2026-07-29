// Command create-callback-concurrent demonstrates multiple concurrent
// CreateCallback operations. Three callbacks are created, each completed
// via a durable Step that calls the Lambda callback API, and all results
// are collected.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Void is a placeholder for steps returning no value.
type Void = struct{}

type Result struct {
	Results      []string `json:"results"`
	AllCompleted bool     `json:"allCompleted"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb1, err := durable.CreateCallback[string](ctx, "api-call-1",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}
	cb2, err := durable.CreateCallback[string](ctx, "api-call-2",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}
	cb3, err := durable.CreateCallback[string](ctx, "api-call-3",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Complete each callback in a durable Step for idempotent replay.
	callbacks := []*durable.Callback[string]{cb1, cb2, cb3}
	for i, cb := range callbacks {
		callbackID := cb.ID()
		stepName := fmt.Sprintf("send-callback-%d", i+1)
		payload, _ := json.Marshal(fmt.Sprintf("result-%d", i+1))

		_, err = durable.Step[Void](ctx, stepName, func(sctx durable.StepContext) (Void, error) {
			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return Void{}, err
			}
			client := lambdasvc.NewFromConfig(cfg)
			_, err = client.SendDurableExecutionCallbackSuccess(sctx,
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     payload,
				})
			return Void{}, err
		})
		if err != nil {
			return Result{}, fmt.Errorf("send callback %d: %w", i+1, err)
		}
	}

	// Collect results.
	r1, err := cb1.Result()
	if err != nil {
		return Result{}, err
	}
	r2, err := cb2.Result()
	if err != nil {
		return Result{}, err
	}
	r3, err := cb3.Result()
	if err != nil {
		return Result{}, err
	}

	return Result{Results: []string{r1, r2, r3}, AllCompleted: true}, nil
}

func main() { durable.Start(handler) }

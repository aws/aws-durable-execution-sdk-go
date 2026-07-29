// Command create-callback-heartbeat demonstrates CreateCallback with
// a heartbeat timeout. A durable Step sends a heartbeat to keep the
// callback alive, then completes it.
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
	Value string `json:"longTaskResult"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[string](ctx, "long-running-task",
		durable.WithCallbackHeartbeatTimeout(10*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Send heartbeat + success in a durable Step.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-heartbeat-and-complete", func(sctx durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(sctx)
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)

		// Send heartbeat.
		_, err = client.SendDurableExecutionCallbackHeartbeat(
			sctx,
			&lambdasvc.SendDurableExecutionCallbackHeartbeatInput{
				CallbackId: &callbackID,
			})
		if err != nil {
			return Void{}, fmt.Errorf("heartbeat: %w", err)
		}

		// Complete the callback.
		result, _ := json.Marshal("long-task-done")
		_, err = client.SendDurableExecutionCallbackSuccess(
			sctx,
			&lambdasvc.SendDurableExecutionCallbackSuccessInput{
				CallbackId: &callbackID,
				Result:     result,
			})
		return Void{}, err
	})
	if err != nil {
		return Result{}, err
	}

	value, err := cb.Result()
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func main() { durable.Start(handler) }

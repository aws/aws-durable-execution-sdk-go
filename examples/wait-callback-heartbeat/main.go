// Command wait-callback-heartbeat demonstrates sending heartbeats during
// a long-running callback. The submitter sends a heartbeat to keep the
// callback alive, then completes it successfully.
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
	value, err := durable.WaitForCallback[string](ctx, "heartbeat-callback",
		func(sctx durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)

			// Send a heartbeat to reset the timeout.
			_, err = client.SendDurableExecutionCallbackHeartbeat(
				context.Background(),
				&lambdasvc.SendDurableExecutionCallbackHeartbeatInput{
					CallbackId: &callbackID,
				})
			if err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}

			sctx.Logger().Info("Heartbeat sent, now completing", "callbackId", callbackID)

			// Complete the callback.
			result, _ := json.Marshal("heartbeat-success")
			_, err = client.SendDurableExecutionCallbackSuccess(
				context.Background(),
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     result,
				})
			return err
		},
		durable.WithCallbackHeartbeatTimeout(10*time.Second),
	)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value, Completed: true}, nil
}

func main() { durable.Start(handler) }

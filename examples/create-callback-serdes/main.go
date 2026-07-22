// Command create-callback-serdes demonstrates CreateCallback that
// receives structured data requiring custom deserialization handling.
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

// Void is a placeholder for steps returning no value.
type Void = struct{}

type CustomData struct {
	ID        int    `json:"id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type Result struct {
	ReceivedData CustomData `json:"receivedData"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[CustomData](ctx, "custom-serdes-callback",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Send structured data in a durable Step.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-custom-data", func(_ durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)

		data := CustomData{
			ID:        99,
			Message:   "custom serialized data",
			Timestamp: "2026-07-01T12:00:00Z",
		}
		payload, _ := json.Marshal(data)
		_, err = client.SendDurableExecutionCallbackSuccess(
			context.Background(),
			&lambdasvc.SendDurableExecutionCallbackSuccessInput{
				CallbackId: &callbackID,
				Result:     payload,
			})
		return Void{}, err
	})
	if err != nil {
		return Result{}, err
	}

	result, err := cb.Result()
	if err != nil {
		return Result{}, err
	}
	return Result{ReceivedData: result}, nil
}

func main() { durable.Start(handler) }

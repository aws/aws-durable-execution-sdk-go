// Command wait-callback-serdes demonstrates WaitForCallback with custom
// serialization/deserialization. The callback sends structured data that
// is deserialized with a custom unmarshaler.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type CustomData struct {
	ID        int    `json:"id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Metadata  struct {
		Version   string `json:"version"`
		Processed bool   `json:"processed"`
	} `json:"metadata"`
}

type Result struct {
	ReceivedData CustomData `json:"receivedData"`
	IsProcessed  bool       `json:"isProcessed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	result, err := durable.WaitForCallback[CustomData](ctx, "custom-serdes-callback",
		func(sctx durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)

			data := CustomData{
				ID:        42,
				Message:   "serialized callback data",
				Timestamp: "2026-01-01T00:00:00Z",
			}
			data.Metadata.Version = "1.0"
			data.Metadata.Processed = true
			payload, _ := json.Marshal(data)

			_, err = client.SendDurableExecutionCallbackSuccess(
				sctx,
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     payload,
				})
			return err
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, err
	}

	return Result{
		ReceivedData: result,
		IsProcessed:  result.Metadata.Processed,
	}, nil
}

func main() { durable.Start(handler) }

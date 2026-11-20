// Command wait-callback-serdes demonstrates [durable.WaitForCallback] with a
// per-callback serializer set through [durable.WithCallbackSerdes]. The
// submitter step sends structured JSON with Metadata.Processed unset, and
// the callback result is decoded with processedSerdes, which
// [durable.SerdesOf] builds on [durable.JSONSerdes] and which sets
// Metadata.Processed. IsProcessed is therefore true only when the
// per-callback override ran. For a handler-wide callback decoder see the
// serde-callback-deserializer example and [durable.WithCallbackDeserializer].
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

// processedSerdes decodes the callback payload with durable.JSONSerdes and
// marks it processed. The sender produces the payload bytes and the SDK
// decodes them, so Marshal only delegates to durable.JSONSerdes.
var processedSerdes = durable.SerdesOf(
	func(ctx context.Context, meta durable.SerdesContext, d CustomData) ([]byte, error) {
		return durable.JSONSerdes.Marshal(ctx, meta, d)
	},
	func(ctx context.Context, meta durable.SerdesContext, data []byte) (CustomData, error) {
		var d CustomData
		if err := durable.JSONSerdes.Unmarshal(ctx, meta, data, &d); err != nil {
			return d, err
		}
		d.Metadata.Processed = true
		return d, nil
	},
)

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
		durable.WithCallbackSerdes(processedSerdes),
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

// Command create-callback-serdes demonstrates [durable.CreateCallback] with a
// per-callback serializer set through [durable.WithCallbackSerdes]. A durable
// Step sends structured JSON to the callback, and cb.Result decodes it with
// messageSerdes, which [durable.SerdesOf] builds on [durable.JSONSerdes] and
// which uppercases Message. Default decoding keeps the sender's casing, so an
// uppercased Message shows the per-callback override took effect. For a
// handler-wide callback decoder, see the serde-callback-deserializer example
// and [durable.WithCallbackDeserializer].
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

// messageSerdes decodes the callback payload with durable.JSONSerdes and
// uppercases Message. The sender produces the payload bytes and the SDK
// decodes them, so Marshal only delegates to durable.JSONSerdes.
var messageSerdes = durable.SerdesOf(
	func(ctx context.Context, meta durable.SerdesContext, d CustomData) ([]byte, error) {
		return durable.JSONSerdes.Marshal(ctx, meta, d)
	},
	func(ctx context.Context, meta durable.SerdesContext, data []byte) (CustomData, error) {
		var d CustomData
		if err := durable.JSONSerdes.Unmarshal(ctx, meta, data, &d); err != nil {
			return d, err
		}
		d.Message = strings.ToUpper(d.Message)
		return d, nil
	},
)

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[CustomData](ctx, "custom-serdes-callback",
		durable.WithCallbackTimeout(30*time.Second),
		durable.WithCallbackSerdes(messageSerdes))
	if err != nil {
		return Result{}, err
	}

	// Send structured data in a durable Step.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-custom-data", func(sctx durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(sctx)
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
			sctx,
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

// Command serde-callback-deserializer demonstrates configuring a
// handler-level callback deserializer with [durable.WithCallbackDeserializer].
// The custom deserializer applies an observable transformation (uppercasing)
// that default JSON decoding would never produce, proving the custom logic
// runs for every callback in the execution.
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

// uppercaseDeserializer JSON-decodes callback payloads and uppercases any
// string value. This is observably different from the default deserializer,
// which would preserve the original casing.
type uppercaseDeserializer struct{}

func (uppercaseDeserializer) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}
	// If the target is a *string, uppercase the decoded value.
	if sp, ok := v.(*string); ok {
		*sp = strings.ToUpper(*sp)
	}
	return nil
}

// Output captures the results of two callbacks, both of which should
// reflect the custom deserializer's uppercasing transformation.
type Output struct {
	First  string `json:"first"`
	Second string `json:"second"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	first, err := durable.WaitForCallback[string](ctx, "approval-1",
		func(_ durable.StepContext, callbackID string) error {
			return completeCallback(callbackID, "hello first")
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Output{}, fmt.Errorf("callback-1 result: %w", err)
	}

	second, err := durable.WaitForCallback[string](ctx, "approval-2",
		func(_ durable.StepContext, callbackID string) error {
			return completeCallback(callbackID, "hello second")
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Output{}, fmt.Errorf("callback-2 result: %w", err)
	}

	return Output{First: first, Second: second}, nil
}

func completeCallback(callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	client := lambdasvc.NewFromConfig(cfg)
	result, _ := json.Marshal(value)
	_, err = client.SendDurableExecutionCallbackSuccess(context.Background(),
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     result,
		})
	return err
}

func main() { durable.Start(handler, durable.WithCallbackDeserializer(uppercaseDeserializer{})) }

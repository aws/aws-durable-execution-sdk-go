// Command serde-callback-deserializer demonstrates configuring a
// handler-level callback deserializer with [durable.WithCallbackDeserializer].
// The custom deserializer applies an observable transformation (uppercasing)
// that default JSON decoding would never produce, proving the custom logic
// runs for every callback in the execution.
package main

import (
	"encoding/json"
	"fmt"
	"strings"

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
	cb1, err := durable.CreateCallback[string](ctx, "approval-1")
	if err != nil {
		return Output{}, fmt.Errorf("create callback-1: %w", err)
	}

	cb2, err := durable.CreateCallback[string](ctx, "approval-2")
	if err != nil {
		return Output{}, fmt.Errorf("create callback-2: %w", err)
	}

	first, err := cb1.Result()
	if err != nil {
		return Output{}, fmt.Errorf("callback-1 result: %w", err)
	}

	second, err := cb2.Result()
	if err != nil {
		return Output{}, fmt.Errorf("callback-2 result: %w", err)
	}

	return Output{First: first, Second: second}, nil
}

func main() { durable.Start(handler, durable.WithCallbackDeserializer(uppercaseDeserializer{})) }

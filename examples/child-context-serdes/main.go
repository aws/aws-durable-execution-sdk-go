// Command child-context-serdes demonstrates [durable.RunInChildContext]
// with a custom [durable.WithChildSerdes] option. The custom serializer
// uppercases string results, verifying that the serdes round-trip is
// applied on the child context's return value.
package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseSerdes uppercases on marshal and passes through on unmarshal.
type uppercaseSerdes struct{}

func (uppercaseSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return []byte(strings.ToUpper(string(b))), nil
}

func (uppercaseSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Input accepts an optional payload string.
type Input struct {
	Payload string `json:"payload"`
}

func handler(ctx durable.Context, event Input) (string, error) {
	payload := event.Payload
	if payload == "" {
		payload = "hello"
	}

	return durable.RunInChildContext(ctx, "serdes-child",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
				return payload, nil
			})
		},
		durable.WithChildSerdes(uppercaseSerdes{}))
}

func main() { durable.Start(handler) }

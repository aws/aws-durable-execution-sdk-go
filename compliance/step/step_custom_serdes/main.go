// Command step_custom_serdes implements conformance requirement 1-6: a
// step with a custom serdes that uppercases string values on
// serialization.
package main

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseSerdes uppercases the serialized form; deserialization is
// standard JSON.
type uppercaseSerdes struct{}

func (uppercaseSerdes) Marshal(_ durable.SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return []byte(strings.ToUpper(string(b))), nil
}

func (uppercaseSerdes) Unmarshal(_ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func handler(ctx durable.Context, event string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return event, nil
	}, durable.WithStepSerdes(uppercaseSerdes{}))
}

func main() {
	durable.Start(handler)
}

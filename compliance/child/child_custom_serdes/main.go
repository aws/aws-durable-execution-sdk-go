// Command child_custom_serdes implements conformance requirement 3-14: a
// child context with a custom serdes that uppercases the result on
// serialization.
package main

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseSerdes uppercases the raw serialized form; deserialization
// returns the data as-is (the checkpoint contains the uppercase value).
type uppercaseSerdes struct{}

func (uppercaseSerdes) Marshal(_ durable.SerdesContext, v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return []byte(strings.ToUpper(string(b))), nil
	}
	return []byte(strings.ToUpper(s)), nil
}

func (uppercaseSerdes) Unmarshal(_ durable.SerdesContext, data []byte, v any) error {
	// The stored value is raw uppercase text (not JSON-encoded).
	if sp, ok := v.(*string); ok {
		*sp = string(data)
		return nil
	}
	return json.Unmarshal(data, v)
}

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "serdes-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
	}, durable.WithChildSerdes(uppercaseSerdes{}))
}

func main() {
	durable.Start(handler)
}

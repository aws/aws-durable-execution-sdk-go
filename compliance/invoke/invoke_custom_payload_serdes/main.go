// Command invoke_custom_payload_serdes implements conformance requirement
// 5-15: a custom payload serdes uppercases string payloads before they are
// sent to the target.
package main

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercasePayloadSerdes uppercases string values on serialization;
// deserialization is standard JSON.
type uppercasePayloadSerdes struct{}

func (uppercasePayloadSerdes) Marshal(v any) ([]byte, error) {
	if s, ok := v.(string); ok {
		return json.Marshal(strings.ToUpper(s))
	}
	return json.Marshal(v)
}

func (uppercasePayloadSerdes) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func handler(ctx durable.Context, event any) (any, error) {
	return durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event,
		durable.WithInvokePayloadSerdes(uppercasePayloadSerdes{}))
}

func main() {
	durable.Start(handler)
}

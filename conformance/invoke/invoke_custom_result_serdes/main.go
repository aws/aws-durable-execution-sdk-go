// Command invoke_custom_result_serdes implements conformance requirement
// 5-16: a custom result serdes uppercases the invoke result on
// deserialization.
package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseResultSerdes uppercases the raw serialized result text on
// deserialization, mirroring the other languages' conformance handlers;
// serialization is standard JSON.
type uppercaseResultSerdes struct{}

func (uppercaseResultSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (uppercaseResultSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	s, ok := v.(*string)
	if !ok {
		return json.Unmarshal(data, v)
	}
	*s = strings.ToUpper(string(data))
	return nil
}

func handler(ctx durable.Context, event string) (string, error) {
	return durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event,
		durable.WithInvokeResultSerdes(uppercaseResultSerdes{}))
}

func main() {
	durable.Start(handler)
}

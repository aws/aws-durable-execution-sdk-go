// Command map_custom_serde implements conformance requirement 9-14: Map
// configured with a custom per-item serializer round-trips each iteration result.
package main

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// wrapSerdes wraps on serialize, unwraps on deserialize (real, non-identity).
type wrapSerdes struct{}

func (wrapSerdes) Marshal(v any) ([]byte, error) {
	s := v.(string)
	return []byte("wrapped:" + s), nil
}

func (wrapSerdes) Unmarshal(data []byte, v any) error {
	s := string(data)
	unwrapped := strings.TrimPrefix(s, "wrapped:")
	ptr := v.(*string)
	*ptr = unwrapped
	return nil
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"x", "y"}
	result, err := durable.Map(ctx, "serdes", items, func(_ durable.Context, item string, _ int) (string, error) {
		return strings.ToUpper(item), nil
	}, durable.WithMaxConcurrency(1), durable.WithBatchSerdes(wrapSerdes{}))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}

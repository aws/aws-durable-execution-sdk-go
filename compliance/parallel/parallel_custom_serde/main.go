// Command parallel_custom_serde implements conformance requirement 8-15.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type wrappedSerdes struct{}
type wrapped struct {
	Wrapped string `json:"wrapped"`
}

func (wrappedSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T, want string", v)
	}
	return json.Marshal(wrapped{Wrapped: s})
}
func (wrappedSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	var w wrapped
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	ptr, ok := v.(*string)
	if !ok {
		return fmt.Errorf("unexpected type %T, want *string", v)
	}
	*ptr = w.Wrapped
	return nil
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "serde", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "x", nil }},
		{Func: func(_ durable.Context) (string, error) { return "y", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithBatchSerdes(wrappedSerdes{}))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }

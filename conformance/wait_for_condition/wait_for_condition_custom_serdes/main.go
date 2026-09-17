// Command wait_for_condition_custom_serdes implements conformance
// requirement 6-11: custom serializer/deserializer for state round-trip.
package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// prefixSerdes adds an "ENC:" prefix on serialize and strips it on
// deserialize, exercising the custom serdes round-trip.
type prefixSerdes struct{}

func (s *prefixSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	str, ok := v.(string)
	if !ok {
		return json.Marshal(v)
	}
	return json.Marshal("ENC:" + str)
}

func (s *prefixSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return json.Unmarshal(data, v)
	}
	decoded := strings.TrimPrefix(raw, "ENC:")
	ptr, ok := v.(*string)
	if !ok {
		return json.Unmarshal(data, v)
	}
	*ptr = decoded
	return nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.WaitForCondition(ctx, "", func(_ durable.StepContext, state string) (string, error) {
		return state + "x", nil
	}, durable.ConditionConfig[string]{
		InitialState: "",
		WaitStrategy: func(state string, _ int) durable.WaitDecision {
			if len(state) >= 2 {
				return durable.WaitDecision{Continue: false}
			}
			return durable.WaitDecision{Continue: true, Delay: time.Second}
		},
		Serdes: &prefixSerdes{},
	})
}

func main() {
	durable.Start(handler)
}

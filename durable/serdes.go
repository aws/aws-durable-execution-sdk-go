package durable

import (
	"context"
	"encoding/json"
)

// jsonSerdes is the default Serdes, backed by encoding/json.
type jsonSerdes struct{}

var _ Serdes = jsonSerdes{}

func (jsonSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

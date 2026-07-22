package durable

import "encoding/json"

// jsonSerdes is the default Serdes, backed by encoding/json.
type jsonSerdes struct{}

var _ Serdes = jsonSerdes{}

func (jsonSerdes) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonSerdes) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

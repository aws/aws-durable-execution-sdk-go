// Package utils provides default implementations used across the SDK:
// JSON-based serialization, a basic logger, and retry-strategy presets.
package utils

import (
	"encoding/json"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// JSONSerdes is the SDK's default Serdes implementation, serializing
// checkpointed values as JSON. entityID and executionARN are accepted to
// satisfy the types.Serdes interface (useful for alternate implementations
// that key external storage by them) but are unused here.
type JSONSerdes struct{}

func (JSONSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (JSONSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var v any
	if err := json.Unmarshal([]byte(pointer), &v); err != nil {
		return nil, err
	}
	return v, nil
}

// DefaultSerdes returns the SDK's default Serdes (JSON-based).
func DefaultSerdes() types.Serdes {
	return JSONSerdes{}
}

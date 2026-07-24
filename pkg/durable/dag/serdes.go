package dag

import "encoding/json"

// unmarshalResult decodes a JSON-encoded task result into T. Used on the
// replay/deserialization path by Result[T]. See DAG_SPEC_GO.md §8.
func unmarshalResult[T any](raw []byte) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, err
	}
	return v, nil
}

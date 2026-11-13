package telemetry

import (
	"bytes"
	"encoding/json"
)

// marshalNoHTMLEscape marshals v to JSON with HTML escaping disabled, so
// <, > and & stay literal. It matches json.Marshal in every other way:
// map keys are sorted and a nil value encodes as null. json.Encoder.Encode
// appends one trailing newline, which this function removes.
//
// The durable package stores checkpoints with the same setting, so a
// record carries a result with the characters the checkpoint has.
//
// Every place that serializes a record or one of its content values must
// call this function instead of json.Marshal. json.Marshal escapes the
// bytes a json.Marshaler returns, and Record, OperationRecord and
// json.RawMessage are all json.Marshalers. So one json.Marshal call on the
// way out would escape the characters that an earlier call kept literal.
func marshalNoHTMLEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

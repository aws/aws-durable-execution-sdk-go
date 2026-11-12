package telemetry

import "encoding/json"

// ContentConfig controls what content is included in emitted records and
// how it is formatted.
//
// Content values (Record.Input, Record.Output, OperationRecord.Result) are
// JSON values. Input and output are marshaled once from the handler's typed
// values, so a Go string becomes a JSON string. An operation result is the
// checkpointed wire text: it is used as-is when it is valid JSON and
// encoded as a JSON string otherwise. There is no per-field length cap. An
// exporter that implements SizeLimiter bounds the whole record instead
// (see Truncate).
type ContentConfig struct {
	// IncludeInput controls whether execution input is included in
	// records. Defaults to true.
	IncludeInput bool

	// IncludeOutput controls whether execution output is included in
	// records. Defaults to true.
	IncludeOutput bool

	// IncludeOperationResults controls whether operation result fields
	// are included. Defaults to false.
	IncludeOperationResults bool

	// IncludeOperationErrors controls whether operation error fields
	// are included. Defaults to true.
	IncludeOperationErrors bool

	// IncludeExecutionError controls whether the execution-level error
	// is included. Defaults to true.
	IncludeExecutionError bool

	// OperationFilter, if non-nil, is called for each operation. Only
	// operations for which it returns true are included in the record.
	// Different invocations may call it concurrently. The function and
	// its captured state must be safe for concurrent use.
	OperationFilter func(OperationRecord) bool

	// Redactor, if non-nil, is applied to the text of every content value
	// (input, output, and operation results) before it is stored. Use this
	// to mask sensitive data. For input and output the text is the
	// marshaled JSON. For an operation result it is the checkpointed wire
	// text. If the returned text is valid JSON it is emitted as that JSON
	// value; otherwise it is emitted as a JSON string. If it is empty, the
	// field is omitted. Redactor is not called for content that the
	// Include* fields exclude.
	// Different invocations may call it concurrently. The function and
	// its captured state must be safe for concurrent use.
	Redactor func(string) string
}

// DefaultContentConfig returns the default configuration: input, output,
// operation errors, and the execution error are included; operation
// results are not.
func DefaultContentConfig() ContentConfig {
	return ContentConfig{
		IncludeInput:            true,
		IncludeOutput:           true,
		IncludeOperationResults: false,
		IncludeOperationErrors:  true,
		IncludeExecutionError:   true,
	}
}

// contentValue turns content text into the JSON value stored on a record.
// It applies Redactor when set. Valid JSON text is stored as-is. Any other
// text is stored as a JSON string, so the record always marshals. Empty
// text yields nil, so omitempty drops the key.
//
// The operation result path relies on the fallback: the wire text is
// normally JSON, but a custom serializer may produce something else, and
// the JS exporter likewise falls back to the raw string.
func (c ContentConfig) contentValue(text string) json.RawMessage {
	if text == "" {
		return nil
	}
	if c.Redactor != nil {
		text = c.Redactor(text)
		if text == "" {
			return nil
		}
	}
	if json.Valid([]byte(text)) {
		return json.RawMessage(text)
	}
	quoted, err := json.Marshal(text)
	if err != nil {
		// json.Marshal of a string only fails on an invalid encoder
		// state, which cannot happen here. Omit rather than emit an
		// unmarshalable record.
		return nil
	}
	return quoted
}

// marshalContent marshals a typed handler value once and passes the JSON
// text through contentValue. A nil value or a marshal failure yields nil,
// so the key is omitted.
func (c ContentConfig) marshalContent(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return c.contentValue(string(b))
}

// ApplyToRecord filters and transforms a record's content fields in place
// based on the configuration.
func (c ContentConfig) ApplyToRecord(r *Record) {
	if !c.IncludeInput {
		r.Input = nil
	}
	if !c.IncludeOutput {
		r.Output = nil
	}
	if !c.IncludeExecutionError {
		r.Error = nil
	}

	if c.OperationFilter != nil || !c.IncludeOperationResults || !c.IncludeOperationErrors {
		filtered := r.Operations[:0]
		for _, op := range r.Operations {
			if c.OperationFilter != nil && !c.OperationFilter(op) {
				continue
			}
			if !c.IncludeOperationResults {
				op.Result = nil
			}
			if !c.IncludeOperationErrors {
				op.Error = nil
			}
			filtered = append(filtered, op)
		}
		r.Operations = filtered
	}
}

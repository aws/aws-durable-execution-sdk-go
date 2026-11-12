package telemetry

import "encoding/json"

// ContentConfig controls what content appears in emitted records. The
// zero value includes the execution input and output unchanged, omits
// operation results, and includes operation and execution errors.
//
// Content values (Record.Input, Record.Output, OperationRecord.Result) are
// JSON values. There is no per-field length cap. An exporter that
// implements SizeLimiter bounds the whole record instead (see Truncate).
//
// A transform function receives the content decoded as a JSON value: a
// map[string]any, []any, string, float64, bool, or nil. It returns the
// value to record, which is marshaled again. If a transform panics, or its
// return value cannot be marshaled, the field is omitted. Transforms may
// be called concurrently by different invocations. Each function and its
// captured state must be safe for concurrent use.
type ContentConfig struct {
	// ExcludeInput omits the execution input from records.
	ExcludeInput bool

	// InputTransform, if non-nil, receives the execution input decoded as
	// a JSON value and returns the value to record. Use it to remove or
	// mask sensitive fields. It is not called when ExcludeInput is true.
	InputTransform func(any) any

	// ExcludeOutput omits the execution output from records.
	ExcludeOutput bool

	// OutputTransform, if non-nil, receives the execution output decoded
	// as a JSON value and returns the value to record. It is not called
	// when ExcludeOutput is true.
	OutputTransform func(any) any

	// Operations controls per-operation content.
	Operations ContentOperations
}

// ContentOperations controls the content of operation entries.
type ContentOperations struct {
	// Overrides are matched to operations by OperationName. When several
	// entries share a name, the last one wins.
	Overrides []OperationOverride

	// ExcludeErrors omits the error field from every operation entry. The
	// execution-level error on the record is always included.
	ExcludeErrors bool
}

// OperationOverride changes the content of every operation with the
// given name.
type OperationOverride struct {
	// OperationName selects the operations this override applies to.
	OperationName string

	// Exclude drops the operation from the record.
	Exclude bool

	// Result opts the operation's result into the record. Results are
	// omitted unless an override sets this. The function receives the
	// checkpointed result decoded as a JSON value, or the raw text as a
	// string when that text is not valid JSON. The plugin does not run
	// the operation's own serializer, so an operation with a custom
	// Serdes delivers its serialized form here. To record a result
	// unchanged, return the argument.
	Result func(any) any
}

// overrideFor returns the last override whose OperationName equals name,
// or nil when there is none.
func (c ContentOperations) overrideFor(name string) *OperationOverride {
	for i := len(c.Overrides) - 1; i >= 0; i-- {
		if c.Overrides[i].OperationName == name {
			return &c.Overrides[i]
		}
	}
	return nil
}

// inputValue returns the JSON value to record for the execution input,
// or nil when the input is excluded, absent, or cannot be recorded.
func (c ContentConfig) inputValue(v any) json.RawMessage {
	if c.ExcludeInput {
		return nil
	}
	return typedValue(v, c.InputTransform)
}

// outputValue returns the JSON value to record for the execution output,
// or nil when the output is excluded, absent, or cannot be recorded.
func (c ContentConfig) outputValue(v any) json.RawMessage {
	if c.ExcludeOutput {
		return nil
	}
	return typedValue(v, c.OutputTransform)
}

// resultValue returns the JSON value to record for an operation result.
// text is the checkpointed wire text. The result is omitted (nil) when no
// override opts it in or when the text is empty.
func (c ContentConfig) resultValue(name, text string) json.RawMessage {
	o := c.Operations.overrideFor(name)
	if o == nil || o.Result == nil || text == "" {
		return nil
	}
	var v any
	if json.Valid([]byte(text)) {
		if err := json.Unmarshal([]byte(text), &v); err != nil {
			return nil
		}
	} else {
		v = text
	}
	return transformValue(v, o.Result)
}

// applyOperations drops excluded operations from r and strips operation
// errors when configured. It writes to a new slice, so the caller's
// backing array is never modified.
func (c ContentConfig) applyOperations(r *Record) {
	if len(c.Operations.Overrides) == 0 && !c.Operations.ExcludeErrors {
		return
	}
	kept := make([]OperationRecord, 0, len(r.Operations))
	for _, op := range r.Operations {
		if o := c.Operations.overrideFor(op.Name); o != nil && o.Exclude {
			continue
		}
		if c.Operations.ExcludeErrors {
			op.Error = nil
		}
		kept = append(kept, op)
	}
	r.Operations = kept
}

// typedValue marshals a handler's typed value and applies fn to it. A nil
// value, a marshal failure, or a failed transform yields nil, so the key
// is omitted. The value is marshaled once before the transform so that
// fn sees the JSON form, not the Go type: a Go string arrives as a string
// and a struct arrives as a map[string]any.
func typedValue(v any, fn func(any) any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	if fn == nil {
		return b
	}
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		return nil
	}
	return transformValue(decoded, fn)
}

// transformValue calls fn on v and marshals the return value. A panic in
// fn or a marshal failure yields nil, so the field is omitted rather than
// recorded unredacted.
func transformValue(v any, fn func(any) any) json.RawMessage {
	out, ok := callTransform(fn, v)
	if !ok {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return b
}

// callTransform runs fn and reports false if it panics.
func callTransform(fn func(any) any, v any) (out any, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			out, ok = nil, false
		}
	}()
	return fn(v), true
}

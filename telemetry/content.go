package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// ContentConfig controls what content appears in emitted records. The
// zero value includes the execution input and output unchanged, omits
// operation results, and includes operation and execution errors.
//
// Content values (Record.Input, Record.Output, OperationRecord.Result) are
// JSON values. There is no per-field length cap. An exporter that
// implements SizeLimiter bounds the whole record instead (see Truncate).
//
// A transform function receives the content decoded as a JSON value: a
// map[string]any, []any, string, json.Number, bool, or nil. Numbers are
// json.Number rather than float64 so that integers above 2^53 keep their
// exact digits through the transform; call Int64 or Float64 on the value
// to do arithmetic. The transform returns the value to record, which is
// marshaled again. Marshaling writes no whitespace and sorts the keys of
// every object, so the recorded text can differ from the original text in
// key order. The characters <, > and & are written literally, not as
// \u003c, \u003e and \u0026. If a transform panics, or its return value
// cannot be marshaled, the field is omitted. Transforms may be called
// concurrently by different invocations. Each function and its captured
// state must be safe for concurrent use.
//
// Transforms run only for sampled-in invocations; see Config.SamplingRate.
// InputTransform and OutputTransform run at most once per invocation. A
// Result transform runs once for an operation's result in an invocation,
// and again only if that result text changes. Each new invocation replays
// checkpointed results, so it calls the Result transform again. Transforms
// run while the plugin holds its per-invocation lock, which delays that
// invocation's other hooks. So a transform should return quickly and must
// not block.
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
	// unchanged, return the argument. It is not called when Exclude is true.
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
// override opts it in, the operation is excluded, or the text is empty.
func (c ContentConfig) resultValue(name, text string) json.RawMessage {
	o := c.Operations.overrideFor(name)
	if o == nil || o.Exclude || o.Result == nil || text == "" {
		return nil
	}
	v, err := decodeJSON([]byte(text))
	if err != nil {
		// Not valid JSON: the transform receives the raw text as a string.
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

// decodeJSON decodes b into a generic JSON value. Numbers decode as
// json.Number, not float64. A float64 holds 53 bits of mantissa, so an
// int64 above 2^53 would be rounded by the decode alone, and a transform
// that returns its argument unchanged would still record an altered
// number. json.Number keeps the original digits and the encoder writes
// them back verbatim. Decode reads one value and stops, so trailing bytes
// would pass unnoticed and "1 2" would decode as 1. The Token call after
// Decode must therefore reach io.EOF; anything else is an error. So the
// function accepts exactly the inputs json.Valid accepts, and callers can
// pass arbitrary text without a separate validity check.
func decodeJSON(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errTrailingData
	}
	return v, nil
}

// errTrailingData reports bytes after the first JSON value in decodeJSON.
var errTrailingData = errors.New("telemetry: trailing data after JSON value")

// typedValue marshals a handler's typed value and applies fn to it. An
// untyped nil interface value, a marshal failure, or a failed transform
// yields nil, so the key is omitted. A typed nil (a nil pointer, map, or
// slice) is not caught by the nil check: it marshals to the JSON text
// null and is recorded as null. That matches the checkpointed output the
// SDK stores for the same handler return value. The value is marshaled
// once before the transform so that fn sees the JSON form, not the Go
// type: a Go string arrives as a string and a struct arrives as a
// map[string]any.
func typedValue(v any, fn func(any) any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := marshalNoHTMLEscape(v)
	if err != nil {
		return nil
	}
	if fn == nil {
		return b
	}
	decoded, err := decodeJSON(b)
	if err != nil {
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
	b, err := marshalNoHTMLEscape(out)
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

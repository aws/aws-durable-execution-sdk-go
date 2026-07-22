package insight

import "encoding/json"

// ValueTransform transforms a value before it is included in an emitted
// record, matching the JS SDK's own content.input/content.output
// transform-function form. Applied to the RAW Go value (the same value
// plugin.InvocationInfo.ExecutionInput/InvocationEndInfo.ExecutionResult
// carry - this SDK's own handler-level event/result type, not a
// re-serialized/re-parsed copy) when used for ContentConfig.Input/
// Output specifically. OperationOverride.Result (below) uses this SAME
// function type, but is applied to a DIFFERENT kind of value - the
// operation's own CHECKPOINTED, already-serialized result (JSON-decoded
// when possible, else the raw string - see OperationRecord.Result's own
// doc), matching the JS SDK's own explicitly documented distinction
// between these two cases exactly.
type ValueTransform func(value any) any

// ContentConfig controls what data is included in emitted records,
// matching a subset of the JS SDK's own content option - see this
// type's own field docs for exactly what this Go port implements versus
// what remains genuinely unimplemented (result-related pieces - see
// OperationOverride's own doc).
type ContentConfig struct {
	// Input controls the execution input field. One of:
	//   - nil (the zero value): include as-is - the JS SDK's own
	//     documented default.
	//   - IncludeAsIs / ExcludeContent (see those vars' own doc): an
	//     explicit include-as-is or exclude, without a transform.
	//   - Any other ValueTransform: applied to the raw input value; its
	//     return value is what's included (a nil return excludes the
	//     field, same as ExcludeContent).
	Input ValueTransform

	// Output controls the execution output (result) field - see Input's
	// own doc for the exact same three-way behavior, applied to the
	// handler's own returned result instead.
	Output ValueTransform

	// Operations controls per-operation content - see OperationsConfig's
	// own doc.
	Operations OperationsConfig
}

// OperationsConfig controls per-operation content within Content,
// matching the JS SDK's own content.operations option.
type OperationsConfig struct {
	// IncludeErrors controls whether a failed operation's own Error
	// field is populated. Defaults to true (included) when this
	// ContentConfig is the zero value - see ShouldIncludeErrors's own
	// doc for the Go zero-value-collision handling this needs, matching
	// SamplingRate/SortKey's own established precedent elsewhere in
	// this package.
	IncludeErrors *bool

	// Overrides are matched by OperationName, applied in order (a LATER
	// entry for the same name wins, matching the JS SDK's own documented
	// "last matching entry wins" rule). See OperationOverride's own doc
	// for what this Go port does and does not implement.
	Overrides []OperationOverride
}

// OperationOverride is a single content.operations.overrides entry,
// matching the JS SDK's own shape.
type OperationOverride struct {
	// OperationName matches OperationRecord.Name exactly.
	OperationName string

	// Exclude, if true, drops this operation from the emitted record's
	// own Operations array entirely.
	Exclude bool

	// Result, if non-nil, OPTS IN this operation's own checkpointed
	// result (see OperationRecord.Result's own doc for exactly what
	// value this receives, and its real, JS-SDK-inherited caveat around
	// custom Serdes) and applies this transform to it - matching the JS
	// SDK's own documented "opt specific operation results in (with
	// optional transforms)" behavior exactly: operation results are
	// EXCLUDED by default (Result stays nil on the emitted
	// OperationRecord) unless a matching override explicitly requests
	// them via this field. Set to IncludeAsIs to opt in WITHOUT any
	// further transformation.
	Result ValueTransform
}

// ShouldIncludeErrors reports whether operation-level errors should be
// included, applying OperationsConfig.IncludeErrors's own documented
// default (true) when left nil - a plain bool field could not
// distinguish "caller left this unset, use the default" from "caller
// explicitly set IncludeErrors: false", the same zero-value-collision
// class of problem SortKey's own *string design solves elsewhere in
// this package (see that field's own doc for the identical reasoning).
func (c OperationsConfig) ShouldIncludeErrors() bool {
	if c.IncludeErrors == nil {
		return true
	}
	return *c.IncludeErrors
}

// applyOverride finds the LAST matching OperationOverride for name
// (matching the JS SDK's own "last matching entry wins" rule for
// duplicate/overlapping override entries), or reports found=false if no
// override matches name at all.
func applyOverride(overrides []OperationOverride, name string) (ov OperationOverride, found bool) {
	for _, o := range overrides {
		if o.OperationName == name {
			ov = o
			found = true
		}
	}
	return ov, found
}

// applyContent applies cfg to record IN PLACE - called on the shallow
// copy OnInvocationEnd already builds for dispatch (see that method's
// own doc), never on Plugin's own internal p.record, matching
// OperationDetail's own established "filter the emitted copy, keep the
// internal bookkeeping copy whole" precedent.
func applyContent(record *WorkflowInsightRecord, cfg ContentConfig) {
	record.Input = applyValueTransform(cfg.Input, record.Input)
	record.Output = applyValueTransform(cfg.Output, record.Output)

	if !cfg.Operations.ShouldIncludeErrors() {
		for i := range record.Operations {
			record.Operations[i].Error = nil
		}
	}

	if len(cfg.Operations.Overrides) == 0 {
		return
	}
	filtered := make([]OperationRecord, 0, len(record.Operations))
	for _, op := range record.Operations {
		ov, found := applyOverride(cfg.Operations.Overrides, op.Name)
		if found && ov.Exclude {
			continue
		}
		if found && ov.Result != nil {
			op.Result = applyValueTransform(ov.Result, decodeRawResult(op.rawResult))
		}
		filtered = append(filtered, op)
	}
	record.Operations = filtered
}

// decodeRawResult decodes raw (an operation's own checkpointed result,
// in its still-serialized wire form - see OperationRecord.rawResult's
// own doc) into the value an OperationOverride.Result transform
// actually receives, matching the JS SDK's own documented decoding rule
// exactly: JSON-parsed when raw is valid JSON, otherwise the raw string
// itself unchanged. An empty raw (no checkpointed result at all, e.g. an
// operation that never reached a terminal SUCCEEDED status) decodes to
// nil.
func decodeRawResult(raw string) any {
	if raw == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return raw
	}
	return decoded
}

// applyValueTransform applies transform to value, matching Input/
// Output's own documented three-way behavior: a nil transform (the
// ContentConfig zero value) returns value unchanged (include as-is);
// any non-nil transform is called and its own return value (which may
// itself be nil, excluding the field - encoding/json's own omitempty
// tag on WorkflowInsightRecord.Input/Output already omits a nil value
// from the emitted JSON) is used.
func applyValueTransform(transform ValueTransform, value any) any {
	if transform == nil {
		return value
	}
	return transform(value)
}

// ExcludeContent is a ValueTransform that always excludes the field
// (returns nil), for use as ContentConfig.Input/Output when a caller
// wants to explicitly drop the field entirely rather than write
// `func(any) any { return nil }` inline - matching the JS SDK's own
// `input: false` shorthand's intent (Go has no boolean-or-function union
// type to accept a literal `false` the way TypeScript's own `boolean |
// ((v: T) => unknown)` union does, so this named function is the
// closest equivalent).
func ExcludeContent(any) any { return nil }

// IncludeAsIs is a ValueTransform that returns value unchanged, for use
// as ContentConfig.Input/Output when a caller wants to be EXPLICIT about
// including the field as-is (equivalent to leaving Input/Output nil,
// which already does this - provided for symmetry with ExcludeContent
// and for callers who prefer an explicit value over relying on Go's
// zero-value default).
func IncludeAsIs(value any) any { return value }

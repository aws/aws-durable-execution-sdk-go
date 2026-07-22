package insight

import "encoding/json"

// Per-exporter default MaxRecordSizeBytes values, matching the JS SDK's
// own documented defaults exactly (each sitting comfortably under its
// destination's own real hard limit, to leave headroom for the
// destination's own wire envelope - DynamoDB type descriptors,
// CloudWatch Logs event framing, gzip, etc. - which this byte count does
// NOT itself model, matching the JS SDK's own documented caveat on this
// point).
const (
	DefaultMaxRecordSizeBytesLog        = 256_000 // Lambda log, CloudWatch Logs, SQS, EventBridge
	DefaultMaxRecordSizeBytesDynamoDB   = 400_000
	DefaultMaxRecordSizeBytesSQL        = 1_000_000 // Aurora, Redshift, Firehose, OTel
	DefaultMaxRecordSizeBytesS3         = 5_000_000
	DefaultMaxRecordSizeBytesOpenSearch = 10_000_000
	// HTTP and File have NO default - truncation is disabled for them
	// unless the caller sets MaxRecordSizeBytes explicitly, matching the
	// JS SDK's own documented "None¹ ... truncation is disabled unless
	// you set maxRecordSizeBytes" behavior exactly. Deliberately no
	// named constant here (a "default of disabled" is just the zero
	// value already).
)

// Renderer produces the exact byte shape a call to Truncate should size
// its fits-or-doesn't-fit check against, matching the JS SDK's own
// documented "exporters that reshape operations... expose a `render` the
// limiter sizes against, so a record trimmed to its limit reflects what
// is actually serialized" behavior - an exporter using
// OperationsFormatByName/OperationsFormatBoth (see that type's own doc)
// produces a DIFFERENT-sized JSON body than the canonical array shape
// (renderRecord's own "operations" vs "operationsByName" keys are not
// the same size), so sizing against the WRONG shape could under- or
// over-truncate relative to what the exporter actually sends.
type Renderer func(WorkflowInsightRecord) ([]byte, error)

// defaultRenderer is Renderer's own zero-value behavior: the canonical
// WorkflowInsightRecord JSON shape, via plain json.Marshal - used by
// Truncate whenever no exporter-specific Renderer is supplied (every
// exporter that has no OperationsFormat option at all - LambdaLog,
// CloudWatchLogs, S3, DynamoDB, Aurora, Redshift, OpenSearch - always
// renders this same canonical shape, so this is the correct sizing
// function for all of them).
func defaultRenderer(record WorkflowInsightRecord) ([]byte, error) {
	return json.Marshal(record)
}

// Truncate returns a copy of record, best-effort truncated to fit within
// maxBytes when rendered via render (or the canonical JSON shape, if
// render is nil), following the JS SDK's own documented drop order
// exactly:
//
//  1. operation result fields, oldest operation first (see
//     OperationRecord.Result's own doc - this only ever has anything to
//     drop for an operation whose own name matched an
//     OperationOverride.Result opt-in in the first place; most
//     operations, most of the time, have no Result set at all, so this
//     step is frequently a genuine no-op in PRACTICE, but is no longer a
//     no-op BY DESIGN the way it was before OperationRecord.Result
//     existed).
//  2. whole operations, oldest first (Operations[0] is dropped before
//     Operations[1], etc. - matching the JS SDK's own documented
//     "oldest first" order, since Operations is built and kept in the
//     order operations were first observed - see
//     Plugin.upsertOperationLocked's own append-in-first-seen-order
//     behavior).
//  3. as a last resort (once every operation is gone), execution Input,
//     then Output.
//
// Identity/timeline fields (ExecutionARN, FunctionName, Status,
// StartTime, EndTime, DurationMs, ...) are NEVER dropped, matching the
// JS SDK's own documented guarantee - if the record still doesn't fit
// after dropping every operation and both Input/Output, Truncate gives
// up and returns the record with everything already dropped (matching
// the JS SDK's own "best-effort" framing: there is no smaller
// representation left to try).
//
// When maxBytes <= 0, Truncate returns record completely unchanged (no
// size check performed at all, not even a single render call) -
// matching SizeLimiter's own documented "truncation is disabled"
// contract for an exporter with no configured limit.
func Truncate(record WorkflowInsightRecord, maxBytes int, render Renderer) WorkflowInsightRecord {
	if maxBytes <= 0 {
		return record
	}
	if render == nil {
		render = defaultRenderer
	}
	if fits(record, maxBytes, render) {
		return record
	}

	// Step 1: drop each operation's own Result field, oldest first,
	// until it fits or every operation's Result is already gone. Only
	// operations that actually HAVE a Result set (see this function's
	// own doc) are touched - an operation with no Result at all (the
	// common case) is left completely alone by this step.
	for i := range record.Operations {
		if record.Operations[i].Result == nil {
			continue
		}
		record.Operations[i].Result = nil
		record.Operations[i].Truncated = true
		record.Truncated = true
		if fits(record, maxBytes, render) {
			return record
		}
	}

	// Step 2: drop whole operations, oldest first, until it fits or none
	// remain.
	dropped := 0
	for len(record.Operations) > 0 && !fits(record, maxBytes, render) {
		record.Operations = record.Operations[1:]
		dropped++
	}
	if dropped > 0 {
		record.Truncated = true
		record.DroppedOperations = dropped
	}
	if fits(record, maxBytes, render) {
		return record
	}

	// Step 3: Input, then Output, as a last resort.
	if record.Input != nil {
		record.Input = nil
		record.Truncated = true
		record.DroppedInput = true
		if fits(record, maxBytes, render) {
			return record
		}
	}
	if record.Output != nil {
		record.Output = nil
		record.Truncated = true
		record.DroppedOutput = true
	}
	return record
}

// fits reports whether record's own rendered size (via render) is within
// maxBytes. A render error is treated as "does not fit" (safer than
// silently treating an unrenderable record as fitting) - in practice
// WorkflowInsightRecord's own field types (strings, times, bools,
// slices of OperationRecord, and an `any` Input/Output that is whatever
// JSON-compatible value the handler's own event/result already is) are
// always marshalable, so this path is defensive, not expected to
// actually trigger.
func fits(record WorkflowInsightRecord, maxBytes int, render Renderer) bool {
	b, err := render(record)
	if err != nil {
		return false
	}
	return len(b) <= maxBytes
}

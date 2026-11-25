package telemetry

import "slices"

// Renderer produces the exact byte shape a Truncate call should size
// against. Exporters with custom rendering (OperationsFormat) provide
// their own Renderer so truncation sizes against what is actually sent.
type Renderer func(Record) ([]byte, error)

// defaultRenderer is the canonical JSON shape. See marshalNoHTMLEscape.
func defaultRenderer(record Record) ([]byte, error) {
	return marshalNoHTMLEscape(record)
}

// Truncate returns a copy of record, best-effort truncated to fit within
// maxBytes when rendered via render (or canonical JSON if render is nil).
// Drop order: operation results (oldest first), whole operations (oldest
// first), then Input, then Output. Identity/timeline fields are never
// dropped.
//
// When maxBytes <= 0, returns record unchanged (truncation disabled).
func Truncate(record Record, maxBytes int, render Renderer) Record {
	if render == nil {
		// The canonical JSON holds one copy of each operation. So the
		// meter's bound holds for it.
		return truncate(record, maxBytes, defaultRenderer, 1)
	}
	return truncate(record, maxBytes, render, 0)
}

// meteredCopies returns how many copies of each operation exp's Render
// output holds, or 0 when exp is not one of this package's exporters.
//
// The meter in truncate skips a size check only when a bound on the
// rendered size says the check cannot succeed. The bound holds only for a
// rendering this package defines. So the switch lists exact types. A type
// that embeds one of them can define its own Render, and it is a different
// type, so it gets 0 and the exact path.
func meteredCopies(exp RenderingSizeLimiter) int {
	format := OperationsFormatByName
	switch e := exp.(type) {
	case *LambdaLogExporter, *CloudWatchLogsExporter:
	case *EventBridgeExporter:
		format = e.OperationsFormat
	case *FirehoseExporter:
		format = e.OperationsFormat
	case *SQSExporter:
		format = e.OperationsFormat
	default:
		return 0
	}
	if format == OperationsFormatBoth {
		return 2 // the operations array and operationsByName
	}
	return 1
}

// shrinkSlackBytes is added to each operation's bound. It covers the
// operationsByName fields that are not in the operation's own JSON (count,
// failedCount and the duration aggregates) and the separators.
const shrinkSlackBytes = 1024

// truncate implements Truncate. It drops content one step at a time and
// returns the first record that fits:
//
//  1. Results, oldest first. An operation without a Result changes
//     nothing, so no size check runs before it.
//  2. Whole operations, oldest first.
//  3. Input, then Output.
//
// copies is the number of copies of each operation in render's output.
// When copies is 0, every step is measured. When copies > 0, a meter skips
// a check that cannot succeed (see meter). A size that fits always comes
// from a real render. So the result never exceeds maxBytes, and it is the
// same record that measuring every step returns.
func truncate(record Record, maxBytes int, render Renderer, copies int) Record {
	if maxBytes <= 0 {
		return record
	}
	m := meter{render: render, maxBytes: maxBytes, last: -1}
	if m.fits(record) {
		return record
	}
	ops := slices.Clone(record.Operations)
	record.Operations = ops
	var bounds []int
	if copies > 0 {
		bounds = shrinkBounds(ops, copies)
	}
	bound := func(i int) int {
		if bounds == nil {
			return -1
		}
		return bounds[i]
	}

	// The content fields are json.RawMessage with omitempty. An empty
	// non-nil RawMessage is never rendered, so dropping it frees no bytes.
	// So every presence check below is len > 0, not != nil; otherwise an
	// empty value would set Truncated and the Dropped* flags for content
	// that was never in the output.

	// Step 1: drop each operation's Result field, oldest first.
	for i := range ops {
		if len(ops[i].Result) == 0 {
			continue
		}
		ops[i].Result = nil
		ops[i].Truncated = true
		record.Truncated = true
		m.shrunkBy(bound(i))
		if m.fits(record) {
			return record
		}
	}

	// Step 2: drop whole operations, oldest first. Each candidate carries
	// the final truncation metadata, so its size includes those bytes.
	for k := 1; k <= len(ops); k++ {
		candidate := record
		candidate.Operations = ops[k:]
		candidate.Truncated = true
		candidate.DroppedOperations = k
		m.shrunkBy(bound(k - 1))
		if m.fits(candidate) {
			return candidate
		}
		if k == len(ops) {
			record = candidate // nothing fits; Step 3 runs on the empty list
		}
	}

	// Step 3: Input, then Output, as a last resort.
	if len(record.Input) > 0 {
		record.Input = nil
		record.Truncated = true
		record.DroppedInput = true
		if fits(record, maxBytes, render) {
			return record
		}
	}
	if len(record.Output) > 0 {
		record.Output = nil
		record.Truncated = true
		record.DroppedOutput = true
	}
	return record
}

// shrinkBounds returns, for each operation, an upper bound on how many
// bytes dropping its Result or the whole operation can remove from a
// rendering that holds copies copies of it. One copy is the operation's
// JSON, its name's JSON, and shrinkSlackBytes. An operation that does not
// marshal gets -1, which makes the meter measure after it is dropped.
func shrinkBounds(ops []OperationRecord, copies int) []int {
	bounds := make([]int, len(ops))
	for i, op := range ops {
		opJSON, err := marshalNoHTMLEscape(op)
		nameJSON, nameErr := marshalNoHTMLEscape(op.Name)
		if err != nil || nameErr != nil {
			bounds[i] = -1
			continue
		}
		bounds[i] = copies * (len(opJSON) + len(nameJSON) + shrinkSlackBytes)
	}
	return bounds
}

// meter runs the size checks of one truncate call.
//
// After a render, the meter keeps the rendered size. Each drop then adds
// its shrink bound to a running total. While the size minus the total is
// still over maxBytes, the record cannot fit. So fits returns false
// without a render. Otherwise fits renders. A wrong bound can only make
// truncate drop more than needed. It cannot make the result exceed
// maxBytes, because true always comes from a render.
type meter struct {
	render   Renderer
	maxBytes int
	last     int // size at the last render; -1 means render on the next check
	shrink   int // sum of the bounds since the last render
}

// shrunkBy adds bound to the running total. A negative bound is unknown,
// so the next check renders.
func (m *meter) shrunkBy(bound int) {
	if bound < 0 {
		m.last = -1
		return
	}
	m.shrink += bound
}

func (m *meter) fits(record Record) bool {
	if m.last >= 0 && m.last-m.shrink > m.maxBytes {
		return false
	}
	m.shrink = 0
	b, err := m.render(record)
	if err != nil {
		m.last = -1
		return false
	}
	m.last = len(b)
	return len(b) <= m.maxBytes
}

// fits reports whether record's rendered size is within maxBytes.
func fits(record Record, maxBytes int, render Renderer) bool {
	b, err := render(record)
	if err != nil {
		return false
	}
	return len(b) <= maxBytes
}

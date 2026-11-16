package telemetry

import "sort"

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
	if maxBytes <= 0 {
		return record
	}
	if render == nil {
		render = defaultRenderer
	}
	if fits(record, maxBytes, render) {
		return record
	}

	// Steps 1 and 2 drop content oldest-first. They choose how much to drop
	// in different ways.
	//
	// Step 1 clears operation results. It binary-searches for the smallest
	// number of results to clear, so it renders the record O(log n) times
	// instead of once per operation. A binary search is correct only when
	// clearing more results never makes the rendered record larger.
	//
	// The array rendering does not always meet that condition. Clearing a
	// Result also sets the operation's Truncated flag. The flag renders as
	// `,"truncated":true`, which is 17 bytes. The cleared Result frees 10
	// bytes for `,"result":` plus the length of its value. So clearing a
	// Result whose value is shorter than 7 bytes makes the operation up to
	// 6 bytes larger. Step 1 can then clear more results than the limit
	// required. The by-name rendering has no per-operation Truncated key,
	// so it always meets the condition.
	//
	// Step 2 removes whole operations. Removing an operation can make the
	// rendered record larger. The by-name rendering shows a name's Error
	// and Result only when the name occurs once (see GroupOperationsByName).
	// So removing one of two operations that share a name makes the other
	// operation's Error and Result appear. A binary search could then skip
	// a smaller number of removals that fits, and remove more operations
	// than needed. So Step 2 removes one operation at a time and stops at
	// the first number that fits. This costs one render per removed
	// operation.
	//
	// Each candidate is sized with the final truncation metadata already
	// set. So the returned record accounts for those extra bytes.

	ops := record.Operations
	n := len(ops)

	// The content fields are json.RawMessage with omitempty. An empty
	// non-nil RawMessage is never rendered, so dropping it frees no bytes.
	// So every presence check below is len > 0, not != nil; otherwise an
	// empty value would set Truncated and the Dropped* flags for content
	// that was never in the output.

	// Step 1: drop each operation's Result field, oldest first.
	// applyResultDrops returns a copy of ops with the oldest j operations'
	// Result cleared, and whether any Result was actually cleared.
	applyResultDrops := func(j int) (dropped []OperationRecord, any bool) {
		dropped = make([]OperationRecord, n)
		copy(dropped, ops)
		for i := 0; i < j; i++ {
			if len(dropped[i].Result) > 0 {
				dropped[i].Result = nil
				dropped[i].Truncated = true
				any = true
			}
		}
		return dropped, any
	}
	j := sort.Search(n+1, func(j int) bool {
		candidate := record
		cops, any := applyResultDrops(j)
		candidate.Operations = cops
		if any {
			candidate.Truncated = true
		}
		return fits(candidate, maxBytes, render)
	})
	if j > n {
		j = n // clearing every Result still doesn't fit; apply all, then Step 2.
	}
	if cops, any := applyResultDrops(j); any {
		record.Operations = cops
		record.Truncated = true
	} else {
		record.Operations = cops
	}
	if fits(record, maxBytes, render) {
		return record
	}

	// Step 2: drop whole operations, oldest first, one at a time. If even
	// dropping every operation does not fit, all are dropped and Step 3 runs.
	ops = record.Operations
	for k := 1; k <= len(ops); k++ {
		record.Operations = ops[k:]
		record.Truncated = true
		record.DroppedOperations = k
		if fits(record, maxBytes, render) {
			return record
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

// fits reports whether record's rendered size is within maxBytes.
func fits(record Record, maxBytes int, render Renderer) bool {
	b, err := render(record)
	if err != nil {
		return false
	}
	return len(b) <= maxBytes
}

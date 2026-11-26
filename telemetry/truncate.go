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

	// Steps 1 and 2 drop content oldest-first. Each step binary-searches
	// for how much to drop, so it renders the record O(log n) times instead
	// of once per result or operation. Step 1 searches the number of oldest
	// results to clear. Step 2 searches the number of oldest operations to
	// remove.
	//
	// A binary search returns the fewest drops only when each drop makes
	// the rendered record smaller. That is not always true, so a search can
	// drop more than the limit required:
	//   - The array rendering writes `"truncated":true` for an operation
	//     whose Result was cleared. So clearing a short Result, such as
	//     `true`, makes the record larger.
	//   - The by-name rendering shows a name's Error and Result only when
	//     the name occurs once (see GroupOperationsByName). So removing one
	//     of two operations that share a name makes the other operation's
	//     Error and Result appear.
	// This is an accepted trade-off: the code stays simple, and the cost
	// stays O(log n) renders for every record shape and every Renderer.
	//
	// Each candidate record is sized with the final truncation metadata
	// already set. A search returns a count below its upper end only after
	// it rendered that count and the count fit. So a wrong guess can only
	// drop extra content. The result fits whenever dropping everything
	// droppable fits.

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
	// When j > n, clearing every Result still doesn't fit; apply all,
	// then Step 2. Otherwise the search rendered j and it fit.
	cops, any := applyResultDrops(min(j, n))
	record.Operations = cops
	if any {
		record.Truncated = true
	}
	if j <= n {
		return record
	}

	// Step 2: drop whole operations, oldest first. If even dropping every
	// operation does not fit, all are dropped and Step 3 runs.
	ops = record.Operations
	if n > 0 {
		dropOldest := func(k int) Record {
			candidate := record
			candidate.Operations = ops[k:]
			candidate.Truncated = true
			candidate.DroppedOperations = k
			return candidate
		}
		fitsAfter := func(k int) bool {
			return fits(dropOldest(k), maxBytes, render)
		}

		k := 1 + sort.Search(n, func(i int) bool { return fitsAfter(i + 1) })
		if k <= n {
			return dropOldest(k)
		}
		record = dropOldest(n)
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

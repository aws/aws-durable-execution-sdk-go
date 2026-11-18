package telemetry

import (
	"slices"
	"sort"
)

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
	// operation's Error and Result appear. A count of removed operations at
	// which this can happen is a reveal count (see revealCounts).
	//
	// A segment is a range of counts that starts at a reveal count and ends
	// just before the next one. Inside a segment, one more removal does not
	// make the record larger. So the last count of a segment renders the
	// smallest record in that segment. A smaller count that fits can exist
	// only in an earlier segment. So a binary search alone can return a
	// count that is not the smallest. Step 2 therefore runs in three parts:
	//
	//  1. A binary search finds a count k that fits, where k-1 does not.
	//  2. Step 2 renders the last count of every segment below k. It keeps
	//     the lowest of these counts that fits.
	//  3. If one fits, a binary search inside its segment finds the first
	//     count that fits. If none fits, the result is k.
	//
	// The cost is two binary searches plus one render per reveal count
	// below k. A record with unique names has no reveal count. The worst
	// case is a record in which every name occurs exactly twice and the
	// newer operation has an Error. It has n/2 reveal counts. So for that
	// shape the total cost still grows quadratically.
	//
	// The result is the smallest count that fits, with one exception. A
	// removal inside a segment can add a few bytes. DroppedOperations gains
	// a digit at 10, 100, and so on. A name's minDurationMs can gain digits
	// when its shortest operation is removed. So when the record is within
	// a few bytes of the limit, Step 2 can remove more operations than
	// needed.
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

		// Part 1. sort.Search keeps the invariant that k-1 does not fit.
		k := 1 + sort.Search(n, func(i int) bool { return fitsAfter(i + 1) })
		if k <= n {
			// Part 2. The last count of a segment is one less than the next
			// reveal count. Count 0 was checked before Step 2, and k-1 was
			// checked by part 1. So only ends from 1 to k-2 are rendered.
			var ends []int
			for _, r := range revealCounts(ops) {
				if end := r - 1; end >= 1 && end <= k-2 {
					ends = append(ends, end)
				}
			}
			lowest := -1
			for i := len(ends) - 1; i >= 0; i-- {
				if fitsAfter(ends[i]) {
					lowest = i
				}
			}

			// Part 3. Every segment end below ends[lowest] does not fit. So
			// the first count that fits lies in ends[lowest]'s segment.
			if lowest >= 0 {
				lo := 1
				if lowest > 0 {
					lo = ends[lowest-1] + 1
				}
				hi := ends[lowest]
				k = lo + sort.Search(hi-lo, func(i int) bool { return fitsAfter(lo + i) })
			}
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

// revealCounts returns, in ascending order, every count of removed oldest
// operations at which the by-name rendering can grow.
//
// Removing the oldest r operations can remove the second-newest operation
// with some name. That name then occurs once. So GroupOperationsByName
// copies the newest operation's Error and Result into the name's summary.
// If that operation has an Error or a non-empty Result, the rendering can
// grow at r. So r is a reveal count. Each name has at most one reveal
// count. An operation without a name has none, because
// GroupOperationsByName excludes it.
//
// The scan runs newest to oldest. The second operation it sees with a
// name is that name's second-newest operation.
func revealCounts(ops []OperationRecord) []int {
	seen := make(map[string]int)
	newest := make(map[string]int)
	var counts []int // descending
	for i := len(ops) - 1; i >= 0; i-- {
		name := ops[i].Name
		if name == "" {
			continue
		}
		seen[name]++
		switch seen[name] {
		case 1:
			newest[name] = i
		case 2:
			if op := ops[newest[name]]; op.Error != nil || len(op.Result) > 0 {
				counts = append(counts, i+1)
			}
		}
	}
	slices.Reverse(counts)
	return counts
}

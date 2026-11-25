package telemetry

import (
	"bytes"
	"encoding/json"
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
	fullSize, err := renderedSize(record, render)
	if err == nil && fullSize <= maxBytes {
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
	// Clearing a Result also sets the operation's Truncated flag. The array
	// rendering writes the flag: `"truncated":true` (16 bytes) replaces
	// `"result":<value>`. So there clearing a small Result, whose value is
	// 7 bytes or shorter (such as `true`, `1`, `null` or `"ok"`), makes the
	// record larger, and it cannot help the record fit. The by-name
	// rendering writes no per-operation flag. So there clearing any Result
	// makes the record smaller or leaves it unchanged.
	//
	// So Step 1 renders the record once with every small Result cleared.
	// If that render is smaller than the record as given, the rendering
	// does not write the flag, and small results stay candidates. Otherwise
	// Step 1 clears only large results and leaves the small ones in the
	// record. A large Result is always a candidate. Then each cleared
	// candidate makes the record smaller or leaves it unchanged, and the
	// search returns the fewest results to clear. The check costs one
	// render, and only when the record has a small result.
	//
	// Step 2 removes whole operations. It also binary-searches the number
	// of oldest operations to remove. Removing an operation can make the
	// rendered record larger. The by-name rendering shows a name's Error
	// and Result only when the name occurs once (see
	// GroupOperationsByName). So removing one of two operations that share
	// a name makes the other operation's Error and Result appear. Then the
	// search can return a count that fits while a smaller count also fits.
	// Step 2 then removes more operations than the limit required. This
	// can happen only in the by-name rendering, including the by-name part
	// of OperationsFormatBoth. In the array rendering every removal makes
	// the record smaller, so Step 2 removes the fewest. This is an accepted
	// trade-off: the cost stays O(log n) renders for every record shape and
	// every Renderer.
	//
	// Each candidate record is sized with the final truncation metadata
	// already set. Step 2 returns only a count whose candidate record was
	// rendered and fit. So a wrong guess can only remove extra operations.
	// It never makes the result exceed the limit.

	ops := record.Operations
	n := len(ops)

	// The content fields are json.RawMessage with omitempty. An empty
	// non-nil RawMessage is never rendered, so dropping it frees no bytes.
	// So every presence check below is len > 0, not != nil; otherwise an
	// empty value would set Truncated and the Dropped* flags for content
	// that was never in the output.

	// Step 1: clear the Result of each candidate operation, oldest first.
	// The record with no Result cleared was rendered above and does not
	// fit, so the search starts at one.
	var small []int
	for i, op := range ops {
		if len(op.Result) > 0 && !clearingShrinks(op.Result) {
			small = append(small, i)
		}
	}
	keepSmall := false
	if len(small) > 0 && err == nil {
		probe := record
		probe.Operations = make([]OperationRecord, n)
		copy(probe.Operations, ops)
		for _, i := range small {
			probe.Operations[i].Result = nil
			probe.Operations[i].Truncated = true
		}
		size, perr := renderedSize(probe, render)
		keepSmall = perr == nil && size < fullSize
	}
	var candidates []int
	for i, op := range ops {
		if len(op.Result) > 0 && (keepSmall || clearingShrinks(op.Result)) {
			candidates = append(candidates, i)
		}
	}
	m := len(candidates)
	// applyResultDrops returns a copy of ops with the Result of the oldest
	// j candidates cleared.
	applyResultDrops := func(j int) []OperationRecord {
		dropped := make([]OperationRecord, n)
		copy(dropped, ops)
		for _, i := range candidates[:j] {
			dropped[i].Result = nil
			dropped[i].Truncated = true
		}
		return dropped
	}
	if m > 0 {
		j := 1 + sort.Search(m, func(i int) bool {
			candidate := record
			candidate.Operations = applyResultDrops(i + 1)
			candidate.Truncated = true
			return fits(candidate, maxBytes, render)
		})
		if j > m {
			j = m // clearing every candidate still doesn't fit; apply all, then Step 2.
		}
		record.Operations = applyResultDrops(j)
		record.Truncated = true
		if fits(record, maxBytes, render) {
			return record
		}
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

// resultKeyLen and truncatedKeyLen are the byte lengths of the array
// rendering's `"result":` key and its `"truncated":true` member.
const (
	resultKeyLen    = len(`"result":`)
	truncatedKeyLen = len(`"truncated":true`)
)

// clearingShrinks reports whether replacing `"result":<result>` with
// `"truncated":true` makes the array rendering smaller. A result for which
// it is false is a small result. The JSON encoder
// renders a json.RawMessage compacted, so the compacted length counts.
func clearingShrinks(result json.RawMessage) bool {
	size := len(result)
	var buf bytes.Buffer
	if err := json.Compact(&buf, result); err == nil {
		size = buf.Len()
	}
	return resultKeyLen+size > truncatedKeyLen
}

// fits reports whether record's rendered size is within maxBytes.
func fits(record Record, maxBytes int, render Renderer) bool {
	size, err := renderedSize(record, render)
	return err == nil && size <= maxBytes
}

// renderedSize returns the byte length of record as render renders it.
func renderedSize(record Record, render Renderer) (int, error) {
	b, err := render(record)
	return len(b), err
}

package telemetry

import (
	"encoding/json"
	"sort"
)

// Renderer produces the exact byte shape a Truncate call should size
// against. Exporters with custom rendering (OperationsFormat) provide
// their own Renderer so truncation sizes against what is actually sent.
type Renderer func(Record) ([]byte, error)

// defaultRenderer is the canonical JSON shape via json.Marshal.
func defaultRenderer(record Record) ([]byte, error) {
	return json.Marshal(record)
}

// Truncate returns a copy of record, best-effort truncated to fit within
// maxBytes when rendered via render (or canonical JSON if render is nil).
// Drop order: operation results (oldest first), whole operations (oldest
// first), then Input, then Output. Identity/timeline fields are never
// dropped.
//
// When render is nil, dropping more content cannot increase canonical JSON
// size, so Truncate binary-searches for the smallest drop count. A caller-
// supplied renderer may grow — for example by-name output that starts
// including an error once a duplicate name is gone — so those paths walk
// oldest-first and stop at the first candidate that fits.
//
// Each candidate is sized with the final truncation metadata already set,
// so the returned record accounts for those extra bytes. Truncate copies
// the operations slice and does not write through to the caller's backing
// array.
//
// When maxBytes <= 0, returns record unchanged (truncation disabled).
func Truncate(record Record, maxBytes int, render Renderer) Record {
	if maxBytes <= 0 {
		return record
	}
	// Binary search is only sound for the canonical JSON renderer. Capture
	// that before substituting defaultRenderer for a nil argument.
	ordered := render != nil
	if render == nil {
		render = defaultRenderer
	}
	if fits(record, maxBytes, render) {
		return record
	}

	ops := record.Operations
	n := len(ops)

	// Step 1: drop each operation's Result field, oldest first.
	if ordered {
		// A supplied renderer is not assumed monotonic, so this walks
		// oldest-first. Copy once, skip ops with no Result, and render
		// only after a result is actually removed. Otherwise
		// IncludeOperationResults:false re-encodes the same record once
		// per operation before any whole-operation drop.
		dropped := make([]OperationRecord, n)
		copy(dropped, ops)
		record.Operations = dropped
		for i := 0; i < n; i++ {
			if dropped[i].Result == nil {
				continue
			}
			dropped[i].Result = nil
			dropped[i].Truncated = true
			record.Truncated = true
			if fits(record, maxBytes, render) {
				return record
			}
		}
	} else {
		// applyResultDrops returns a copy of ops with the oldest j
		// operations' Result cleared, and whether any Result was
		// actually cleared.
		applyResultDrops := func(j int) (dropped []OperationRecord, any bool) {
			dropped = make([]OperationRecord, n)
			copy(dropped, ops)
			for i := 0; i < j; i++ {
				if dropped[i].Result != nil {
					dropped[i].Result = nil
					dropped[i].Truncated = true
					any = true
				}
			}
			return dropped, any
		}
		resultFits := func(j int) bool {
			candidate := record
			cops, any := applyResultDrops(j)
			candidate.Operations = cops
			if any {
				candidate.Truncated = true
			}
			return fits(candidate, maxBytes, render)
		}
		j := dropCount(n+1, false, resultFits)
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
	}

	// Step 2: drop whole operations, oldest first.
	ops = record.Operations
	n = len(ops)
	opFits := func(k int) bool {
		candidate := record
		candidate.Operations = ops[k:]
		if k > 0 {
			candidate.Truncated = true
			candidate.DroppedOperations = k
		}
		return fits(candidate, maxBytes, render)
	}
	k := dropCount(n+1, ordered, opFits)
	if k > n {
		k = n // even dropping every operation does not fit; drop all, then Step 3.
	}
	record.Operations = ops[k:]
	if k > 0 {
		record.Truncated = true
		record.DroppedOperations = k
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

// dropCount returns how many leading items to drop. n is the search space
// (typically len(ops)+1, including "drop none"). When ordered is true it
// walks 0..n-1 and returns the first index that fits, so a later false
// after an earlier true cannot skip that first fit. When ordered is false
// it binary-searches; pred must then be monotonic.
func dropCount(n int, ordered bool, pred func(int) bool) int {
	if ordered {
		for i := 0; i < n; i++ {
			if pred(i) {
				return i
			}
		}
		return n
	}
	return sort.Search(n, pred)
}

// fits reports whether record's rendered size is within maxBytes.
func fits(record Record, maxBytes int, render Renderer) bool {
	b, err := render(record)
	if err != nil {
		return false
	}
	return len(b) <= maxBytes
}

// truncateContent truncates value to maxLen, returning a ContentField
// with Truncated=true if the value was shortened. Uses JSON-aware
// truncation when the value appears to be JSON, otherwise truncates at
// the byte boundary.
func truncateContent(value string, maxLen int) ContentField {
	if maxLen <= 0 || len(value) <= maxLen {
		return ContentField{Value: value}
	}

	truncated := truncateJSON(value, maxLen)
	return ContentField{Value: truncated, Truncated: true}
}

// truncateJSON performs JSON-aware truncation that tries to cut at a
// safe boundary (after a complete key-value pair) rather than mid-token.
// If the value doesn't look like JSON, it falls back to a simple byte
// truncation.
func truncateJSON(value string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if len(value) <= maxLen {
		return value
	}

	// If it doesn't look like JSON, do a simple truncation.
	if len(value) == 0 || (value[0] != '{' && value[0] != '[') {
		return value[:maxLen]
	}

	// Try to find a good cut point by looking for the last complete
	// JSON boundary character (comma, closing brace/bracket) within
	// the allowed length.
	cutPoint := findJSONCutPoint(value, maxLen)
	if cutPoint > 0 {
		return value[:cutPoint]
	}

	return value[:maxLen]
}

// findJSONCutPoint scans backwards from maxLen to find the last position
// that ends a complete JSON value (after a comma, or at a closing
// brace/bracket). Returns 0 if no good cut point is found within a
// reasonable distance from maxLen.
func findJSONCutPoint(value string, maxLen int) int {
	// Don't search too far back — if we can't find a boundary within
	// 25% of maxLen, just cut at maxLen.
	minPos := maxLen - maxLen/4
	if minPos < 1 {
		minPos = 1
	}

	inString := false
	escaped := false
	lastGoodPos := 0

	for i := 0; i < maxLen && i < len(value); i++ {
		ch := value[i]

		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' && inString {
			escaped = true
			continue
		}

		if ch == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		// Track positions after commas or closing delimiters as good
		// cut points.
		switch ch {
		case ',':
			pos := i + 1
			if pos >= minPos && pos <= maxLen {
				lastGoodPos = pos
			}
		case '}', ']':
			pos := i + 1
			if pos >= minPos && pos <= maxLen {
				lastGoodPos = pos
			}
		}
	}

	return lastGoodPos
}

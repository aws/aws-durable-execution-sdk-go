package durable

import (
	"strconv"
)

// opIDs mints operation IDs for one context. IDs are positional: a root
// context mints "1", "2", ... and a child context whose own operation ID is
// p mints "p-1", "p-2", .... Replay depends on every execution minting the
// same IDs in the same order, so opIDs is confined to its owning goroutine
// and is not safe for concurrent use.
type opIDs struct {
	prefix  string
	counter int
	// named, when non-empty, is the suffix that the NEXT claimed ID uses
	// instead of the positional counter, yielding "{prefix}-{named}". The
	// DAG layer sets it so a task's single underlying operation gets a
	// deterministic NAME-BASED id derived from the task name rather than a
	// shared positional counter. That is what lets concurrent DAG tasks run
	// flat under one scope context: their ids never touch a shared mutable
	// counter, so there is no cross-goroutine race to confine. It is
	// consumed by the first next() call and never auto-increments.
	named string
}

// next claims and returns the next operation ID.
func (ids *opIDs) next() string {
	if ids.named != "" {
		s := ids.formatSuffix(ids.named)
		ids.named = ""
		return s
	}
	ids.counter++
	return ids.format(ids.counter)
}

// peek returns the ID that the next call to next will claim, without
// claiming it.
func (ids *opIDs) peek() string {
	if ids.named != "" {
		return ids.formatSuffix(ids.named)
	}
	return ids.format(ids.counter + 1)
}

// skip claims the next ID without returning it. Replay uses skip to pass
// over operations that never started.
func (ids *opIDs) skip() {
	ids.counter++
}

// advance moves the counter forward by n positions. Used after replaying a
// terminal batch to skip the iteration IDs that were consumed from the
// parent context during the original execution.
func (ids *opIDs) advance(n int) {
	ids.counter += n
}

// child returns the ID minter for a child context whose own operation ID is
// entityID.
func (ids *opIDs) child(entityID string) *opIDs {
	return &opIDs{prefix: entityID}
}

func (ids *opIDs) format(n int) string {
	return ids.formatSuffix(strconv.Itoa(n))
}

// formatSuffix joins the context prefix and a raw suffix ("{prefix}-{suffix}",
// or just the suffix at the root). Used for both positional (numeric) and
// name-based operation IDs.
func (ids *opIDs) formatSuffix(suffix string) string {
	if ids.prefix == "" {
		return suffix
	}
	return ids.prefix + "-" + suffix
}

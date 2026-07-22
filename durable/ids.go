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
}

// next claims and returns the next operation ID.
func (ids *opIDs) next() string {
	ids.counter++
	return ids.format(ids.counter)
}

// peek returns the ID that the next call to next will claim, without
// claiming it.
func (ids *opIDs) peek() string {
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
	s := strconv.Itoa(n)
	if ids.prefix == "" {
		return s
	}
	return ids.prefix + "-" + s
}

//go:build durablecheck

package durable

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
)

// ErrWrongGoroutine indicates that a durable operation was invoked on a
// context from a goroutine other than the context's owner. Durable
// operations must be created in deterministic order, which requires
// confining each context to one goroutine. Use [Go] to run durable work
// concurrently. Match it with [errors.Is].
var ErrWrongGoroutine = errors.New(
	"durable: operation called from a goroutine that does not own the context; use durable.Go for concurrent durable work")

// goroutineOwner records the goroutine a context belongs to and detects
// durable operations invoked from any other goroutine.
type goroutineOwner struct {
	id uint64
	ok bool
}

// currentGoroutineOwner captures the calling goroutine's identity. If the
// identity cannot be determined, ownership checks are disabled rather than
// failing closed, because the check is a diagnostic aid and must never
// reject correct programs.
func currentGoroutineOwner() goroutineOwner {
	id, ok := goid()
	return goroutineOwner{id: id, ok: ok}
}

// check reports an error if the calling goroutine is not the owner.
func (o goroutineOwner) check() error {
	if !o.ok {
		return nil
	}
	id, ok := goid()
	if !ok {
		return nil
	}
	if id != o.id {
		return fmt.Errorf("%w (owner goroutine %d, caller goroutine %d)", ErrWrongGoroutine, o.id, id)
	}
	return nil
}

var goroutinePrefix = []byte("goroutine ")

// goid returns the current goroutine's ID, parsed from the runtime stack
// header ("goroutine N [running]:"). The runtime does not expose goroutine
// IDs directly; the header format has been stable across Go releases and is
// relied on by established leak-detection tooling.
func goid() (uint64, bool) {
	buf := make([]byte, 64)
	n := runtime.Stack(buf, false)
	header := buf[:n]
	if !bytes.HasPrefix(header, goroutinePrefix) {
		return 0, false
	}
	header = header[len(goroutinePrefix):]
	end := bytes.IndexByte(header, ' ')
	if end <= 0 {
		return 0, false
	}
	id, err := strconv.ParseUint(string(header[:end]), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

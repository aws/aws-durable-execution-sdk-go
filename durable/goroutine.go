//go:build !durablenocheck

package durable

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
)

// ErrWrongGoroutine is returned when a durable operation is invoked on a
// [Context] from a goroutine other than the one that owns that Context.
// Every operation that claims an operation ID performs the check, so every
// exported operation on a Context can return it: [Step], [StepAsync],
// [Wait], [WaitAsync], [Invoke], [InvokeAsync], [RunInChildContext],
// [RunInChildContextAsync], [Go], [CreateCallback], [WaitForCallback],
// [WaitForCondition], [Map], [Parallel], [All], [AllSettled], [Any], and
// [Race]. The check runs before the operation claims its operation ID, so
// a rejected call consumes no ID and records no checkpoint. An Async
// operation reports the error through the returned [Future]. Match it with
// [errors.Is]; the returned error wraps ErrWrongGoroutine and names the
// owner and caller goroutines.
//
// Every Context is owned by the goroutine it was created on: the handler
// goroutine for the root Context, and the child goroutine for a Context
// created by [Go] or [RunInChildContextAsync]. Durable operations must be
// claimed in a deterministic order for replay to work, which requires
// confining each Context to one goroutine. Use [Go] to run durable work
// concurrently.
//
// The check is enabled in every default build. Building with
// -tags durablenocheck compiles it out; in that build ErrWrongGoroutine is
// declared but never returned, and a foreign-goroutine call is not detected.
// If the runtime does not expose the goroutine identity (see goid), the
// check is disabled at runtime rather than rejecting correct programs.
var ErrWrongGoroutine = errors.New(
	"durable: operation called from a goroutine that does not own the context; use durable.Go for concurrent durable work")

// goroutineOwner records the goroutine a context belongs to and detects
// durable operations invoked from any other goroutine.
//
// The check runs on every claimOperation call. Its cost is one
// runtime.Stack call into a 64-byte buffer plus a short integer parse.
// runtime.Stack walks and symbolizes every frame of the calling stack even
// though only the first line is kept, so the cost grows with stack depth.
// BenchmarkClaimOperation (goroutine_bench_test.go) measured the checked
// call against the unchecked one on an AMD EPYC 9R14 host with Go 1.25:
//
//	unchecked                  30 ns/op at every depth
//	checked, benchmark depth  4.2 µs/op (about 10 frames)
//	checked, +16 frames      10.5 µs/op
//	checked, +64 frames      29.5 µs/op
//
// That is roughly 0.4 µs per frame. A Lambda handler invoking a durable
// operation is about 12 to 16 frames deep, so the check costs about 5 µs
// per operation in practice. Every claim is followed by a checkpoint
// request to the service, which takes milliseconds, so the check adds well
// under one percent to the cost of an operation. The check is therefore on
// in every default build; -tags durablenocheck compiles it out.
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

// disabledGoroutineOwner returns an owner whose check always succeeds. It
// exists so a benchmark can compare a checked claimOperation with an
// unchecked one inside a single build.
func disabledGoroutineOwner() goroutineOwner { return goroutineOwner{} }

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
// relied on by established leak-detection tooling. A 64-byte buffer holds
// the header line; runtime.Stack discards the rest.
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

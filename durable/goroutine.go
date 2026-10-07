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

// ErrWrongContext is returned when a durable operation is called on a
// Context that is not the innermost active context of the calling
// goroutine. Inside a RunInChildContext, Go, Map, or Parallel body, only
// the context that body received may claim an operation; an operation on
// an enclosing context (the parent, or a sibling) returns this error. A
// step body receives a StepContext, which exposes no durable operations,
// so any operation claimed from a step body is on an enclosing context
// and returns this error.
//
// The check runs before the operation claims its operation ID, so a
// rejected call consumes no ID and records no checkpoint. An Async
// operation reports the error through the returned Future. Match it with
// errors.Is; the returned error wraps ErrWrongContext and names the
// operation and the two contexts.
//
// The goroutine-ownership check runs first. A body that runs on its own
// goroutine (StepAsync, RunInChildContextAsync, Go, a Map item, or a
// Parallel branch) does not own an enclosing context, so an operation on
// that context fails with [ErrWrongGoroutine] instead. ErrWrongContext is
// returned when the call runs on the goroutine that owns the claimed
// context: inside a Step body, a blocking RunInChildContext body, a
// WaitForCondition check, a WaitForCallback submitter, or on a child
// context kept from a body that already returned.
//
// The check is enabled in every default build. Building with
// -tags durablenocheck compiles it out.
var ErrWrongContext = errors.New(
	"durable: operation called on a context that is not the innermost active context; inside a RunInChildContext, Go, Map, or Parallel body use the context that body received, and claim no operation on an enclosing context from inside a step body")

// checkActive reports an [ErrWrongContext] error when c is not the
// innermost active context of its goroutine. name is the operation being
// claimed. The check is skipped when the goroutine identity is unavailable,
// because then contexts of different goroutines cannot be told apart.
func (c *execContext) checkActive(name string) error {
	if c.active == nil || !c.owner.ok {
		return nil
	}
	f := c.active.cur.Load()
	if f == nil || (f.ctx == c && f.stepID == "") {
		return nil
	}
	return fmt.Errorf("%w (operation %q on context %q, active context %q)", ErrWrongContext, name, c.contextID(), f.id())
}

// enterStepBody marks the step body or condition check of operation id,
// claimed on c, as the innermost active body of c's goroutine. It returns
// the function that restores the previous frame. A body that runs on a
// goroutine other than c's owner marks nothing: the ownership check
// already rejects a claim from that goroutine, and c's owner keeps running
// its own code.
func (c *execContext) enterStepBody(id string) func() {
	return c.enterFrame(&activeFrame{ctx: c, stepID: id})
}

// enterBody marks c's own body as the innermost active body of c's
// goroutine. It returns the function that restores the previous frame.
// The SDK calls it around every child body that runs on the goroutine of
// the context it was derived from, and around SDK code that claims
// operations on c from that goroutine.
func (c *execContext) enterBody() func() {
	return c.enterFrame(&activeFrame{ctx: c})
}

// enterFrame installs f as the current frame of c's tracker when the
// calling goroutine owns c, and returns the function that restores the
// previous frame.
func (c *execContext) enterFrame(f *activeFrame) func() {
	if c.active == nil || !c.owner.ok || c.owner.check() != nil {
		return func() {}
	}
	prev := c.active.cur.Swap(f)
	return func() { c.active.cur.Store(prev) }
}

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

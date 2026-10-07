//go:build durablenocheck

package durable

import "errors"

// ErrWrongGoroutine indicates that a durable operation was invoked on a
// [Context] from a goroutine other than the one that owns it. This file is
// compiled only with -tags durablenocheck, which removes the ownership
// check: in such a build ErrWrongGoroutine is declared so callers can
// reference it unconditionally, but it is never returned, and a durable
// operation invoked from a foreign goroutine is not detected. See the
// default build for the full contract.
var ErrWrongGoroutine = errors.New(
	"durable: operation called from a goroutine that does not own the context; use durable.Go for concurrent durable work")

// goroutineOwner is a no-op stand-in for the goroutine ownership record.
// The durablenocheck build tag is an opt-out for callers who have measured
// the check's cost in their own workload and accept undetected
// foreign-goroutine calls in exchange.
type goroutineOwner struct{}

// currentGoroutineOwner returns a no-op owner.
func currentGoroutineOwner() goroutineOwner { return goroutineOwner{} }

// disabledGoroutineOwner returns a no-op owner. It matches the default
// build's constructor so the benchmark compiles in both builds.
func disabledGoroutineOwner() goroutineOwner { return goroutineOwner{} }

// check always succeeds.
func (goroutineOwner) check() error { return nil }

// ErrWrongContext indicates that a durable operation was called on a
// [Context] that is not the innermost active context of the calling
// goroutine. This file is compiled only with -tags durablenocheck, which
// removes the check: in such a build ErrWrongContext is declared so callers
// can reference it unconditionally, but it is never returned. See the
// default build for the full contract.
var ErrWrongContext = errors.New(
	"durable: operation called on a context that is not the innermost active context; inside a RunInChildContext, Go, Map, or Parallel body use the context that body received, and claim no operation on an enclosing context from inside a step body")

// checkActive always succeeds.
func (*execContext) checkActive(string) error { return nil }

// enterStepBody marks nothing.
func (*execContext) enterStepBody(string) func() { return func() {} }

// enterBody marks nothing.
func (*execContext) enterBody() func() { return func() {} }

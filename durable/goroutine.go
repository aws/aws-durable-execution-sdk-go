//go:build !durablecheck

package durable

import "errors"

// ErrWrongGoroutine indicates that a durable operation was invoked on a
// context from a goroutine other than the context's owner. In production
// builds (without the "durablecheck" build tag) this error is never
// returned, but it is declared so callers can reference it unconditionally.
var ErrWrongGoroutine = errors.New(
	"durable: operation called from a goroutine that does not own the context; use durable.Go for concurrent durable work")

// goroutineOwner records the goroutine a context belongs to. In production
// builds (without the "durablecheck" build tag) ownership checks are
// disabled: the check is a diagnostic aid for development, not a
// correctness requirement, and parsing runtime.Stack on every
// claimOperation call has non-trivial overhead.
//
// Build with -tags=durablecheck to enable goroutine ownership validation.
type goroutineOwner struct{}

// currentGoroutineOwner returns a no-op owner in production builds.
func currentGoroutineOwner() goroutineOwner { return goroutineOwner{} }

// check always succeeds in production builds.
func (goroutineOwner) check() error { return nil }

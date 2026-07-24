package durable

import "fmt"

// Deps is the in-memory, single-invocation view of resolved upstream
// results passed to a DAG task's body and its runIf predicate. It contains
// the actual Go values produced by inline dependencies that SUCCEEDED this
// run (not re-deserialized), keyed by task name. Failed, skipped, and
// ordering-only dependencies are absent.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Deps struct {
	m map[string]any
}

// newDeps builds a Deps view from a name->value map (nil-safe).
func newDeps(m map[string]any) Deps {
	if m == nil {
		m = map[string]any{}
	}
	return Deps{m: m}
}

// Get returns the typed result of the upstream task referenced by h.
// Returns [ErrDepNotAvailable] if the dependency is absent (failed,
// skipped, or not an inline dep), or [ErrDepTypeMismatch] if the stored
// value is not a T. The result type is preserved from [TaskHandle]; no
// manual type assertion is needed at the call site.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Get[T any](d Deps, h TaskHandle[T]) (T, error) {
	var zero T
	v, ok := d.m[h.name]
	if !ok {
		return zero, fmt.Errorf("%w: %q", ErrDepNotAvailable, h.name)
	}
	tv, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("%w: %q", ErrDepTypeMismatch, h.name)
	}
	return tv, nil
}

// MustGet is like [Get] but panics on error. Use it at call sites that
// treat a missing ALL_SUCCESS dependency as a programming bug.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func MustGet[T any](d Deps, h TaskHandle[T]) T {
	v, err := Get(d, h)
	if err != nil {
		panic(err)
	}
	return v
}

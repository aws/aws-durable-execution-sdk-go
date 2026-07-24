package operations

import "errors"

// ErrSuspended is the sentinel returned up the call stack when a durable
// operation cannot make forward progress within the current invocation
// (a wait, scheduled retry, or pending callback) and the execution must be
// suspended. It is exported so higher-level orchestrators built on the
// operations package (e.g. the experimental DAG scheduler) can recognize a
// suspend signal returned from an operation and participate in the
// invocation-wide suspend protocol rather than mistaking it for a task
// failure.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
var ErrSuspended = errSuspended

// IsSuspended reports whether err is (or wraps) the suspend sentinel.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func IsSuspended(err error) bool { return errors.Is(err, errSuspended) }

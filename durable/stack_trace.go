package durable

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// MaxStackTraceFrames is the most frames the SDK records in a failure's
// stack trace. Frames are counted from the point of failure outward, so a
// deeper stack loses its outermost frames. The bound keeps a deep stack
// from inflating a checkpoint past the operation size limit.
const MaxStackTraceFrames = 32

// WithStackTraces controls whether the SDK records a stack trace when user
// code fails. Capture is enabled by default.
//
// User code is the handler and every callback a durable operation runs: a
// step body, a child context body, a Map or Parallel item function, and a
// WaitForCondition check or wait strategy. The trace is taken when that
// code hands the failure to the SDK, so the first frame names the user
// function that produced it:
//
//   - For a returned error, the first frame is the failing function itself
//     and the frames after it run outward from the SDK's call into that
//     function through the code that invoked the operation.
//   - For a panic, the trace is taken inside the SDK's recovery while the
//     panicking frames are still on the stack, so it starts at the
//     panicking function.
//   - An error that supplies its own trace through a
//     "StackTrace() []string" method anywhere in its chain is recorded with
//     that trace instead.
//
// Each frame is one string of the form "function file:line", innermost
// frame first, and at most [MaxStackTraceFrames] frames are kept. The
// frames are recorded in the operation's checkpoint and in the FAILED
// invocation response, and the typed operation errors expose them as
// StackTrace. An operation error that already carries a trace keeps it
// when a later operation or the handler returns it, so the recorded trace
// always points at the failure that happened first.
//
// Pass false to record no stack traces. Failures then carry an empty
// StackTrace on every path. Disable capture when checkpoints must stay as
// small as possible or when file paths from the build must not leave the
// function.
func WithStackTraces(enabled bool) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.noStackTraces = !enabled })
}

// captureStackTrace returns the calling goroutine's stack as one string per
// frame, innermost first, formatted "function file:line". The frame of
// captureStackTrace itself and the skip frames above it are omitted, and
// so are the Go runtime's own frames, so a trace taken inside a recovered
// panic begins at the panicking function. At most [MaxStackTraceFrames]
// frames are returned.
func captureStackTrace(skip int) []string {
	// Runtime frames are dropped after the fact, so the raw buffer holds
	// more program counters than the bound to leave room for them.
	pcs := make([]uintptr, MaxStackTraceFrames*2)
	// Skip runtime.Callers itself and captureStackTrace.
	n := runtime.Callers(skip+2, pcs)
	if n == 0 {
		return nil
	}
	frames := runtime.CallersFrames(pcs[:n])
	trace := make([]string, 0, MaxStackTraceFrames)
	for len(trace) < MaxStackTraceFrames {
		fr, more := frames.Next()
		if fr.Function != "" && !strings.HasPrefix(fr.Function, "runtime.") {
			trace = append(trace, formatFrame(fr.Function, fr.File, fr.Line))
		}
		if !more {
			break
		}
	}
	if len(trace) == 0 {
		return nil
	}
	return trace
}

// formatFrame renders one trace frame as "function file:line".
func formatFrame(function, file string, line int) string {
	return fmt.Sprintf("%s %s:%d", function, file, line)
}

// functionFrame returns the trace frame naming fn's own declaration: its
// function name and the file and line where it begins. fn must be a func
// value; the second result is false when it is not or when the runtime
// holds no symbol for it.
func functionFrame(fn any) (string, bool) {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func || v.IsNil() {
		return "", false
	}
	pc := v.Pointer()
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "", false
	}
	file, line := f.FileLine(pc)
	return formatFrame(f.Name(), file, line), true
}

// suppliedStackTrace returns the trace an error carries itself, through a
// StackTrace() []string method on any error in its chain. It returns nil
// when no error in the chain supplies one.
func suppliedStackTrace(err error) []string {
	var st interface{ StackTrace() []string }
	if errors.As(err, &st) {
		return st.StackTrace()
	}
	return nil
}

// boundTrace truncates trace to [MaxStackTraceFrames] frames, keeping the
// innermost ones. An empty trace yields nil.
func boundTrace(trace []string) []string {
	if len(trace) == 0 {
		return nil
	}
	if len(trace) > MaxStackTraceFrames {
		trace = trace[:MaxStackTraceFrames]
	}
	return trace
}

// stackTrace captures the calling goroutine's stack for a failure record,
// skipping the skip frames above the caller. It returns nil when the
// handler was built with [WithStackTraces] set to false.
func (c *execContext) stackTrace(skip int) []string {
	if c.noStackTraces {
		return nil
	}
	return captureStackTrace(skip + 1)
}

// returnedErrorTrace builds the trace for err, an error that the user
// function userFn returned to the SDK. When the error supplies its own
// trace, that trace is returned, bounded to [MaxStackTraceFrames].
// Otherwise the trace begins with userFn's own frame, so it names
// the function that produced the error, followed by the calling
// goroutine's stack from the caller of returnedErrorTrace outward, with
// the skip frames above that caller omitted. The caller's stack is where
// the SDK received the error, so those frames run out through the code
// that invoked the operation. It returns nil when the handler was built
// with [WithStackTraces] set to false, and nil for the SDK's own
// suspension and checkpointer-termination signals, which are never
// recorded as failures.
func (c *execContext) returnedErrorTrace(userFn any, err error, skip int) []string {
	if c.noStackTraces || errors.Is(err, errSuspendExecution) || errors.Is(err, errCheckpointTerminated) {
		return nil
	}
	if supplied := suppliedStackTrace(err); len(supplied) > 0 {
		return boundTrace(supplied)
	}
	trace := captureStackTrace(skip + 1)
	if frame, ok := functionFrame(userFn); ok {
		trace = append([]string{frame}, trace...)
	}
	return boundTrace(trace)
}

// runUserFunc runs one call into user code and returns its result together
// with the stack trace of any failure. call performs the call; userFn is
// the user's function value, used to name the origin frame of a returned
// error as described by [execContext.returnedErrorTrace]. A panic is
// recovered into an error whose message is panicMessage followed by the
// panic value; its trace is taken inside the recovery, while the panicking
// frames are still on the stack, so it begins at the panicking function.
// trace is nil when call succeeds or when capture is disabled.
func runUserFunc[O any](ec *execContext, userFn any, panicMessage string, call func() (O, error)) (result O, trace []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s: %v", panicMessage, r)
			// Skip this deferred function's own frame.
			trace = ec.stackTrace(1)
		}
	}()
	result, err = call()
	if err != nil {
		// Skip runUserFunc's own frame so the captured stack starts at
		// the operation that received the error.
		trace = ec.returnedErrorTrace(userFn, err, 1)
	}
	return result, trace, err
}

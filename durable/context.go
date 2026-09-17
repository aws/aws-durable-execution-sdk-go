package durable

import (
	"context"
	"time"
)

// Context is the durable execution context passed to handler functions and
// child-context functions. It carries the execution's identity and replay
// state, and it is the required first argument of every durable operation.
//
// Context implements [context.Context], so it can be passed directly to AWS
// SDK calls and other context-aware APIs.
//
// A Context is owned by the goroutine it was created on. Durable operations
// invoked on a Context from any other goroutine fail. Use [Go] to run
// durable work concurrently.
//
// Context is sealed: only the SDK can implement it. External types that
// embed or imitate this interface will fail to compile because of the
// unexported method.
type Context interface {
	context.Context

	// ExecutionArn returns the ARN of the current durable execution.
	ExecutionArn() string

	// RequestID returns the AWS request ID of the current Lambda
	// invocation. It is empty outside a Lambda invocation (such as under
	// the [durabletest] local runner).
	//
	// The request ID varies across invocations of one execution, so using
	// it in handler logic outside a [Step] introduces non-determinism on
	// replay. If the raw aws-lambda-go invocation context is genuinely
	// needed, it remains reachable from the embedded [context.Context] via
	// lambdacontext.FromContext(ctx).
	RequestID() string

	// InvokedFunctionARN returns the ARN used to invoke the current
	// Lambda function. It is empty outside a Lambda invocation (such as
	// under the [durabletest] local runner).
	InvokedFunctionARN() string

	// Logger returns the logger for this context, enriched with durable
	// execution metadata. During replay, log output is suppressed by
	// default so that replayed code does not duplicate log lines.
	Logger() Logger

	// IsReplaying reports whether the execution is currently replaying
	// previously checkpointed operations.
	IsReplaying() bool

	// sealed prevents external implementations of Context. Only the SDK
	// creates valid Context values; passing a non-SDK Context to a durable
	// operation panics at runtime as defence in depth.
	sealed()
}

// StepContext is the context passed to step bodies, condition checks, and
// callback submitters. It implements [context.Context], so it can be passed
// directly to AWS SDK calls made inside the step.
//
// StepContext deliberately exposes no durable operations: a step is a single
// atomic unit of work. To group durable operations, use [RunInChildContext]
// or [Go].
//
// StepContext is sealed: only the SDK can implement it. External types that
// embed or imitate this interface will fail to compile because of the
// unexported method. Sealing lets the SDK add methods to StepContext
// without breaking user code.
type StepContext interface {
	context.Context

	// Logger returns the logger for the current step.
	Logger() Logger

	// Attempt returns the 1-based attempt number for this step
	// execution. The first attempt is 1.
	Attempt() int

	// sealed prevents external implementations of StepContext. Only the
	// SDK creates StepContext values, so adding a method to this
	// interface cannot break a user type.
	sealed()
}

// Logger is the minimal structured logging interface used by the SDK.
// Fields are alternating key-value pairs, as in log/slog.
type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
}

// SerdesContext provides contextual information to a [Serdes] implementation,
// enabling context-aware serialization strategies (e.g., using the execution
// ARN or operation ID in file paths for a filesystem-backed serdes).
type SerdesContext struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// OperationID is the positional ID of the operation being serialized
	// (e.g., "1", "1-2-3").
	OperationID string

	// DurableExecutionArn is the ARN of the current durable execution.
	DurableExecutionArn string
}

// Serdes serializes and deserializes operation inputs and results for
// checkpoint storage. The default Serdes uses encoding/json.
//
// Implementations receive the invocation's [context.Context] for
// cancellation and deadline propagation (e.g., when offloading payloads to
// external storage), and a [SerdesContext] with the operation's identity
// and the execution ARN, enabling context-aware serialization strategies
// such as filesystem offloading keyed by operation.
type Serdes interface {
	Marshal(ctx context.Context, meta SerdesContext, v any) ([]byte, error)
	Unmarshal(ctx context.Context, meta SerdesContext, data []byte, v any) error
}

// Deserializer deserializes callback payloads submitted by external
// systems. It is set for the whole handler with [WithCallbackDeserializer].
// Without one, callbacks decode payloads with the handler-level [Serdes],
// which defaults to encoding/json; see [CreateCallback] for the full
// precedence and for how this differs from the other Durable Execution
// SDKs.
type Deserializer interface {
	Unmarshal(data []byte, v any) error
}

// ExecutionStartTime returns the start timestamp of the durable execution.
// This is the checkpointed start time of the root EXECUTION operation,
// recorded when the execution was created. It is the same
// value on every invocation of one execution (including replays), making it
// safe to use between durable operations without introducing
// non-determinism.
//
// For a wall-clock timestamp that varies across invocations, compute it
// inside a [Step] so it is checkpointed once and reused verbatim on replay.
//
// ExecutionStartTime is a package-level function rather than a [Context]
// method because Context is sealed and kept minimal: it exposes only
// [Context.ExecutionArn] (required by the SDK for operation identity on every
// checkpoint call) while derived conveniences that read from the checkpoint
// state—like start time—live as package functions layered on top.
func ExecutionStartTime(ctx Context) time.Time {
	ec, ok := ctx.(*execContext)
	if !ok {
		return time.Time{}
	}
	return ec.executionStartTime
}

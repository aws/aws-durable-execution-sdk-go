package durable

import (
	"context"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
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
type Context interface {
	context.Context

	// ExecutionArn returns the ARN of the current durable execution.
	ExecutionArn() string

	// LambdaContext returns the underlying Lambda invocation context for
	// the current invocation.
	LambdaContext() *lambdacontext.LambdaContext

	// Logger returns the logger for this context, enriched with durable
	// execution metadata. During replay, log output is suppressed by
	// default so that replayed code does not duplicate log lines.
	Logger() Logger

	// IsReplaying reports whether the execution is currently replaying
	// previously checkpointed operations.
	IsReplaying() bool
}

// StepContext is the context passed to step bodies, condition checks, and
// callback submitters. It implements [context.Context], so it can be passed
// directly to AWS SDK calls made inside the step.
//
// StepContext deliberately exposes no durable operations: a step is a single
// atomic unit of work. To group durable operations, use [RunInChildContext]
// or [Go].
type StepContext interface {
	context.Context

	// Logger returns the logger for the current step.
	Logger() Logger

	// Attempt returns the zero-indexed attempt number for this step
	// execution. The first attempt is 0.
	Attempt() int
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
	// OperationID is the positional ID of the operation being serialized
	// (e.g., "1", "1-2-3").
	OperationID string

	// DurableExecutionArn is the ARN of the current durable execution.
	DurableExecutionArn string
}

// Serdes serializes and deserializes operation inputs and results for
// checkpoint storage. The default Serdes uses encoding/json.
//
// Implementations receive a [SerdesContext] with the operation's identity
// and the execution ARN, enabling context-aware serialization strategies
// such as filesystem offloading keyed by operation.
type Serdes interface {
	Marshal(ctx SerdesContext, v any) ([]byte, error)
	Unmarshal(ctx SerdesContext, data []byte, v any) error
}

// Deserializer deserializes callback payloads submitted by external
// systems. Callbacks default to a passthrough (raw string) deserializer
// unless one is configured with [WithCallbackDeserializer].
type Deserializer interface {
	Unmarshal(data []byte, v any) error
}

// CurrentTime returns a replay-safe wall clock value for the given
// [Context]. During replay, it returns the zero [time.Time] so that code
// between durable operations does not produce non-deterministic timestamps.
// During live execution, it returns time.Now().
//
// Use CurrentTime instead of time.Now() for any timestamp read outside a
// Step body. Non-deterministic values that must be read fresh (e.g., an
// actual wall-clock timestamp for business logic) should be computed inside
// a [Step] so they are checkpointed once and reused verbatim on replay.
//
// # Determinism boundary marker
//
// This function is a determinism boundary: it intentionally produces
// different results during replay vs. live execution, ensuring that
// replayed code paths never diverge based on wall-clock drift.
func CurrentTime(ctx Context) time.Time {
	if ctx.IsReplaying() {
		return time.Time{}
	}
	return time.Now()
}

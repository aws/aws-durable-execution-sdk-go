package durable

import (
	"context"

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
}

// Logger is the minimal structured logging interface used by the SDK.
// Fields are alternating key-value pairs, as in log/slog.
type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
}

// Serdes serializes and deserializes operation inputs and results for
// checkpoint storage. The default Serdes uses encoding/json.
type Serdes interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// Deserializer deserializes callback payloads submitted by external
// systems. Callbacks default to a passthrough (raw string) deserializer
// unless one is configured with [WithCallbackDeserializer].
type Deserializer interface {
	Unmarshal(data []byte, v any) error
}

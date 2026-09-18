package durable

import (
	"context"
	"errors"
	"log/slog"
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
// invoked on a Context from any other goroutine fail with
// [ErrWrongGoroutine] before they claim an operation ID. Use [Go] to run
// durable work concurrently; it gives the new goroutine a Context of its
// own. See the package documentation section "Goroutine Ownership".
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

	// Logger returns the logger for this context. Its records carry the
	// request ID and execution ARN as structured attributes, and the
	// tenant ID when the invocation has one, whichever [slog.Handler] is
	// installed with [WithLogHandler] or [ConfigureLogging]. Inside a child
	// context (from [RunInChildContext], [Go], [Map], [Parallel], or
	// [WaitForCallback]) the records also carry the child operation's ID
	// as operationId and its name as operationName. While this context is
	// replaying, log output is suppressed so that replayed code does not
	// duplicate log lines, unless [ReplayLogModeEmit] is in effect, in
	// which case replayed records are emitted with the attribute
	// replay=true. Suppression is decided per branch: each context (root,
	// child context, and each concurrent branch from [Go], [Map], or
	// [Parallel]) consults its own replay state, so a branch that is still
	// replaying stays suppressed even after a sibling branch has reached
	// live execution.
	Logger() *slog.Logger

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

	// Logger returns the logger for the current step body, condition
	// check, or callback submitter. Its records carry the execution
	// attributes of the enclosing context, this operation's ID as
	// operationId, its name as operationName when it has one, and the
	// attempt number. The enclosing context's own operation attributes are
	// replaced, not repeated: a step inside a child context reports the
	// step, not the child.
	Logger() *slog.Logger

	// Attempt returns the 1-based attempt number of the current
	// execution of the user function. The first attempt is 1. It applies
	// to step bodies, to condition checks (where it is the poll attempt
	// number, the same value the wait strategy receives), and to callback
	// submitters (where it is the submitter's retry attempt).
	Attempt() int

	// sealed prevents external implementations of StepContext. Only the
	// SDK creates StepContext values, so adding a method to this
	// interface cannot break a user type.
	sealed()
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
// checkpoint storage. The default Serdes is [JSONSerdes], which uses
// encoding/json.
//
// Implementations receive the invocation's [context.Context] for
// cancellation and deadline propagation (e.g., when offloading payloads to
// external storage), and a [SerdesContext] with the operation's identity
// and the execution ARN, enabling context-aware serialization strategies
// such as filesystem offloading keyed by operation.
//
// The interface is untyped: Marshal takes any and Unmarshal fills a pointer
// passed as any. It has to be, because one Serdes value can serve every
// operation result type in a handler. The handler-wide default set with
// [WithSerdes] and the payload-offloading serdes from [NewFileSystemSerdes]
// both do exactly that, so neither can carry a single type parameter. For a
// serdes written for one result type, use [SerdesOf], which performs the
// type assertion once and hands typed values to your marshal and unmarshal
// functions.
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

// SerdesConfig names the handler-level serializer defaults that
// [ConfigureSerdes] replaces. A nil field keeps the value currently in
// effect, so a config that sets only one field leaves the other unchanged.
type SerdesConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Serdes replaces the default serializer for operation results: steps,
	// child contexts, invokes, and condition state. It has the same role as
	// [WithSerdes].
	Serdes Serdes

	// CallbackDeserializer replaces the default deserializer for callback
	// payloads submitted by external systems. It has the same role as
	// [WithCallbackDeserializer]. Once set it cannot be cleared: to return
	// callbacks to the standard serdes, set Serdes and pass a
	// CallbackDeserializer that delegates to it.
	CallbackDeserializer Deserializer
}

// ConfigureSerdes replaces the handler-level serializer defaults for the
// rest of the invocation. It is the in-handler counterpart of [WithSerdes]
// and [WithCallbackDeserializer], for a handler that must choose its
// serializer from the event payload rather than at construction time.
//
// The new defaults apply to operations started on ctx after the call and to
// every child context and concurrent branch derived from ctx after the
// call. A context derived before the call, such as a branch already
// started with [Go], keeps the configuration it was derived with.
// Per-operation options such as [WithStepSerdes] and [WithCallbackSerdes]
// continue to take precedence over the configured defaults, exactly as
// they do over the construction-time options. The handler's own input and
// result are always JSON and are not affected.
//
// ConfigureSerdes must run identically on every invocation of an
// execution, including replays. A step's result is written to the
// checkpoint with the serdes in effect when the step first runs, and read
// back from the checkpoint with the serdes in effect when the step is
// replayed. If those differ, the replay cannot decode the checkpointed
// bytes and the operation fails with a [SerdesError]; the execution cannot
// recover, because the checkpoint is fixed. So decide the configuration
// from inputs that are the same on every invocation: the event payload,
// or a value already returned by a durable operation. Never decide it from
// wall-clock time, random values, environment that can change between
// invocations, or the outcome of a non-durable call. For the same reason,
// call ConfigureSerdes at the same point in the handler on every
// invocation, normally before the first durable operation.
//
// ConfigureSerdes must be called on the goroutine that owns ctx, like every
// durable operation; from any other goroutine it fails with
// [ErrWrongGoroutine]. It returns an error only for that case and for a
// Context not created by the SDK.
//
//	func handler(ctx durable.Context, event OrderEvent) (OrderResult, error) {
//		if event.Compressed {
//			if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: gzipSerdes{}}); err != nil {
//				return OrderResult{}, err
//			}
//		}
//		// Every operation from here on uses gzipSerdes unless it passes
//		// its own serdes option.
//		...
//	}
func ConfigureSerdes(ctx Context, cfg SerdesConfig) error {
	ec, ok := ctx.(*execContext)
	if !ok {
		return errors.New("durable: ConfigureSerdes: Context was not created by the SDK")
	}
	return ec.configureSerdes(cfg)
}

// ReplayLogMode selects what happens to a log record emitted while the
// emitting context is replaying checkpointed operations. Set it for the
// whole handler with [WithReplayLogMode] or for the rest of an invocation
// with [ConfigureLogging].
type ReplayLogMode int

const (
	// ReplayLogModeUnchanged keeps the mode currently in effect. It is the
	// zero value, so a [LogConfig] that leaves the field unset does not
	// change the mode. [WithReplayLogMode] treats it as
	// [ReplayLogModeSuppress].
	ReplayLogModeUnchanged ReplayLogMode = iota

	// ReplayLogModeSuppress drops every record a context emits while it is
	// replaying, before the record is built, so replayed code does not
	// duplicate the lines it wrote when it first ran. This is the default.
	ReplayLogModeSuppress

	// ReplayLogModeEmit emits the records a context writes while it is
	// replaying, each with the attribute replay=true; a live record carries
	// no replay attribute. The mode is for diagnosing a replay problem: the
	// output shows what the replayed code did up to the point where the
	// problem appeared. Every line written before the execution suspended
	// appears again on each later invocation, once per invocation that
	// replays it, so expect duplicate lines. Records emitted in this mode
	// still pass through the handler's level filter and, for the default
	// handler, AWS_LAMBDA_LOG_LEVEL.
	//
	// The top-level replay key belongs to the SDK in every mode. An
	// attribute named replay that a handler body adds at the top level,
	// through [slog.Logger.With] or with a record, is dropped so the key
	// stays single-valued; the same name inside a group the logger opened
	// with [slog.Logger.WithGroup] is kept.
	ReplayLogModeEmit
)

// LogConfig names the logging settings that [ConfigureLogging] replaces. A
// zero field keeps the value currently in effect, so a config that sets
// only one field leaves the other unchanged.
type LogConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Handler replaces the [slog.Handler] behind [Context.Logger] and
	// [StepContext.Logger]. It has the same role as [WithLogHandler]. The
	// SDK attaches the execution attributes (requestId, executionArn, and
	// tenantId when present) and each scope's operation attributes to the
	// new handler through its WithAttrs method, wraps it with replay
	// suppression, and adds the fields plugins return from
	// [Plugin.EnrichLogContext], exactly as it does for the handler given
	// at construction. nil keeps the current handler.
	Handler slog.Handler

	// ReplayLogMode replaces the treatment of records emitted during
	// replay. It has the same role as [WithReplayLogMode].
	// [ReplayLogModeUnchanged], the zero value, keeps the current mode.
	ReplayLogMode ReplayLogMode
}

// ConfigureLogging replaces the logging settings for the rest of the
// invocation. It is the in-handler counterpart of [WithLogHandler] and
// [WithReplayLogMode], for a handler that must choose its logger from the
// event payload or from runtime configuration rather than at construction
// time, or that wants replayed log output only for one execution.
//
// The new settings apply to ctx and to every child context and concurrent
// branch derived from ctx after the call. A context derived before the
// call, such as a branch already started with [Go], keeps the settings it
// was derived with. A new handler reaches loggers obtained from
// [Context.Logger] or [StepContext.Logger] after the call; a logger
// obtained before the call keeps the previous handler. A new
// [ReplayLogMode] reaches every logger of ctx and of contexts derived from
// it after the call, including loggers obtained before the call, because
// the mode is read on every record.
//
// The settings last for the current invocation only. The next invocation
// of the execution starts from the construction-time options again. The
// handler body runs from its start on every invocation, so a call placed
// before the first durable operation re-applies the settings each time.
//
// ConfigureLogging does not affect determinism. It claims no operation ID,
// writes no checkpoint, and changes no operation's ordering or result, so
// it may be called conditionally and at different points on different
// invocations without causing a non-deterministic replay. This differs
// from [ConfigureSerdes], whose settings are baked into checkpoint content.
//
// ConfigureLogging must be called on the goroutine that owns ctx, like
// every durable operation; from any other goroutine it fails with
// [ErrWrongGoroutine]. It returns an error only for that case and for a
// Context not created by the SDK.
//
//	func handler(ctx durable.Context, event OrderEvent) (OrderResult, error) {
//		if event.Debug {
//			if err := durable.ConfigureLogging(ctx, durable.LogConfig{
//				Handler:       slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}),
//				ReplayLogMode: durable.ReplayLogModeEmit,
//			}); err != nil {
//				return OrderResult{}, err
//			}
//		}
//		// Records from here on, including from replayed steps, reach the
//		// text handler; replayed records carry replay=true.
//		...
//	}
func ConfigureLogging(ctx Context, cfg LogConfig) error {
	ec, ok := ctx.(*execContext)
	if !ok {
		return errors.New("durable: ConfigureLogging: Context was not created by the SDK")
	}
	return ec.configureLogging(cfg)
}

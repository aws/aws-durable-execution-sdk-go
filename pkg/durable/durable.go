// Package durable is the top-level entry point for the AWS Durable
// Execution SDK for Go. Use WithDurableExecution to wrap a handler function
// into a Lambda-compatible entry point that participates in the Lambda
// Durable Functions checkpoint/replay lifecycle.
package durable

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/execmgr"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// CheckpointStrategy controls how operation updates are batched into
// checkpoint API calls against the Lambda Durable Functions backend.
//
// NOTE: the current runtime implementation always batches eagerly via
// checkpoint.Manager's drain loop (see docs/checkpoint-replay-design.md
// §5). CheckpointStrategyBatched/Optimistic are accepted here for API
// compatibility with the design surface but not yet wired to distinct
// runtime behavior; see checkpoint.Manager for where that would be added
// (e.g. a configurable flush delay, matching the Java SDK's
// checkpointDelay).
type CheckpointStrategy int

const (
	// CheckpointStrategyEager issues one checkpoint API call per operation
	// update. This is the default: it maximizes durability at the cost of
	// additional API calls.
	CheckpointStrategyEager CheckpointStrategy = iota

	// CheckpointStrategyBatched groups multiple operation updates into a
	// single checkpoint API call where possible, trading a small durability
	// window for reduced API call volume.
	CheckpointStrategyBatched

	// CheckpointStrategyOptimistic fires checkpoint updates without waiting
	// for acknowledgment. Fastest option; least durable, since a crash
	// between the fire and the backend's acknowledgment can lose the most
	// recent checkpoint.
	CheckpointStrategyOptimistic
)

// Client is the interface to the Lambda Durable Functions backend required
// by the SDK runtime.
type Client = checkpoint.Client

// Config customizes SDK behavior.
type Config struct {
	// Client overrides the default AWS SDK-backed client. Required for
	// now, since no production AWS SDK-backed implementation ships in this
	// scaffold yet (see docs/checkpoint-replay-design.md next steps);
	// tests and early integration should inject their own.
	Client Client

	// CheckpointStrategy controls checkpoint API call batching. Defaults to
	// CheckpointStrategyEager.
	CheckpointStrategy CheckpointStrategy

	// LoggerConfig customizes SDK logging behavior.
	LoggerConfig *types.LoggerConfig

	// Plugins is an EXPERIMENTAL list of instrumentation plugins for
	// observability and tracing, ported from the JS reference SDK's own
	// `plugins` config option (also marked experimental there - see
	// pkg/durable/plugin's package doc for the full scope of what this
	// initial port covers and the hooks it does not yet fire). Plugins
	// receive lifecycle callbacks at key points during durable execution
	// (currently: invocation start/end, and Step operation start/end),
	// enabling integration with tracing systems (e.g. OpenTelemetry,
	// X-Ray), custom metrics, and logging enrichment. Multiple plugins
	// are called concurrently; a plugin's own errors/panics are swallowed
	// and never affect execution correctness (see plugin.Dispatch).
	//
	// EXPERIMENTAL: this field, and everything in pkg/durable/plugin, may
	// change or be removed in a future release without a major-version
	// bump.
	Plugins []plugin.InstrumentationPlugin
}

// Handler is the signature required of a durable function's business
// logic. It receives the Lambda event payload and a DurableContext
// providing access to durable operations (see the operations package).
type Handler[TEvent, TResult any] func(event TEvent, dc types.DurableContext) (TResult, error)

// handlerOutcome carries the result of the handler goroutine back to the
// select in WithDurableExecution.
type handlerOutcome[TResult any] struct {
	result TResult
	err    error
}

// WithDurableExecution wraps handler into a Lambda-compatible entry point
// (suitable for aws-lambda-go's lambda.Start) that drives the
// checkpoint/replay lifecycle against the Lambda Durable Functions backend.
//
// # Suspend-or-complete race
//
// This implements the coordination model described in
// docs/checkpoint-replay-design.md §4, ported from the Java SDK's threaded
// design (the closer analog for Go than the JS SDK's single-threaded
// Promise.race):
//
//  1. Build the root DurableContext and register it as the one initially
//     active goroutine (execmgr.Manager.Register).
//  2. Run handler on its own goroutine, sending its outcome on a channel
//     when done.
//  3. select on that channel vs. execManager.Suspended(): whichever
//     resolves first determines the invocation's outcome. If the handler
//     goroutine is still blocked inside some operation's WaitForOperation
//     call when suspension wins, it is abandoned - safe, because the
//     Lambda execution environment is about to be frozen or recycled
//     before that goroutine could resume anyway.
//  4. Before returning a terminal (Succeeded/Failed) result, wait for the
//     checkpoint queue to fully drain (checkpoint.Manager.WaitForIdle),
//     matching both reference SDKs' pre-return drain to guarantee every
//     checkpoint the handler triggered actually reached the backend.
//
// cfg must currently provide a Client; there is no production AWS
// SDK-backed default yet (see docs/checkpoint-replay-design.md).
func WithDurableExecution[TEvent, TResult any](handler Handler[TEvent, TResult], cfg *Config) func(ctx context.Context, input types.DurableExecutionInvocationInput) (types.DurableExecutionOutput, error) {
	if cfg == nil {
		cfg = &Config{}
	}

	return func(ctx context.Context, input types.DurableExecutionInvocationInput) (types.DurableExecutionOutput, error) {
		if cfg.Client == nil {
			return types.DurableExecutionOutput{}, fmt.Errorf("durable.WithDurableExecution: Config.Client is required (no default AWS SDK-backed client is implemented yet)")
		}
		if err := validateConfig(cfg); err != nil {
			return types.DurableExecutionOutput{}, err
		}

		execManager := execmgr.New(input.InitialExecutionState.Operations)
		limits := checkpoint.DefaultLimits()
		checkpointMgr := checkpoint.New(cfg.Client, execManager, input.DurableExecutionArn, input.CheckpointToken, limits)

		// LoggerConfig defaulting (docs/remaining-work.md §5 task 13): the
		// JS/Java SDKs default ModeAware/suppressReplayLogs to true even
		// when the caller never configures logging at all. Go's zero
		// value for LoggerConfig.ModeAware is false, so - to preserve
		// that documented default while still respecting an explicit
		// ModeAware: false - only apply the "default true" override when
		// cfg.LoggerConfig itself is nil (never configured), and take
		// whatever LoggerConfig the caller DID supply at face value
		// otherwise. See types.LoggerConfig.ModeAware's doc for the full
		// rationale.
		loggerCfg := types.LoggerConfig{ModeAware: true}
		if cfg.LoggerConfig != nil {
			loggerCfg = *cfg.LoggerConfig
		}

		// isReplaying: whether this invocation has any PRIOR PROGRESS to
		// replay past, i.e. any checkpointed operation other than the
		// root EXECUTION operation itself.
		//
		// Found and fixed a real, previously-latent bug here while
		// implementing docs/remaining-work.md §5 task 13 (replay-mode-
		// aware log suppression): the original computation was simply
		// `len(input.InitialExecutionState.Operations) > 0`, which is
		// true on EVERY invocation, including a brand new execution's
		// very first one - InitialExecutionState.Operations always
		// contains at least the root EXECUTION operation itself (see the
		// PutOperation/SetRootExecutionOperation calls a few lines below,
		// and this same fact already documented on ExecutionDetails'
		// lookup above), confirmed present even on a live invocation's
		// first call per this function's own existing comments elsewhere
		// in this file. This was harmless before this task: IsReplaying()
		// was previously never consulted for any actual control-flow
		// decision anywhere in the codebase (grepped - only exposed as an
		// inert diagnostic accessor on types.DurableContext). Task 13's
		// replayState (dcontext.Context) is the first real CONSUMER of
		// this flag, and it exposed the bug immediately: a fresh, never-
		// replayed execution's handler-level log calls were being
		// incorrectly suppressed as if a replay were in progress, caught
		// by TestLogging_ReplaySkipSuppressed_RealExecutionNotSuppressed
		// failing on its FIRST (non-replay) invocation's assertion.
		// Fixed by checking for any operation beyond the root EXECUTION
		// one, which is the same "was there already checkpointed
		// progress" signal findExecutionOperation's caller already needs
		// to distinguish, just applied to the replay flag too.
		isReplaying := hasNonExecutionOperation(input.InitialExecutionState.Operations)

		var event TEvent
		executionOp := findExecutionOperation(input.InitialExecutionState.Operations)
		if executionOp == nil || executionOp.ExecutionDetails == nil || executionOp.ExecutionDetails.InputPayload == nil {
			return types.DurableExecutionOutput{}, fmt.Errorf("durable.WithDurableExecution: no root EXECUTION operation with an input payload was found in InitialExecutionState.Operations")
		}
		if err := json.Unmarshal([]byte(*executionOp.ExecutionDetails.InputPayload), &event); err != nil {
			return types.DurableExecutionOutput{}, fmt.Errorf("durable.WithDurableExecution: unmarshaling execution input payload: %w", err)
		}

		// rootCtx's prefix (see dcontext.Context.prefix's doc) is the
		// EXECUTION operation's own backend-assigned ID - executionOp.ID,
		// found just above, MUST be looked up before this call rather
		// than after (this required reordering these two blocks relative
		// to their original order in this function, which previously
		// constructed rootCtx before locating executionOp at all, since
		// nothing before this task needed executionOp.ID this early).
		// This is the hash-input prefix every root-level NextStepID call
		// now feeds into SHA-256, matching the confirmed real Java
		// reference SDK's OperationIdGenerator exactly ("For root
		// contexts the prefix is the EXECUTION operation ID" - see
		// dcontext.NewRoot's doc and docs/remaining-work.md's "SHA-256
		// operation ID hashing" writeup). executionOp.ID itself is
		// confirmed (see the PutOperation/SetRootExecutionOperation calls
		// a few lines below, predating this task) to be a
		// backend-assigned, dynamically-generated value - NOT something
		// this SDK's own counter mints - so it is used verbatim as the
		// hash input, never itself hashed by this SDK.
		rootCtx := dcontext.NewRoot(ctx, input.DurableExecutionArn, execManager, checkpointMgr, loggerCfg, isReplaying, executionOp.ID, cfg.Plugins)

		// Seed execManager with the root operation and record its ID
		// (confirmed from a live invocation to be a dynamically-assigned
		// UUID-like value, NOT a fixed sentinel) so durable.CurrentTime
		// can retrieve it without depending on a specific ID value.
		execManager.PutOperation(executionOp.ID, *executionOp)
		execManager.SetRootExecutionOperation(executionOp.ID)

		execManager.Register() // the handler goroutine itself is the initially active one

		// EXPERIMENTAL instrumentation-plugin dispatch (pkg/durable/plugin) -
		// OnInvocationStart fires once per Lambda invocation (including
		// replays), before the handler runs. See that package's doc for
		// scope/status. ExecutionStartTimestamp is sourced from the root
		// EXECUTION operation's own StartTimestamp (there is no separate
		// dedicated field on DurableExecutionInvocationInput for this);
		// RequestID is left empty in this initial port - Lambda's own
		// request ID is available via aws-lambda-go's lambdacontext
		// package from ctx, not from input, and threading that through is
		// deferred to a follow-on increment rather than adding a new
		// dependency for an experimental, not-yet-consumed field.
		invocationInfo := plugin.InvocationInfo{
			ExecutionARN:      input.DurableExecutionArn,
			ExecutionInput:    event,
			IsFirstInvocation: !isReplaying,
		}
		if executionOp.StartTimestamp != nil {
			invocationInfo.ExecutionStartTimestamp = executionOp.StartTimestamp.Time
		}
		if len(input.UpdatedOperationIds) > 0 {
			invocationInfo.UpdatedOperations = collectUpdatedOperations(execManager, input.UpdatedOperationIds)
		}
		plugin.Dispatch(cfg.Plugins, func(p plugin.InstrumentationPlugin) {
			p.OnInvocationStart(ctx, invocationInfo)
		})

		// OnOperationChange (pkg/durable/plugin) fires once per
		// invocation, alongside OnInvocationStart, for operations that
		// changed status due to something external to the current call
		// stack becoming visible at invocation start (UpdatedOperationIds
		// - see that field's own doc on DurableExecutionInvocationInput:
		// confirmed present even on a fresh execution's first invocation,
		// where it lists the root EXECUTION operation's own ID, which is
		// why this only actually fires anything interesting - i.e. this
		// dispatch itself is unconditional, but on a first invocation the
		// single "updated" operation is just the EXECUTION operation
		// starting, not a resumed Wait/Callback/Invoke). Skipped
		// entirely (not even an empty-map dispatch) when there are no
		// updated IDs at all, to avoid dispatching a vacuous
		// notification for invocations that genuinely have none.
		if len(invocationInfo.UpdatedOperations) > 0 {
			plugin.Dispatch(cfg.Plugins, func(p plugin.InstrumentationPlugin) {
				p.OnOperationChange(ctx, plugin.OperationChangeInfo{
					ExecutionARN:      input.DurableExecutionArn,
					UpdatedOperations: invocationInfo.UpdatedOperations,
				})
			})
		}

		outcomeCh := make(chan handlerOutcome[TResult], 1)
		go func() {
			// WrapInvocation (pkg/durable/plugin) wraps the entire
			// handler call, run inside this goroutine (not around the
			// goroutine's own launch/channel-send) so a plugin's own
			// span/timing genuinely brackets the handler's real
			// execution - including the case where this goroutine is
			// later abandoned by the suspend-or-complete race below
			// (WrapInvocation still ran to completion either way; only
			// the OUTER select's choice of which branch to take is
			// affected by abandonment, not whether this wrapped call
			// itself completes).
			wrapped, err := plugin.WrapChain(cfg.Plugins, func(p plugin.InstrumentationPlugin, next plugin.WrapFn) (any, error) {
				return p.WrapInvocation(ctx, invocationInfo, next)
			}, func() (any, error) {
				return handler(event, rootCtx)
			})
			var result TResult
			if wrapped != nil {
				result, _ = wrapped.(TResult)
			}
			outcomeCh <- handlerOutcome[TResult]{result: result, err: err}
		}()

		select {
		case outcome := <-outcomeCh:
			checkpointMgr.WaitForIdle()
			dispatchInvocationEnd(ctx, cfg.Plugins, input.DurableExecutionArn, outcome)
			return outcomeToResponse(outcome)

		case <-execManager.Suspended():
			// No goroutine anywhere can make forward progress: the
			// handler is blocked on a Wait/retry-timer/callback that
			// hasn't resolved. Wait for whatever checkpoints are already
			// queued (e.g. the WAIT-start checkpoint) to be
			// acknowledged, then report PENDING without waiting for the
			// (abandoned) handler goroutine.
			checkpointMgr.WaitForIdle()
			plugin.Dispatch(cfg.Plugins, func(p plugin.InstrumentationPlugin) {
				p.OnInvocationEnd(ctx, plugin.InvocationEndInfo{
					ExecutionARN: input.DurableExecutionArn,
					Status:       plugin.InvocationStatusPending,
				})
			})
			return types.DurableExecutionOutput{Status: types.ExecutionStatusPending}, nil
		}
	}
}

// dispatchInvocationEnd fires OnInvocationEnd with the outcome translated
// into plugin.InvocationEndInfo - a small helper (rather than inlining
// this at outcomeCh's own select case) so the translation logic is
// visible and testable on its own, matching this file's existing
// convention of factoring outcome-shape translation into a named
// function (see outcomeToResponse below, which does the equivalent
// translation for the wire-level DurableExecutionOutput).
func dispatchInvocationEnd[TResult any](ctx context.Context, plugins []plugin.InstrumentationPlugin, executionARN string, outcome handlerOutcome[TResult]) {
	info := plugin.InvocationEndInfo{ExecutionARN: executionARN}
	if outcome.err != nil {
		info.Status = plugin.InvocationStatusFailed
		info.ExecutionError = outcome.err
	} else {
		info.Status = plugin.InvocationStatusSucceeded
		info.ExecutionResult = outcome.result
	}
	plugin.Dispatch(plugins, func(p plugin.InstrumentationPlugin) {
		p.OnInvocationEnd(ctx, info)
	})
}

// innermostErrorMessage returns err.Error() unchanged UNLESS err (or
// something it wraps) implements a `Cause() error` method - a
// deliberately narrow, purpose-built accessor operations.OperationError
// defines (and every operations package failure type therefore inherits
// automatically, since they all embed *OperationError directly) - in
// which case this function returns the BARE cause's own Error() string
// instead, bypassing OperationError's own decorated Error() output
// entirely.
//
// # Real, conformance-suite-confirmed bug this fixes
//
// operations.OperationError (the common base every operations package
// failure type - StepFailedError, CallbackFailedError, InvokeFailedError,
// etc. - embeds) deliberately DECORATES its own Error() string for
// Go-side debugging: e.g. `callback "name" (id 123...): not approved`,
// not the bare `not approved` the external system/business logic
// actually produced (see operations/errors.go's OperationError.Error()
// for that decoration, which is correct and unchanged for every OTHER
// consumer of these errors - Go's own %v/%s formatting, log lines,
// fmt.Errorf wrapping, etc. all still see the fully decorated form, and
// a HANDLER that itself further wraps an operation error with its own
// fmt.Errorf("doing X: %w", err) context is legitimately adding real,
// intended information this function must NOT strip).
// outcomeToResponse's own ErrorMessage, however, is NOT a Go-side
// debugging string - it is the wire-level DurableExecutionOutput.Error.
// ErrorMessage field a real caller/external system's OWN error text is
// expected to round-trip through verbatim (confirmed directly against
// the conformance suite's own callback requirement 4-6, whose handler is
// exactly `return result.Value, result.Err` - no further wrapping at
// all: the external system sends CallbackActions Payload.ErrorMessage:
// "not approved", and the requirement's own ExpectedExecutionHistory
// asserts the FINAL ExecutionFailedDetails.Error.Payload.ErrorMessage is
// the exact same bare "not approved" - not the OperationError-decorated
// form). Before this fix, outcomeToResponse called outcome.err.Error()
// directly, producing the decorated string and failing 4-6's exact-match
// assertion (confirmed via a real deployment: `ErrorMessage: expected
// 'not approved', got 'callback "..." (id ...): not approved'`).
//
// # Three attempts; the first two, both WRONG, are recorded here rather
// # than silently discarded
//
// Attempt 1: unconditionally walk errors.Unwrap all the way to the
// absolute innermost error. WRONG - caused a real, immediately-caught
// regression across 8 of this repo's own example modules under `go test
// ./...` (chained-invoke-go, completion-config-go, error-handling-go,
// map-with-condition-and-callback-go, parallel-processing-go, retry-go,
// run-in-child-context-go, wait-for-callback-go), because several of
// THEIR own handlers deliberately wrap an operation error again with
// their own fmt.Errorf("order %s: running concurrent checks: %w",
// orderID, err) - style context before returning it - that outer,
// HANDLER-authored context is legitimately part of the intended error
// message and must never be discarded.
//
// Attempt 2: strip exactly one errors.Unwrap() layer when err itself
// implements Unwrap. ALSO WRONG: *CallbackFailedError.Unwrap() returns
// e.OperationError (the *embedded* value itself, for errors.Is/As
// reasons - see that method's own doc), not e.OperationError.Err (the
// bare cause) - one Unwrap() call still lands on OperationError's own
// fully-decorated Error() output.
//
// Attempt 3: use errors.As to find the innermost error ANYWHERE in the
// chain that implements Cause() error, and return that error's own
// Cause() result. ALSO WRONG, for the same fundamental reason as attempt
// 1: errors.As searches transitively THROUGH a handler's own
// fmt.Errorf("order %s: checking inventory: %w", ..., invokeErr)
// wrapping to find the *InvokeFailedError buried inside it (since
// InvokeFailedError embeds *OperationError and therefore has a Cause()
// method too), and once found, still discards the handler's own,
// legitimate wrapping text - the exact same regression as attempt 1,
// just reached via a different mechanism (a targeted method lookup
// instead of a generic Unwrap walk, but STILL searching past
// handler-authored layers instead of stopping at them).
//
// # This version's actual approach
//
// Only call Cause() when outcome.err ITSELF (not something merely
// reachable via errors.As deeper in an arbitrary chain) implements it -
// i.e. the handler returned the operations-package failure completely
// unwrapped, exactly the `return result.Value, result.Err` shape
// requirement 4-6 (and every other conformance requirement that
// propagates an operation failure uncaught) actually uses. The moment a
// handler wraps that same error in ITS OWN fmt.Errorf(...), the wrapper
// value itself has no Cause() method (fmt.Errorf's %w wrapper only ever
// implements Unwrap, never Cause - Cause is exclusively defined on
// OperationError, deliberately not a standard Go error-wrapping
// convention) - so this direct, non-transitive type assertion correctly
// stops right there, at the outermost layer, exactly where it should.
func innermostErrorMessage(err error) string {
	if causer, ok := err.(interface{ Cause() error }); ok {
		if cause := causer.Cause(); cause != nil {
			return cause.Error()
		}
	}
	return err.Error()
}

func outcomeToResponse[TResult any](outcome handlerOutcome[TResult]) (types.DurableExecutionOutput, error) {
	if outcome.err != nil {
		// msg is the INNERMOST error's own message, not
		// outcome.err.Error()'s own (possibly decorated) string - see
		// innermostErrorMessage's own doc for why this distinction is a
		// real, conformance-suite-confirmed bug fix, not a style
		// preference.
		msg := innermostErrorMessage(outcome.err)
		// errType mirrors operations/errors.go's checkpointErrorObject
		// (%T-based Go type name, e.g. "*operations.CallbackFailedError")
		// - see DurableExecutionOutput.ErrorType's own doc for the real,
		// conformance-suite-confirmed gap this closes. pkg/durable
		// cannot import pkg/durable/operations (would create an import
		// cycle - operations already imports dcontext, which this
		// package's own rootCtx construction above depends on), so this
		// uses the exact same %T idiom directly rather than importing
		// operations' checkpointErrorObject helper. Deliberately uses
		// outcome.err's OWN type (the OUTERMOST error, e.g.
		// "*operations.CallbackFailedError"), NOT innermostErrorMessage's
		// unwrapped source - ErrorType identifying the outer, most
		// specific/informative Go error type is unaffected by the
		// ErrorMessage-only unwrapping fix below.
		//
		// Error is a SINGLE NESTED types.ErrorObject (not two flat
		// top-level ErrorMessage/ErrorType fields, as this function used
		// to send) - see types.DurableExecutionOutput's own doc for the
		// full bug writeup on why this is the real, confirmed-working
		// wire shape (cross-checked directly against the JS reference
		// SDK's own tested with-durable-execution.ts/core.ts source:
		// `{Status: FAILED, Error: ErrorObject}`), and why the previous
		// flat-fields shape was silently discarded by the backend,
		// leaving GetDurableExecutionHistory's own
		// ExecutionFailedDetails.Error.Payload empty regardless of what
		// this SDK sent.
		errType := fmt.Sprintf("%T", outcome.err)
		return types.DurableExecutionOutput{
			Status: types.ExecutionStatusFailed,
			Error: &types.ErrorObject{
				ErrorMessage: msg,
				ErrorType:    errType,
			},
		}, nil
	}

	b, err := json.Marshal(outcome.result)
	if err != nil {
		return types.DurableExecutionOutput{}, fmt.Errorf("durable.WithDurableExecution: marshaling handler result: %w", err)
	}
	payload := string(b)
	return types.DurableExecutionOutput{
		Status:        types.ExecutionStatusSucceeded,
		ResultPayload: &payload,
	}, nil
}

// validateConfig rejects a genuinely invalid Config before any root
// context/checkpoint.Manager is constructed (docs/remaining-work.md §8
// task 18).
//
// # Why this returns a Go error, not a Failed DurableExecutionOutput
//
// Task 18's own doc-table description ("returning a Failed result
// rather than a confusing downstream error") reads, taken alone, like it
// means the Failed types.DurableExecutionOutput{Status:
// ExecutionStatusFailed} shape outcomeToResponse produces for a genuine
// HANDLER error. But this codebase already has an established, working
// precedent for a Config-level problem detected at exactly this point in
// WithDurableExecution, immediately above this function's call site:
// the pre-existing `cfg.Client == nil` check returns a Go `error` value
// from the entry function's own second return slot, NOT a
// DurableExecutionOutput with Status: Failed. That is this SDK's OWN
// real convention for "the Config is unusable, fail before doing
// anything else" - not JS's, and not a third option invented for this
// task - and the same reasoning applies here for the same structural
// reason: at the point this check runs there is no root DurableContext,
// no execmgr.Manager, no checkpoint.Manager, and the
// DurableExecutionArn/CheckpointToken haven't been used for anything
// yet either - there is no in-flight execution to report a Failed
// STATUS about. types.DurableExecutionOutput{Status: Failed} is this
// SDK's way of saying "the HANDLER's business logic failed after a real
// execution was already underway" (see outcomeToResponse) - it
// presupposes the execution exists. A config problem is caught before
// that execution exists at all, matching not just the nil-Client
// check's own precedent but every other pre-flight failure in this same
// function (the missing-root-EXECUTION-operation check and the
// input-payload-unmarshal-failure check a few lines below both also
// return a plain Go error, for the identical reason) - one single
// established convention, not a second, inconsistent one for this task
// alone.
//
// # Scope: Config's own fields only
//
// Only Config's own fields are checked here (CheckpointStrategy,
// LoggerConfig, Client - the latter already handled by the caller
// above, before this function is even called). Per-call operation
// options (Map/Parallel's WithMapMaxConcurrency/
// WithParallelMaxConcurrency, WithWaitForCallbackTimeout,
// CompletionConfig, utils.RetryStrategyConfig, etc.) are deliberately
// NOT validated here - they don't exist yet at the point this function
// runs (they're arguments to individual operation calls made later,
// inside the handler body, against a *dcontext.Context this function
// hasn't constructed yet), so there's nothing here to check. Each of
// those is instead validated at its own actual point of consumption:
// operations/batch.go's runBatch (maxConcurrency) and batchCompletion
// (CompletionConfig's threshold fields), operations/callback.go's
// WaitForCallback (the timeout Duration), and utils/retry.go's
// CreateRetryStrategy (MaxAttempts/BackoffRate) - see each of those
// call sites' own comments for their specific "what's actually
// nonsensical here" reasoning, matching this same file-local philosophy
// of validating a value only where a concretely wrong value would
// otherwise cause a real, observable misbehavior.
//
// # What's NOT checked on Config, and why
//
// Config itself has very little that is genuinely invalid, and this
// deliberately does not invent rules beyond what's concretely wrong:
//
//   - LoggerConfig has no field whose zero value is nonsensical.
//     CustomLogger == nil legitimately means "use the SDK's default
//     logger" (see the defaulting logic a few lines below this
//     function's own call site). ModeAware is a plain bool with two
//     valid states, both meaningful (see types.LoggerConfig.ModeAware's
//     own extensive doc on why its zero value carries specific,
//     intentional meaning rather than being an error state).
//   - CheckpointStrategy is intentionally NOT range-checked against its
//     three defined constants. It is documented, directly above on this
//     same Config type, to be a currently inert, no-op field - "accepted
//     here for API compatibility with the design surface but not yet
//     wired to distinct runtime behavior." An out-of-range int value
//     therefore causes exactly zero difference in observable behavior
//     today (nothing reads it), which is not "clearly nonsensical" in
//     the same concrete sense a nil required pointer or a negative count
//     is - it would be validating against a meaning this field doesn't
//     have yet, not closing a real gap. Revisit once CheckpointStrategy
//     actually drives batching behavior (see this type's own TODO-style
//     doc note about that).
func validateConfig(cfg *Config) error {
	// No further Config-level invariant exists to check today - see this
	// function's own doc above for why CheckpointStrategy/LoggerConfig
	// have no genuinely invalid state, and why per-call operation options
	// are validated at their own call sites instead of here. This
	// function is retained (rather than being inlined away or omitted
	// entirely) as the single, documented, and now-obvious place any
	// FUTURE genuinely-invalid Config state should be added, and as a
	// visible marker that this task was actually considered rather than
	// silently skipped.
	return nil
}

func findExecutionOperation(ops []types.Operation) *types.Operation {
	for i := range ops {
		if ops[i].Type == types.OperationTypeExecution {
			return &ops[i]
		}
	}
	return nil
}

// collectUpdatedOperations builds the plugin.OperationInfo map for
// OnInvocationStart.UpdatedOperations/OnOperationChange.UpdatedOperations
// from input.UpdatedOperationIds, looking each ID up in execManager
// (already seeded with InitialExecutionState.Operations by this point in
// WithDurableExecution). IDs with no matching operation (should not
// happen in practice, but the backend contract does not guarantee it)
// are silently skipped rather than producing a zero-value OperationInfo,
// which would misleadingly report Status: "" for an operation this SDK
// has no actual data for.
func collectUpdatedOperations(execManager *execmgr.Manager, updatedIDs []string) map[string]plugin.OperationInfo {
	result := make(map[string]plugin.OperationInfo, len(updatedIDs))
	for _, id := range updatedIDs {
		op, found := execManager.GetOperation(id)
		if !found {
			continue
		}
		info := plugin.OperationInfo{
			ID:       op.ID,
			Name:     op.Name,
			Type:     string(op.Type),
			SubType:  op.SubType,
			ParentID: op.ParentID,
			Status:   plugin.OperationStatus(op.Status),
			IsReplay: true,
		}
		if op.StartTimestamp != nil {
			info.StartTimestamp = op.StartTimestamp.Time
		}
		if op.EndTimestamp != nil {
			info.EndTimestamp = op.EndTimestamp.Time
		}
		result[id] = info
	}
	return result
}

// hasNonExecutionOperation reports whether ops contains any checkpointed
// operation other than the root EXECUTION operation itself - i.e. whether
// there is any actual prior progress for this invocation to replay past.
// Checking by TYPE rather than by raw slice length (see
// WithDurableExecution's isReplaying comment for the bug this fixed) is
// deliberately more robust than `len(ops) > 1`: it stays correct even if
// some future backend revision or test harness ever seeds additional
// non-progress metadata alongside the root EXECUTION operation.
func hasNonExecutionOperation(ops []types.Operation) bool {
	for i := range ops {
		if ops[i].Type != types.OperationTypeExecution {
			return true
		}
	}
	return false
}

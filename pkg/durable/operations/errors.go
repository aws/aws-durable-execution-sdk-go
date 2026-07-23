package operations

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// errSuspended is returned up the call stack (never to the top-level
// handler's actual return value; see durable.WithDurableExecution) when an
// operation's WaitForOperation call reports the execution suspended before
// the operation completed. It signals "abandon this goroutine, the
// invocation is ending as PENDING" rather than a real failure.
//
// Mirrors Java's SuspendExecutionException, but as a plain sentinel error
// rather than an exception-like control-flow mechanism, per Go idiom (see
// docs/checkpoint-replay-design.md §4 comparison table).
var errSuspended = fmt.Errorf("durable execution suspended")

// errNotImplemented marks scaffold operations pending further porting work.
// See docs/checkpoint-replay-design.md for the tracked implementation plan;
// operations not yet listed as done there still return this.
func errNotImplemented(op string) error {
	return fmt.Errorf("%s: not yet implemented (scaffold)", op)
}

// checkpointErrorObject builds the types.OperationError (a types.ErrorObject
// alias - see wire.go) checkpointed for a failed operation, populating BOTH
// ErrorMessage and ErrorType from err.
//
// # Bug fixed here
//
// Every call site that constructs a types.OperationError to checkpoint a
// failure (Step's retryOrFail, Invoke's failure path, WaitForCondition's
// failure path, and Map/Parallel's batch item failure path in batch.go) used
// to write the literal struct `&types.OperationError{ErrorMessage:
// err.Error()}` directly, leaving ErrorType permanently unset (its Go zero
// value, ""). Because wire.go's ErrorObject.ErrorType field is tagged
// `json:"ErrorType,omitempty"`, an empty ErrorType is omitted from the
// checkpoint payload entirely - not merely empty-stringed, but the KEY
// ITSELF absent from the JSON object - confirmed via a real deployed
// conformance-harness invocation (requirement 1-19's
// StepFailedDetails.Error.Payload came back as exactly
// `{"ErrorMessage": "..."}`, no ErrorType key at all) and by grepping every
// other operation kind in this package, all of which shared the exact same
// omission (not a Step-specific bug). This contradicts the conformance
// suite's own universal expectation (every *-suite requirement whose step
// fails expects StepFailedDetails.Error.Payload.ErrorType present, usually
// matched via a wildcard '*' or a type-name regex like
// `${/.*TransientError$/}` for 1-15/1-16) and the cross-SDK convention (the
// JS/Java/Python reference SDKs all populate ErrorType with the thrown
// exception's class/constructor name).
//
// Go has no runtime "class name" concept for a bare error interface value,
// but the idiomatic equivalent - and the one every other %T-based
// diagnostic in this codebase already uses (see e.g. invoke.go/step.go's
// own "converting checkpointed result to %T" messages) - is the error
// value's own concrete Go type name via fmt.Sprintf("%T", err), e.g.
// "*errors.errorString" for a plain errors.New(...) value, or
// "*handlers.TransientError" for a custom named error type (matching this
// conformance harness's own step_1_15.go/step_1_16.go TransientError type,
// whose type NAME - not its package-qualified form - is what
// `${/.*TransientError$/}`-style requirement assertions match against).
func checkpointErrorObject(err error) *types.OperationError {
	if err == nil {
		return nil
	}
	return &types.OperationError{
		ErrorMessage: err.Error(),
		ErrorType:    fmt.Sprintf("%T", err),
	}
}

// -----------------------------------------------------------------------
// Structured error hierarchy (docs/remaining-work.md §4 task 10)
// -----------------------------------------------------------------------
//
// # Design
//
// The JS SDK models its error hierarchy as an actual class hierarchy under
// its errors/ package (DurableOperationError as the common base, with
// StepError/CallbackError/etc. as subclasses, plus separately-classified
// non-deterministic-error and serdes-errors modules). Java does the same
// with DurableExecutionException as an abstract base and ~20 concrete
// subtypes (per docs/remaining-work.md §4).
//
// Go has no exception/class hierarchy, so this package ports the SAME
// conceptual model - one common identifiable "this came from a durable
// operation" base, plus specific typed errors for each operation family -
// using the idiomatic Go mechanism instead: every concrete error type here
// both (a) implements the error interface directly and (b) embeds
// *OperationError, so:
//
//   - `errors.As(err, &durableErr)` recovers the common base (operation ID,
//     name, kind) from ANY of these types, mirroring what a caller would
//     get from catching the JS/Java base class.
//   - `errors.As(err, &stepErr)` (or &CallbackFailureError, etc.) recovers
//     the SPECIFIC type when the caller cares about the distinction JS's
//     StepError/CallbackError/etc. subclasses (or Java's concrete
//     DurableExecutionException subtypes) provide.
//   - `errors.Is`/`errors.As` both work through fmt.Errorf's `%w` wrapping
//     at every call site that further wraps one of these (e.g. Step's
//     "step %q (id %s) failed after %d attempt(s): %w"), since Go's
//     errors.As/Is walk the full Unwrap() chain regardless of how many
//     %w layers sit on top of the originating structured error.
//
// This intentionally does NOT introduce a Go "interface{ isDurableError()
// }" marker interface as the primary discriminant - embedding a
// concrete *OperationError struct and relying on errors.As's normal
// type-matching semantics is simpler, requires no sentinel methods, and is
// the standard idiom recommended by the Go standard library's own errors
// package documentation for exactly this "family of related error types"
// use case.
//
// # Coverage (matching the task list)
//
//   - StepFailedError       - a Step exhausted its retry strategy (step.go)
//   - CallbackFailedError    - a callback was explicitly failed by the
//     external system, or its checkpointed operation ended in a
//     non-success terminal state (callback.go)
//   - InvokeFailedError      - a chained Invoke's target function failed,
//     timed out, or was stopped (invoke.go)
//   - ChildContextFailedError - RunInChildContext's fn returned an error
//     (invoke.go)
//   - BatchItemFailedError / BatchFailedError - a single Map
//     iteration/Parallel branch failed, or the overall batch failed once
//     aggregated (batch.go) - BatchFailedError replaces the old bare
//     *AggregateError as the batch's terminal type; AggregateError is
//     retained as the concrete list-of-errors payload embedded inside it
//     (see batch.go) rather than removed, since it is already part of
//     this package's public API and callers may already be type-asserting
//     it directly.
//   - ConditionFailedError   - WaitForCondition exhausted its retry
//     strategy without the condition ever being met (wait_for_condition.go)
//   - NonDeterministicReplayError - a checkpointed operation's Type/SubType
//     does not match what the calling operation expected during replay
//     (task 11 - see replay_check.go)
//   - SerdesError            - a Serdes.Serialize/Deserialize call failed
//     for any operation's input/result/state (used across step.go,
//     callback.go, invoke.go, batch.go, wait_for_condition.go)
//   - ResultTooLargeError    - a single operation's serialized result
//     exceeds the checkpoint payload threshold (docs/remaining-work.md §6
//     task 16 - see that block's doc, further down this file, for the
//     real-backend research this is based on and why it is a conservative
//     fallback rather than an automatic offload)
//
// Each type documents, in its own doc comment, which operation(s) produce
// it and what the caller can inspect on it.

// OperationKind identifies which durable operation family produced a
// structured error, for callers that want to branch on it without a full
// type switch (e.g. logging/metrics). Mirrors the OperationType values
// used on the wire (types.OperationType) but is deliberately a distinct
// type: some kinds here (ChildContext, BatchItem, Batch, Condition) map to
// the same wire OperationType (CONTEXT, STEP) under different SubTypes, so
// collapsing this onto types.OperationType directly would lose the
// distinction that actually matters to a caller of this package.
type OperationKind string

const (
	OperationKindStep           OperationKind = "STEP"
	OperationKindWait           OperationKind = "WAIT"
	OperationKindCallback       OperationKind = "CALLBACK"
	OperationKindInvoke         OperationKind = "INVOKE"
	OperationKindChildContext   OperationKind = "CHILD_CONTEXT"
	OperationKindBatchItem      OperationKind = "BATCH_ITEM"
	OperationKindBatch          OperationKind = "BATCH"
	OperationKindCondition      OperationKind = "WAIT_FOR_CONDITION"
	OperationKindNonDeterminism OperationKind = "NON_DETERMINISTIC_REPLAY"
	OperationKindSerdes         OperationKind = "SERDES"
)

// Wire SubType values checkpointed on types.OperationUpdate/read back on
// types.Operation - NOT to be confused with OperationKind above (an
// internal, error-classification-only concept with its own, differently-
// cased string values like "CHILD_CONTEXT"). These are the literal,
// real-backend-echoed-verbatim SubType field values, confirmed against
// the AWS Durable Execution Conformance Test Suite
// (github.com/aws/aws-durable-execution-conformance-tests,
// test-requirements/*/*.yaml's own ExpectedExecutionHistory blocks -
// every single one of that suite's SubType expectations uses exactly
// this PascalCase convention: Step, Wait, Callback, RunInChildContext,
// Map, MapIteration, Parallel, ParallelBranch, WaitForCondition,
// WaitForCallback) and by directly re-invoking the real, already-
// deployed Java reference SDK's simple-step-example and inspecting its
// own real GetDurableExecutionHistory response.
//
// Two real, scoped bugs this constant block's introduction fixes (see
// each constant's own call site for the specific before/after):
//  1. Plain Step, Wait, and CreateCallback/Callback never set SubType at
//     all (sent empty/omitted) - the conformance suite expects an
//     explicit Step/Wait/Callback literal for these base cases, not an
//     absent field. Confirmed via a real deployed conformance-harness
//     invocation: GetDurableExecutionHistory returned StepStarted/
//     StepSucceeded events with NO SubType key present whatsoever,
//     contradicting the suite's universal expectation.
//  2. Every place this package DID already set a SubType used
//     SCREAMING_SNAKE_CASE (WAIT_FOR_CONDITION, RUN_IN_CHILD_CONTEXT,
//     MAP, MAP_ITERATION, PARALLEL, PARALLEL_BRANCH) - confirmed, via a
//     real GetDurableExecutionHistory call against an already-deployed
//     example (map-with-condition-and-callback-go-example), that the
//     real backend echoes back EXACTLY whatever string this SDK sends,
//     verbatim, with no normalization - so the wrong casing was
//     entirely this SDK's own doing, not a backend quirk, and every
//     other SDK (confirmed for Java) uses the PascalCase form the
//     conformance suite itself expects.
const (
	subTypeStep              = "Step"
	subTypeWait              = "Wait"
	subTypeCallback          = "Callback"
	subTypeRunInChildContext = "RunInChildContext"
	subTypeMap               = "Map"
	subTypeMapIteration      = "MapIteration"
	subTypeParallel          = "Parallel"
	subTypeParallelBranch    = "ParallelBranch"
	subTypeWaitForCondition  = "WaitForCondition"
	subTypeWaitForCallback   = "WaitForCallback"
)

// OperationError is the common base embedded by every structured error
// type in this package - the Go analog of the JS SDK's
// DurableOperationError / Java's DurableExecutionException base class
// (see this file's top-level doc for the full design rationale).
//
// Callers that only care "did SOME durable operation fail, and which
// one/what was it called" (without needing the operation-specific detail
// a subtype like StepFailedError carries) can recover just this via:
//
//	var opErr *operations.OperationError
//	if errors.As(err, &opErr) {
//		log.Printf("operation %s (id=%s, kind=%s) failed", opErr.Name, opErr.ID, opErr.Kind)
//	}
//
// OperationError itself also implements error (Error() delegates to the
// wrapped Err, or a generic message if Err is nil), so it is valid to
// return *OperationError directly, though in practice every operation in
// this package returns one of the more specific subtypes below, which all
// embed *OperationError and therefore satisfy this same errors.As check.
type OperationError struct {
	// Kind identifies which operation family produced this error.
	Kind OperationKind
	// ID is the checkpointed operation's step ID (e.g. "1-3").
	ID string
	// Name is the caller-supplied name passed to the operation (Step's
	// id parameter, Map/Parallel's id, etc.) - distinct from ID, which is
	// the SDK-internal hierarchical step ID, not a human-chosen name.
	Name string
	// Err is the underlying cause, if any (e.g. the error a Step's fn
	// returned, or the external system's callback-failure message).
	// May be nil for errors that are self-describing (e.g. a timeout).
	Err error
}

func (e *OperationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	base := fmt.Sprintf("%s %q (id %s)", kindLabel(e.Kind), e.Name, e.ID)
	if e.Err != nil {
		return base + ": " + e.Err.Error()
	}
	return base
}

// Unwrap exposes the underlying cause to errors.Is/errors.As chains, so
// e.g. errors.Is(err, someSentinel) still finds a sentinel wrapped deep
// inside a StepFailedError's Err chain.
func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Cause returns e.Err directly - the SAME value Unwrap() returns for a
// bare *OperationError, but INTENTIONALLY DIFFERENT from what a
// subtype's own Unwrap() override returns (see e.g.
// *CallbackFailedError.Unwrap(), which deliberately returns
// e.OperationError itself, not e.OperationError.Err, so that
// errors.As(err, &opErr) can still find the intermediate *OperationError
// value in the chain - a real, intentional design choice for errors.Is/
// As semantics, not a bug). Cause exists as a SEPARATE, purpose-built
// accessor precisely because that Unwrap() indirection makes generic
// errors.Unwrap-based message extraction produce the WRONG (still
// fully-decorated) string for e.g. *CallbackFailedError - see
// pkg/durable/durable.go's innermostErrorMessage for the real,
// conformance-suite-confirmed bug this was added to fix, and that
// function's own doc for the two prior, both-WRONG approaches
// (generically unwrapping all the way down, and generically unwrapping
// exactly once) that this named, unambiguous accessor replaces. Every
// operations package failure type embeds *OperationError directly (see
// this file's own "Design" doc above), so Cause() is automatically
// promoted and available on every one of them (StepFailedError,
// CallbackFailedError, InvokeFailedError, ConditionFailedError,
// ChildContextFailedError, BatchFailedError, etc.) with no per-type
// override needed - only OperationError itself defines it, exactly once.
func (e *OperationError) Cause() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func kindLabel(k OperationKind) string {
	switch k {
	case OperationKindStep:
		return "step"
	case OperationKindWait:
		return "wait"
	case OperationKindCallback:
		return "callback"
	case OperationKindInvoke:
		return "invoke"
	case OperationKindChildContext:
		return "child context"
	case OperationKindBatchItem:
		return "batch item"
	case OperationKindBatch:
		return "batch"
	case OperationKindCondition:
		return "waitForCondition"
	case OperationKindNonDeterminism:
		return "replay"
	case OperationKindSerdes:
		return "serdes"
	default:
		return "operation"
	}
}

// StepFailedError is returned when a Step exhausts its configured retry
// strategy (or fails outright under a no-retry strategy) - the terminal
// failure path in step.go's retryOrFail. Attempt records how many times
// the step body actually ran before giving up, mirroring what the JS/Java
// SDKs' equivalent StepError/StepExecutionException expose.
type StepFailedError struct {
	*OperationError
	// Attempt is the final attempt number (1-based) that failed.
	Attempt int
}

// Unwrap exposes the embedded *OperationError base itself (not just its
// own Err) to errors.As, so `errors.As(err, &opErr)` (recovering the
// common base - see OperationError's doc) works uniformly across every
// concrete type in this file. Without this override, Go's embedding
// promotes OperationError.Unwrap() directly onto StepFailedError, which
// returns OperationError.Err (the underlying cause) instead of the base
// itself - meaning errors.As(err, &opErr) would skip straight past the
// base to its cause and never match *OperationError at all. Every other
// concrete error type below needs the identical override for the same
// reason.
func (e *StepFailedError) Unwrap() error { return e.OperationError }

// CallbackFailedError is returned when a CALLBACK operation ends in a
// non-success terminal state: the external system explicitly called the
// SendDurableExecutionCallbackFailure-equivalent API (see
// callback.go's callbackError), or the callback timed out waiting for a
// response (either CreateCallback's own WithCallbackTimeout/
// WithCallbackHeartbeatTimeout options, or WaitForCallback's still-
// unenforced-client-side WithWaitForCallbackTimeout - see that option's
// own doc). Timeout records which case this is.
type CallbackFailedError struct {
	*OperationError
	// Timeout is true if this failure represents the callback timing out
	// (either the overall CallbackOptions.TimeoutSeconds or the
	// HeartbeatTimeoutSeconds variant - see callbackError's doc for how
	// the two are distinguished only via CallbackDetails.Error.Payload.
	// ErrorType's string value, not a separate flag on the wire) rather
	// than being explicitly failed by the external system. Set correctly
	// as of this callback suite's implementation - see callbackError's
	// doc for the specific bug (Timeout hard-coded false) this fixed.
	Timeout bool
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *CallbackFailedError) Unwrap() error { return e.OperationError }

// InvokeFailedError is returned when a chained Invoke's target function
// fails, times out, or is stopped - any CHAINED_INVOKE terminal status
// other than Succeeded (invoke.go's invokeError). Status records exactly
// which terminal outcome occurred so callers can distinguish e.g. a
// TimedOut target from a Failed one without string-matching the error
// message.
type InvokeFailedError struct {
	*OperationError
	// Status is the checkpointed operation's terminal status (Failed,
	// TimedOut, Stopped, or Cancelled - never Succeeded, since that path
	// does not produce an error at all).
	Status types.OperationStatus
	// Timeout is a convenience flag equivalent to Status ==
	// types.OperationStatusTimedOut, matching the task's "invoke
	// failure/timeout" coverage requirement with an explicit, easy-to-check
	// field rather than requiring callers to compare Status themselves.
	Timeout bool
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *InvokeFailedError) Unwrap() error { return e.OperationError }

// ChildContextFailedError is returned when RunInChildContext's fn returns
// a (non-suspension) error - the CONTEXT/RUN_IN_CHILD_CONTEXT FAIL path in
// invoke.go. Also produced, with Kind set to OperationKindBatchItem
// instead via BatchItemFailedError (see below), by a single Map
// iteration/Parallel branch's identical CONTEXT/<subType> FAIL path in
// batch.go - the two are distinguished by Kind/type since a Map item
// failure and a bare RunInChildContext failure are meaningfully different
// to a caller (one is one-of-many, the other is the whole call), even
// though the underlying checkpoint shape (a failed CONTEXT operation) is
// identical.
//
// # "Original error not reconstructable" semantics
//
// Matching Java's documented behavior for a branch/child-context error
// that can't be round-tripped (docs/remaining-work.md task 10): a
// checkpointed CONTEXT operation's error is persisted as a
// types.ErrorObject{ErrorMessage, ErrorType, ...} - a message plus a
// type-name STRING, not a serialized Go error value - so on REPLAY (as
// opposed to the same invocation that produced the failure), the original
// concrete error type is fundamentally not recoverable; only its message
// survives. Reconstructed carries this: it is true when this
// ChildContextFailedError was built by reading a checkpointed CONTEXT
// operation's Error field back on replay (Original is nil in that case,
// since there is nothing further to unwrap-to), and false when it was
// built directly from fn's actual returned error within the SAME
// invocation that ran fn (Original holds that real error, so
// errors.As/Is against it still works normally). This mirrors the
// JS/Java SDKs' own documented limitation exactly - it is not something
// this Go port could close without the backend itself persisting more
// than a message+type-name string, which contextError's doc already notes
// is the wire shape confirmed against the official API reference.
type ChildContextFailedError struct {
	*OperationError
	// Reconstructed is true if this error was rebuilt from a checkpointed
	// CONTEXT operation's persisted Error (replay path - Original is nil
	// in this case, since the real Go error value is gone) rather than
	// from fn's actual returned error within the same invocation that ran
	// it (Original is non-nil in that case). See this type's doc.
	Reconstructed bool
	// Original is fn's actual returned error, when available (see
	// Reconstructed's doc). This is ALSO what OperationError.Err holds -
	// Original is provided as a same-named, more discoverable alias
	// specifically for this type, matching how a caller reading this
	// type's fields would expect to find the "original error" mentioned
	// in its own doc comment without having to know Err is where it
	// lives on the embedded base.
	Original error
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *ChildContextFailedError) Unwrap() error { return e.OperationError }

// BatchItemFailedError is returned for a single Map iteration's or
// Parallel branch's failure - the CONTEXT/MAP_ITERATION or
// CONTEXT/PARALLEL_BRANCH FAIL path in batch.go's runBatchItem. See
// ChildContextFailedError's doc for the "original error not
// reconstructable on replay" semantics, which apply identically here
// (Reconstructed/Original have the same meaning).
type BatchItemFailedError struct {
	*OperationError
	Reconstructed bool
	Original      error
	// Index is this item's/branch's zero-based position in the input
	// items/branches slice, matching the index runBatch already threads
	// through internally - included so a caller inspecting one failed
	// item out of a BatchFailedError's collected errors can tell which
	// one without re-deriving it from error-message text.
	Index int
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *BatchItemFailedError) Unwrap() error { return e.OperationError }

// BatchFailedError is the terminal error for a Map/Parallel operation
// whose overall completion policy was not met (batch.go's finishBatch) -
// e.g. the default Promise.all-style "any failure fails the whole batch"
// policy, or a configured CompletionConfig's thresholds not being
// satisfied. Errors collects one error per failed item/branch (each
// normally a *BatchItemFailedError, but see AggregateError's own doc for
// why the collected slice is untyped []error) in the same order they
// appear in the batch's BatchResult.Items.
//
// AggregateError (this package's pre-existing collected-errors type) is
// embedded directly rather than duplicated, so existing callers doing a
// type assertion against *AggregateError (as batch.go's own Any/Race
// helpers already do internally) continue to work unchanged against a
// *BatchFailedError too, via errors.As - BatchFailedError adds the
// structured OperationError base (ID/Name/Kind) on top without breaking
// that existing shape.
type BatchFailedError struct {
	*OperationError
	*AggregateError
}

// Error prefers AggregateError's own message (which lists every
// underlying item error) over OperationError's generic
// "batch "name" (id id): <cause>" shape, since AggregateError's message is
// strictly more informative for this specific type - matching how
// finishBatch already constructed and returned a bare *AggregateError
// before this task, so existing error-message-based expectations (none of
// which assert exact text in this repo's own tests, but external callers
// might) see the same message text as before.
func (e *BatchFailedError) Error() string {
	if e.AggregateError != nil {
		return e.AggregateError.Error()
	}
	if e.OperationError != nil {
		return e.OperationError.Error()
	}
	return "batch failed"
}

// Unwrap gives errors.Is/errors.As access to BOTH embedded bases: the
// OperationError chain (ID/Name/Kind, and beyond that whatever Err holds)
// and, separately, callers doing errors.As(&aggErr) find *AggregateError
// directly since Go's errors.As already looks for an exact embedded-type
// match before needing to traverse Unwrap - this method exists so
// errors.Is/As ALSO keep working through OperationError.Err's own chain
// (e.g. a sentinel wrapped inside one of the batch's underlying causes),
// which a single-target Unwrap can't express, hence returning a slice via
// the multi-error Unwrap() []error convention (supported by errors.Is/As
// since Go 1.20).
func (e *BatchFailedError) Unwrap() []error {
	var out []error
	if e.OperationError != nil {
		out = append(out, e.OperationError)
	}
	if e.AggregateError != nil {
		out = append(out, e.AggregateError)
	}
	return out
}

// ConditionFailedError is returned when WaitForCondition's retry strategy
// gives up before checkFn ever reports ConditionMet: true - either
// because checkFn itself returned an error on its final attempt, or
// because the condition simply was never met and the strategy exhausted
// its retries (wait_for_condition.go's pollAndCheckpoint). ConditionMet is
// always false when this error is returned (that is precisely what makes
// it a failure); it is included explicitly rather than left implicit so a
// caller inspecting the error doesn't have to infer it.
type ConditionFailedError struct {
	*OperationError
	// Attempt is the final attempt (1-based) that failed / after which
	// the strategy gave up.
	Attempt int
	// CheckErr is checkFn's own returned error on the final attempt, if
	// any (nil if the final attempt returned ConditionMet: false with no
	// error - i.e. checkFn itself never failed, the condition just was
	// never satisfied before retries ran out).
	CheckErr error
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *ConditionFailedError) Unwrap() error { return e.OperationError }

// SerdesError is returned when a types.Serdes.Serialize or Deserialize
// call fails for any operation's input, result, or intermediate state -
// the Go analog of the JS SDK's serdes-errors module / Java's
// SerDesException (docs/remaining-work.md §4). Direction records which
// of the two operations failed, and EntityID is the step ID the
// (de)serialization was being performed for, matching the entityID
// parameter every types.Serdes method already takes.
type SerdesError struct {
	*OperationError
	// Direction is either "serialize" or "deserialize", matching which
	// Serdes method failed.
	Direction string
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *SerdesError) Unwrap() error { return e.OperationError }

// newSerdesError wraps a Serdes.Serialize/Deserialize failure as a
// *SerdesError, used across every operation file that calls into a
// types.Serdes (step.go, callback.go, invoke.go, batch.go,
// wait_for_condition.go) so serdes failures are UNIFORMLY structured
// regardless of which operation triggered them, rather than each call
// site inventing its own fmt.Errorf wrapping independently.
func newSerdesError(direction, entityID, name string, cause error) *SerdesError {
	return &SerdesError{
		OperationError: &OperationError{Kind: OperationKindSerdes, ID: entityID, Name: name, Err: cause},
		Direction:      direction,
	}
}

// -----------------------------------------------------------------------
// Non-deterministic replay detection (docs/remaining-work.md §4 task 11)
// -----------------------------------------------------------------------

// NonDeterministicReplayError is returned when a checkpointed operation
// encountered during replay does not match what the calling operation
// expected at that step ID - the Go analog of JS's
// validateReplayConsistency/non-deterministic-error and Java's
// NonDeterministicExecutionException (docs/remaining-work.md §4). This
// indicates the handler's code changed between deployments in a way that
// breaks the fundamental assumption every checkpoint/replay operation in
// this SDK depends on: that re-running the handler's code from the top
// issues the exact same sequence of durable operations, in the exact same
// order, with the exact same IDs, as the invocation(s) that originally
// produced the checkpointed operation log.
//
// See replay_check.go's checkReplayConsistency for where this is raised:
// every operation's runXxx function now checks the checkpointed
// operation's Type (and SubType, where the operation distinguishes
// sibling operations by SubType - e.g. WaitForCondition vs. plain Step,
// both STEP-typed) against what IT expects to find at that step ID BEFORE
// reading any type-specific *Details field out of it. Before this task,
// every operation instead read straight into e.g. existing.StepDetails
// without checking existing.Type first - so a step ID that used to be a
// STEP (StepDetails populated) but is now called as a Wait (which reads
// WaitDetails) would silently see a nil/zero-value WaitDetails and return
// an empty/wrong result rather than erroring - exactly the "silently
// misinterpreting the operation's data" bug this task closes.
type NonDeterministicReplayError struct {
	*OperationError
	// ExpectedType/ExpectedSubType is what the CALLING operation (e.g.
	// operations.Wait) expects to find checkpointed at this step ID.
	// ExpectedSubType is empty if the calling operation does not
	// distinguish by SubType (e.g. plain Step, Wait, CreateCallback,
	// Invoke, RunInChildContext all check Type alone; WaitForCondition
	// and the Map/Parallel family additionally check SubType, since they
	// share a wire Type with a sibling operation - STEP for
	// Step/WaitForCondition, CONTEXT for RunInChildContext/Map/Parallel).
	ExpectedType    types.OperationType
	ExpectedSubType string
	// ActualType/ActualSubType is what was ACTUALLY found checkpointed at
	// this step ID - i.e. what a PRIOR invocation (or a prior, differently
	// -coded deployment of this same handler) actually recorded there.
	ActualType    types.OperationType
	ActualSubType string
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *NonDeterministicReplayError) Unwrap() error { return e.OperationError }

func (e *NonDeterministicReplayError) Error() string {
	expected := describeOperationShape(e.ExpectedType, e.ExpectedSubType)
	actual := describeOperationShape(e.ActualType, e.ActualSubType)
	return fmt.Sprintf(
		"non-deterministic replay detected at operation id %s (name %q): expected %s (called by this handler's current code), but the checkpointed operation log has %s (recorded by a prior invocation) - the handler's code has changed in a way that breaks replay determinism between deployments",
		e.ID, e.Name, expected, actual,
	)
}

func describeOperationShape(t types.OperationType, subType string) string {
	if subType == "" {
		return string(t)
	}
	return fmt.Sprintf("%s (subType %s)", t, subType)
}

// checkReplayConsistency is called by every operation's runXxx function
// immediately after a successful GetOperation(id) lookup and BEFORE
// reading any type-specific *Details field off the result, to verify the
// checkpointed operation actually IS the kind of operation the caller
// expects to find there. See NonDeterministicReplayError's doc for the
// full rationale and the concrete bug this closes.
//
// expectedSubType may be "" for operations that don't distinguish by
// SubType (see NonDeterministicReplayError.ExpectedSubType's doc) - in
// that case only existing.Type is checked, and existing.SubType is
// ignored entirely (NOT required to also be empty), since some operations
// that don't care about SubType share a wire Type with ones that do (a
// plain Step's checkpointed STEP operation has SubType=="", while
// WaitForCondition's has SubType=="WAIT_FOR_CONDITION" - both are valid,
// self-consistent STEP operations from Step's own point of view, which
// never looks at SubType at all).
//
// kind/id/name are used only to populate the returned error's
// OperationError base if a mismatch is found; they do not affect the
// comparison itself.
func checkReplayConsistency(existing types.Operation, expectedType types.OperationType, expectedSubType string, kind OperationKind, id, name string) error {
	if existing.Type != expectedType {
		return nonDeterministicError(existing, expectedType, expectedSubType, kind, id, name)
	}
	if expectedSubType != "" && existing.SubType != expectedSubType {
		return nonDeterministicError(existing, expectedType, expectedSubType, kind, id, name)
	}
	return nil
}

func nonDeterministicError(existing types.Operation, expectedType types.OperationType, expectedSubType string, kind OperationKind, id, name string) error {
	return &NonDeterministicReplayError{
		OperationError:  &OperationError{Kind: OperationKindNonDeterminism, ID: id, Name: name},
		ExpectedType:    expectedType,
		ExpectedSubType: expectedSubType,
		ActualType:      existing.Type,
		ActualSubType:   existing.SubType,
	}
}

// asNonDeterministic is a small helper so call sites can propagate a
// non-deterministic-replay error detected deeper in the call stack (it is
// itself never wrapped further by the operations that surface it -
// callers should be able to errors.As straight to
// *NonDeterministicReplayError without needing to know how many layers of
// %w it might otherwise have been wrapped in) - kept as a named predicate
// rather than inlining errors.As at every call site for readability.
func isNonDeterministic(err error) bool {
	var nd *NonDeterministicReplayError
	return errors.As(err, &nd)
}

// -----------------------------------------------------------------------
// Large single-operation result handling (docs/remaining-work.md §6 task 16)
// -----------------------------------------------------------------------
//
// # What this task's own framing got wrong, and what real research found
// instead
//
// This task's own text (docs/remaining-work.md's original §6 task 16, and
// the JS/Java comparison-table row it was based on) asserted a specific
// backend mechanism: "if a single operation's serialized result exceeds
// the payload limit, checkpoint it separately... 1. Checkpoint result to
// backend 2. Return empty response 3. Backend stores and returns result."
// This session tried to independently confirm that claim against the
// authoritative source (the official AWS API Reference for
// CheckpointDurableExecution/GetDurableExecutionState, and the AWS SDK for
// Go v2's own independently-generated lambda/types package - the same two
// sources types/wire.go and awssdk/client.go were already verified
// against) BEFORE implementing anything, per this task's own explicit
// instructions not to trust a paraphrased claim from this repo's own prior
// doc without re-checking it.
//
// The claim does NOT hold up. Read in full (2026-07-17):
//   - API_CheckpointDurableExecution.html's Request Body / Response
//     Elements: Payload (request) and every *Details.Result field
//     (response) are plain, ungated strings with no companion "stored
//     separately" flag, S3 URI, or pointer field of any kind. There is no
//     second API call, no "return empty response, backend fetches it
//     later" flow documented anywhere on this page.
//   - github.com/aws/aws-sdk-go-v2/service/lambda@v1.99.0/types/types.go -
//     the same independently-generated source awssdk/client.go was built
//     against - has no S3/URI/offload-shaped field on OperationUpdate,
//     Operation, or any *Details/*Options struct either. Grepped
//     specifically for S3/Uri/Url/Offload/StoredSeparately/
//     PayloadLocation/ResultLocation-shaped names; none exist.
//   - The official AWS Durable Execution SDK Developer Guide's own "Manage
//     state" best-practices page (docs.aws.amazon.com/durable-execution/
//     patterns/best-practices/state/, fetched live) describes the REAL,
//     confirmed pattern for a large single result, and it is the opposite
//     of automatic: "Store references, not payloads" - the application
//     itself stages large data in S3/DynamoDB from inside a step and
//     returns only a reference; a custom Serdes "can compress, encrypt, or
//     offload to external storage while returning only a pointer." The
//     one CONFIRMED automatic large-payload mechanism that page documents
//     is unrelated to this task's scope: Map/Parallel's own BatchResult
//     is automatically summarized above 256KB (that is a batch-level
//     concern already handled by batch.go's own finishBatch/BatchResult
//     path, not a single-operation result).
//   - The same guide's Serialization page documents a concrete, real,
//     already-shipping realization of "custom Serdes offloads to external
//     storage": a FileSystem SerDes with an OVERFLOW storage mode that
//     "uses the standard durable execution checkpoint store and only
//     writes to the filesystem when the value would exceed the durable
//     execution checkpoint size limit." This is exactly the "checkpoint
//     it separately" idea this task's original text gestured at - but it
//     is a user-opted-in types.Serdes implementation requiring an
//     EFS/S3-Files mount the caller must provision, not something this
//     SDK's runtime can transparently and automatically do to every
//     operation's result on the caller's behalf without that
//     infrastructure existing. Implementing a Go FileSystemSerdes is a
//     substantial, separate, infrastructure-dependent feature (the
//     TypeScript/Python guide sections for it are fully written; Java's
//     own guide says "Coming soon" for the same feature) - out of scope
//     for this task, which is specifically about the CHECKPOINT PATH's
//     own behavior when a result is oversized, not about building a new
//     Serdes implementation.
//   - The task text's own "6MB Lambda limit" citation is ALSO independently
//     confirmed, but as something else entirely: docs.aws.amazon.com/
//     lambda/latest/dg/gettingstarted-limits.html documents "6 MB each for
//     request and response (synchronous)" under "Invocation payload" -
//     this is the plain Lambda Invoke API's OWN request/response size
//     limit (relevant to operations.Invoke's target-function payload,
//     and to the handler's own final return value, which Lambda
//     serializes and returns separately from this SDK's checkpoint path -
//     see types.go's Serdes doc and the Developer Guide's "Lambda handler
//     serialization" section), NOT a distinct, separately-documented
//     single-CHECKPOINT payload limit. No AWS documentation page found in
//     this session states a numeric single-operation checkpoint payload
//     limit distinct from checkpoint.DefaultLimits().MaxPayloadBytes
//     (750KB) - the one real, if JS-SDK-sourced rather than
//     AWS-doc-sourced, number already in this codebase governing
//     checkpoint payloads at all.
//
// # What this session implements instead: fail clearly, not outright
//
// Per this task's own explicit fallback instructions for exactly this
// "cannot confirm the real mechanism" outcome: rather than inventing a
// fictional automatic offload call (which would be actively wrong,
// unverifiable against any real backend behavior, and impossible to test
// meaningfully - there is nothing to fake), this adds CLIENT-SIDE
// detection of an oversized single-operation result BEFORE it is ever
// sent to Checkpoint().Enqueue, returning a new structured
// *ResultTooLargeError instead. This directly replaces the PRE-EXISTING
// behavior (confirmed by reading every one of this file's already-listed
// serialize call sites before this task - step.go, invoke.go x2,
// batch.go, wait_for_condition.go - none of them checked size at all) of
// silently enqueueing an oversized payload and letting checkpoint.Manager
// either split it across an awkward multi-item batch boundary or send it
// as an outsized single-item request that the real backend would
// presumably reject with an opaque, low-level error (an unconfirmed
// guess, since this task did not trigger a real oversized request against
// a live endpoint - but strictly worse for a caller than a clear,
// actionable, typed SDK-level error either way).
//
// resultTooLargeThresholdBytes reuses checkpoint.DefaultLimits().
// MaxPayloadBytes (750KB) specifically BECAUSE this task's own research
// found no separately-documented, more-specific single-operation limit to
// use instead (see the honesty note above) - re-deriving a second,
// invented threshold with no real source would be exactly the kind of
// unverified assertion this codebase's conventions warn against. Using
// the SAME number that already, real-ly bounds one checkpoint batch is a
// conservative, defensible choice: a single operation's result that alone
// exceeds the limit governing an entire BATCH of updates can certainly
// never fit, regardless of what else might be batched alongside it.
//
// checkResultSize is called at every call site in this package that is
// about to hand a serialized result string to Checkpoint().Enqueue as an
// OperationUpdate.Payload (see step.go's executeAndCheckpoint, invoke.go's
// Invoke and RunInChildContext, batch.go's runBatchItem,
// wait_for_condition.go's pollAndCheckpoint) - CreateCallback/
// WaitForCallback do NOT call it, because a callback's result payload
// originates from an external system's SendDurableExecutionCallbackSuccess
// -equivalent call, not from this SDK serializing a caller-produced Go
// value; there is nothing for this SDK to size-check before the fact on
// that path.
func checkResultSize(serialized string, kind OperationKind, id, name string) error {
	if len(serialized) <= resultTooLargeThresholdBytes {
		return nil
	}
	return &ResultTooLargeError{
		OperationError: &OperationError{Kind: kind, ID: id, Name: name},
		SizeBytes:      len(serialized),
		ThresholdBytes: resultTooLargeThresholdBytes,
	}
}

// resultTooLargeThresholdBytes is the size (in serialized bytes) above
// which checkResultSize rejects a single operation's result before
// checkpointing it. See this block's top-level doc for why this reuses
// checkpoint.DefaultLimits().MaxPayloadBytes rather than a separately
// invented number - deliberately NOT imported as a live reference to
// checkpoint.DefaultLimits() itself, to avoid this package importing
// pkg/durable/checkpoint (which would create operations -> checkpoint ->
// operations, since checkpoint.Manager's own Enqueue path is reached via
// dcontext.Context.Checkpoint(), and dcontext already imports operations'
// sibling packages - see checkpoint/errors.go's own top-level doc for the
// identical import-cycle reasoning behind ITS choice not to import
// operations). Kept as a plain literal, with this comment as the single
// source of truth for why the two numbers must be kept in sync if
// checkpoint.DefaultLimits() ever changes.
const resultTooLargeThresholdBytes = 750 * 1024

// ResultTooLargeError is returned when a single durable operation's
// serialized result exceeds resultTooLargeThresholdBytes, BEFORE it is
// checkpointed - see this file's "Large single-operation result handling"
// block above for the full research this is based on, and why this is a
// deliberate, conservative, "fail clearly instead of failing outright"
// fallback rather than an automatic offload: this session could not
// confirm any real backend mechanism for checkpointing an oversized
// single result out-of-band, after checking the official API reference,
// the AWS SDK for Go v2's independently-generated Lambda types, and the
// official Developer Guide's Serialization/Manage-state pages.
//
// A caller hitting this error has two real, currently-available options,
// both confirmed against the official Developer Guide's own documented
// guidance (see this file's research notes above) rather than invented
// for this error's doc: (1) restructure the operation to return a
// reference (an S3 key, a DynamoDB item ID) instead of the full payload -
// the guide's own explicitly recommended pattern for this exact
// situation, or (2) supply a custom types.Serdes (via WithStepSerdes,
// WithInvokeSerdes, WithChildSerdes, etc.) that offloads the value to
// external storage itself and returns a small pointer string - which is
// precisely what the guide's confirmed FileSystem SerDes (OVERFLOW mode)
// does, though this Go SDK does not yet ship that specific
// implementation (see this file's block doc; tracked as a distinct,
// larger, infrastructure-dependent follow-up task, not part of this one).
type ResultTooLargeError struct {
	*OperationError
	// SizeBytes is the actual serialized size (in bytes) that triggered
	// this error.
	SizeBytes int
	// ThresholdBytes is the threshold that was exceeded
	// (resultTooLargeThresholdBytes at the time this error was created).
	ThresholdBytes int
}

// Unwrap - see StepFailedError.Unwrap's doc for why this override is
// required on every concrete type in this file.
func (e *ResultTooLargeError) Unwrap() error { return e.OperationError }

func (e *ResultTooLargeError) Error() string {
	return fmt.Sprintf(
		"%s %q (id %s): serialized result is %d bytes, which exceeds the %d-byte single-operation checkpoint threshold - restructure the operation to return a reference instead of the full payload (e.g. stage large data in S3/DynamoDB and return a key), or supply a custom Serdes that offloads to external storage and returns a pointer; see ResultTooLargeError's doc for details",
		kindLabel(e.Kind), e.Name, e.ID, e.SizeBytes, e.ThresholdBytes,
	)
}

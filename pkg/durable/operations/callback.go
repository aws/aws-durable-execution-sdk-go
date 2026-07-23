package operations

import (
	"encoding/json"
	"fmt"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// WaitForCallbackOption configures a WaitForCallback invocation.
type WaitForCallbackOption[T any] func(*waitForCallbackConfig[T])

type waitForCallbackConfig[T any] struct {
	timeout          *types.Duration
	heartbeatTimeout *types.Duration
	submitterRetry   func(err error, attempt int) types.RetryDecision
	serdes           types.Serdes
}

// WithWaitForCallbackTimeout sets the maximum duration the backend will
// wait for the external system to submit a terminal callback result
// before unilaterally timing the callback out, surfaced to the handler
// as a *CallbackFailedError with Timeout=true (ErrorType
// "Callback.Timeout") - checkpointed as CallbackOptions.TimeoutSeconds
// on the inner CALLBACK/CALLBACK START, exactly like the lower-level
// CreateCallback's own WithCallbackTimeout option.
//
// # Bug this fixes
//
// This option's doc used to say "NOT YET IMPLEMENTED... WaitForCallback
// does not currently enforce any timeout," reasoning that enforcement
// would require racing WaitForOperation against a separate timeout timer
// - a real limitation IF WaitForCallback needed to enforce the timeout
// itself. It does not: the timeout is entirely backend-driven (the
// backend independently unilaterally times out the CALLBACK operation
// and checkpoints CallbackTimedOut - see CreateCallback's own
// WithCallbackTimeout doc and callback.go's callbackError, both already
// confirmed working end-to-end against a real deployment in the Callback
// suite's 4-3/4-11/4-14 requirements), so this SDK only ever needed to
// hand cfg.timeout to CreateCallback's own WithCallbackTimeout option -
// a plain wiring gap, not a missing backend feature or a missing
// suspend-racing mechanism. Confirmed via a real deployment
// (requirement 7-5 of the WaitForCallback conformance suite: a 3-second
// WithWaitForCallbackTimeout with no external system ever responding)
// that this now produces the exact expected CallbackTimedOut/
// Callback.Timeout/ContextFailed/ExecutionFailed sequence once wired
// through below.
func WithWaitForCallbackTimeout[T any](d types.Duration) WaitForCallbackOption[T] {
	return func(c *waitForCallbackConfig[T]) { c.timeout = &d }
}

// WithWaitForCallbackHeartbeatTimeout sets the maximum duration the
// backend will wait between heartbeats (or since callback creation,
// before the first heartbeat) before unilaterally timing the callback
// out, surfaced to the handler as a *CallbackFailedError with
// Timeout=true (ErrorType "Callback.Heartbeat") - checkpointed as
// CallbackOptions.HeartbeatTimeoutSeconds on the inner CALLBACK/CALLBACK
// START, exactly like the lower-level CreateCallback's own
// WithCallbackHeartbeatTimeout option.
//
// # Gap this closes
//
// Before this option existed, WaitForCallback exposed NO way at all to
// request a heartbeat timeout - only the lower-level CreateCallback did
// (via WithCallbackHeartbeatTimeout), never threaded through
// WaitForCallback's own composition. This is the same class of "missing
// wiring, not a missing backend feature" gap WithWaitForCallbackTimeout's
// own doc describes; this option and the cfg.heartbeatTimeout wiring
// below close it the same way. Confirmed via a real deployment
// (requirements 7-12 and 7-13 of the WaitForCallback conformance suite:
// a 5-second heartbeat timeout with no heartbeat ever sent times out
// with ErrorType Callback.Heartbeat as expected, and a 10-second
// heartbeat timeout with an external heartbeat sent first, then a
// success, resolves successfully without ever timing out).
func WithWaitForCallbackHeartbeatTimeout[T any](d types.Duration) WaitForCallbackOption[T] {
	return func(c *waitForCallbackConfig[T]) { c.heartbeatTimeout = &d }
}

// WithWaitForCallbackSubmitterRetryStrategy sets a retry strategy for the
// submitter step (the checkpointed STEP that hands the generated
// callback ID off to the external system) - passed straight through to
// the internal Step call via operations.WithStepRetryStrategy, giving
// WaitForCallback's submitter the exact same retry configurability a
// caller already has on a plain Step.
//
// # Gap this closes
//
// Before this option existed, WaitForCallback's internal `Step(child,
// id+"-submit", ...)` call passed NO StepOption at all (confirmed by
// reading this file's own WaitForCallback body before this fix), so
// there was no way to configure retry on the submitter specifically -
// it silently always ran under Step's own zero-option default
// (utils.Presets.NoRetry(), i.e. never retry). This is a genuine,
// distinct gap from the timeout/heartbeat one above (it is about the
// submitter STEP's retry policy, not the CALLBACK's own timeout), first
// exposed by the WaitForCallback conformance suite's requirement 7-7 (a
// submitter that throws on every attempt, with a configured 2-attempt/
// 1-second-delay retry policy that must exhaust before the operation
// fails) - confirmed fixed via a real deployment: with this option
// wired through to the submitter's Step call below, 7-7 now produces the
// exact expected StepStarted/StepFailed (attempt 1) ->
// InvocationCompleted (suspends for the 1s retry delay) -> StepStarted/
// StepFailed (attempt 2) -> ContextFailed -> ExecutionFailed sequence.
func WithWaitForCallbackSubmitterRetryStrategy[T any](fn func(err error, attempt int) types.RetryDecision) WaitForCallbackOption[T] {
	return func(c *waitForCallbackConfig[T]) { c.submitterRetry = fn }
}

// WithWaitForCallbackSerdes sets a custom serializer for the callback
// result.
func WithWaitForCallbackSerdes[T any](s types.Serdes) WaitForCallbackOption[T] {
	return func(c *waitForCallbackConfig[T]) { c.serdes = s }
}

// WaitForCallback creates a callback, invokes submitter (as a checkpointed
// step) to hand the callback ID off to an external system (e.g. an
// approval email, a webhook registration), and suspends execution until
// that external system calls the corresponding
// SendDurableExecutionCallbackSuccess/Failure API.
//
// This combines CreateCallback + a submitter step + waiting into a single
// operation for the common human-in-the-loop / external-webhook pattern,
// matching the confirmed real-backend flowchart exactly (internal SDK
// Operation Diagrams design doc, "WaitForCallback"):
// CONTEXT/WAIT_FOR_CALLBACK START -> CALLBACK/CALLBACK START (new
// callbackId) -> STEP/STEP START running submitter (with its own full
// Step-style retry subgraph) -> await the callback ->
// CONTEXT/WAIT_FOR_CALLBACK SUCCEED/FAIL wrapping the callback's outcome.
//
// Implemented as a straightforward composition of RunInChildContext (for
// the CONTEXT/WAIT_FOR_CALLBACK wrapper), CreateCallback (for the
// CALLBACK/CALLBACK registration), and Step (for the submitter, getting
// Step's retry behavior for free) - not a fourth checkpoint-state-machine
// primitive, exactly as the design docs for this operation anticipated.
func WaitForCallback[T any](dc types.DurableContext, id string, submitter func(sc types.StepContext, callbackID string) error, opts ...WaitForCallbackOption[T]) (T, error) {
	cfg := &waitForCallbackConfig[T]{serdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}
	// Config validation (docs/remaining-work.md §8 task 18): a negative
	// timeout/heartbeatTimeout Duration has no valid interpretation -
	// unlike CheckpointStrategy's out-of-range-but-currently-inert case
	// (see durable.go's validateConfig doc for why THAT one is
	// deliberately NOT rejected), a negative duration is unambiguously
	// nonsensical on its own terms (durationToSeconds-style helpers
	// elsewhere in this codebase, e.g. utils/retry.go, treat a Duration
	// purely as a sum of Days/Hours/Minutes/Seconds with no independent
	// validity check of their own) - reject it up front instead of
	// silently handing a negative TimeoutSeconds/HeartbeatTimeoutSeconds
	// to the backend.
	if cfg.timeout != nil {
		if secs := cfg.timeout.Days*86400 + cfg.timeout.Hours*3600 + cfg.timeout.Minutes*60 + cfg.timeout.Seconds; secs < 0 {
			var zero T
			return zero, fmt.Errorf("operations.WaitForCallback: WithWaitForCallbackTimeout duration must be non-negative, got %d total seconds", secs)
		}
	}
	if cfg.heartbeatTimeout != nil {
		if secs := cfg.heartbeatTimeout.Days*86400 + cfg.heartbeatTimeout.Hours*3600 + cfg.heartbeatTimeout.Minutes*60 + cfg.heartbeatTimeout.Seconds; secs < 0 {
			var zero T
			return zero, fmt.Errorf("operations.WaitForCallback: WithWaitForCallbackHeartbeatTimeout duration must be non-negative, got %d total seconds", secs)
		}
	}

	return RunInChildContext(dc, id, func(child types.DurableContext) (T, error) {
		var callbackOpts []CallbackOption[T]
		if cfg.timeout != nil {
			callbackOpts = append(callbackOpts, WithCallbackTimeout[T](*cfg.timeout))
		}
		if cfg.heartbeatTimeout != nil {
			callbackOpts = append(callbackOpts, WithCallbackHeartbeatTimeout[T](*cfg.heartbeatTimeout))
		}
		resultCh, callbackID, err := CreateCallback[T](child, id+"-callback", callbackOpts...)
		if err != nil {
			var zero T
			return zero, err
		}

		var stepOpts []StepOption[struct{}]
		if cfg.submitterRetry != nil {
			stepOpts = append(stepOpts, WithStepRetryStrategy[struct{}](cfg.submitterRetry))
		}
		if _, err := Step(child, id+"-submit", func(sc types.StepContext) (struct{}, error) {
			return struct{}{}, submitter(sc, callbackID)
		}, stepOpts...); err != nil {
			var zero T
			return zero, err
		}

		result := AwaitCallback(child, resultCh)
		if result.Err != nil {
			var zero T
			return zero, result.Err
		}
		return result.Value, nil
	}, WithChildSerdes[T](cfg.serdes), withChildSubType[T](subTypeWaitForCallback))
}

// AwaitCallback blocks the calling goroutine until resultCh (as returned
// by CreateCallback) delivers a result, correctly deregistering this
// goroutine as active for the duration of the wait and re-registering it
// once a result arrives - see CreateCallback's doc ("Receiving the
// result: use AwaitCallback, not a raw channel receive") for why a plain
// `<-resultCh` is a real correctness bug, not just a style preference.
//
// dc must be created by this SDK's runtime (the same requirement every
// other operation in this package has) - it's used only to reach the
// shared execmgr.Manager for the register/deregister pair, not to
// checkpoint anything itself (CreateCallback already did that).
func AwaitCallback[T any](dc types.DurableContext, resultCh <-chan CallbackResult[T]) CallbackResult[T] {
	c, ok := dc.(*dcontext.Context)
	if !ok {
		// No execManager to coordinate with - fall back to a plain
		// receive rather than panicking, matching this package's general
		// preference for returning a descriptive error over crashing,
		// though in practice every real call path provides a
		// runtime-constructed dc.
		return <-resultCh
	}

	select {
	case result := <-resultCh:
		return result
	default:
	}

	c.ExecManager().Deregister()
	defer c.ExecManager().Register()
	return <-resultCh
}

// CallbackOption configures a CreateCallback invocation.
type CallbackOption[T any] func(*callbackConfig[T])

type callbackConfig[T any] struct {
	serdes           types.Serdes
	timeoutSeconds   *int
	heartbeatSeconds *int
}

// WithCallbackSerdes sets a custom serializer for the callback's result.
func WithCallbackSerdes[T any](s types.Serdes) CallbackOption[T] {
	return func(c *callbackConfig[T]) { c.serdes = s }
}

// WithCallbackTimeout sets the maximum duration (in whole seconds) the
// backend will wait for the external system to submit a terminal
// SendDurableExecutionCallbackSuccess/Failure call before unilaterally
// timing the callback out - checkpointed as CallbackOptions.
// TimeoutSeconds on the CALLBACK/CALLBACK START update (types/wire.go's
// CallbackOptions; confirmed against the official API reference's
// CheckpointDurableExecution request syntax).
//
// # Bug this fixes
//
// Before this option existed, CreateCallback's OperationUpdate never set
// CallbackOptions at all - types.CallbackOptions.TimeoutSeconds/
// HeartbeatTimeoutSeconds were already fully defined on the wire type
// and already threaded through checkpoint/awssdk's marshaling path (see
// awssdk/client.go's TimeoutSeconds/HeartbeatTimeoutSeconds mapping,
// covered by an existing client_test.go case), but nothing in this
// package's own CreateCallback ever populated them - a genuine,
// verified gap (confirmed by grepping this entire package for
// "CallbackOptions" before this fix: zero matches outside wire.go/
// client.go). This meant a caller had no way to request a backend-
// enforced timeout on a plain CreateCallback call, only the same
// unenforced-on-the-Go-side WithWaitForCallbackTimeout accepted (and
// still does not enforce, per that option's own doc) on the higher-level
// WaitForCallback composition. This option closes the gap for
// CreateCallback specifically: the timeout enforcement itself is
// entirely backend-driven (the same "CallbackTimedOut" terminal
// checkpoint event Wait's own WaitSucceeded is - see
// awaitCallback/callbackError below for how a TIMED_OUT terminal status
// now surfaces as a *CallbackFailedError with Timeout=true), this SDK
// only needs to hand the requested duration to the backend up front.
func WithCallbackTimeout[T any](d types.Duration) CallbackOption[T] {
	secs := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds
	return func(c *callbackConfig[T]) { c.timeoutSeconds = &secs }
}

// WithCallbackHeartbeatTimeout sets the maximum duration (in whole
// seconds) the backend will wait between heartbeats (or since callback
// creation, before the first heartbeat) before unilaterally timing the
// callback out - checkpointed as CallbackOptions.HeartbeatTimeoutSeconds,
// distinct from WithCallbackTimeout's overall TimeoutSeconds (a callback
// may configure either, both, or neither - see CallbackOptions' doc).
// See WithCallbackTimeout's doc for the shared "this option didn't exist
// at all before" bug this closes.
func WithCallbackHeartbeatTimeout[T any](d types.Duration) CallbackOption[T] {
	secs := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds
	return func(c *callbackConfig[T]) { c.heartbeatSeconds = &secs }
}

// CallbackResult is delivered on the channel returned by CreateCallback.
type CallbackResult[T any] struct {
	Value T
	Err   error
}

// CreateCallback is the low-level primitive underlying WaitForCallback. It
// registers a callback with the Lambda Durable Functions backend and
// returns a channel that resolves when the external system calls
// SendDurableExecutionCallbackSuccess/Failure, along with the callback ID
// to hand off out-of-band (typically via a Step, so the hand-off itself is
// replay-safe).
//
// Prefer WaitForCallback unless you need to interleave callback
// registration with other durable operations before suspending - matching
// the confirmed real-backend flowchart's note that ctx.createCallback
// returns immediately with a callbackPromise and callbackId, letting the
// caller "do other processing" before awaiting the promise.
//
// # Receiving the result: use AwaitCallback, not a raw channel receive
//
// Callers MUST receive from the returned channel via AwaitCallback, not
// a plain `<-resultCh` - a raw receive leaves the calling goroutine
// registered as active (per execmgr.Manager.Register/Register's
// contract) for the ENTIRE time it's actually blocked waiting on an
// external system, which is exactly the condition that should allow the
// whole execution to suspend. This was a real, deterministically
// reproducible bug (not just a -race flake): masked in the single
// top-level-callback case (the top-level handler goroutine's own
// un-tracked block happened to coincidentally compensate for
// awaitCallback's THEN-also-missing Register call - both since fixed),
// but became a genuinely wrong "never suspends" bug once a callback was
// created and awaited from inside a Parallel/Map branch, found via a
// test written while hardening those operations' own concurrency this
// session.
//
// # Implementation notes
//
// Checkpoints CALLBACK/CALLBACK START (mirroring the flowchart), then
// blocks the calling goroutine via ExecManager.WaitForOperation exactly
// like Wait - if no other goroutine is active, this drops the active
// count to zero and the whole invocation suspends (returns PENDING),
// resuming only once an external SendDurableExecutionCallbackSuccess/
// Failure call updates the operation to a terminal status and a
// subsequent invocation observes it. On replay where the callback has
// already resolved, CreateCallback replay-skips exactly like Step: the
// checkpointed result/error is returned on the channel immediately
// without re-registering.
//
// The returned channel always has exactly one value sent before being
// closed - buffered so the caller is never required to receive before
// CreateCallback returns.
func CreateCallback[T any](dc types.DurableContext, id string, opts ...CallbackOption[T]) (<-chan CallbackResult[T], string, error) {
	cfg := &callbackConfig[T]{serdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}

	c, ok := dc.(*dcontext.Context)
	if !ok {
		return nil, "", fmt.Errorf("operations.CreateCallback: dc must be created by this SDK's runtime (got %T)", dc)
	}

	callbackStepID := c.NextStepID()
	resultCh := make(chan CallbackResult[T], 1)

	if existing, found := c.ExecManager().GetOperation(callbackStepID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CALLBACK before reading CallbackDetails off of it below - see
		// NonDeterministicReplayError's doc for the general rationale.
		if err := checkReplayConsistency(existing, types.OperationTypeCallback, "", OperationKindCallback, callbackStepID, id); err != nil {
			return nil, "", err
		}

		callbackID := ""
		if existing.CallbackDetails != nil {
			callbackID = existing.CallbackDetails.CallbackID
		}
		switch existing.Status {
		case types.OperationStatusSucceeded:
			val, err := deserializeCallbackResult[T](cfg.serdes, existing, callbackStepID)
			resultCh <- CallbackResult[T]{Value: val, Err: err}
			close(resultCh)
			return resultCh, callbackID, nil
		case types.OperationStatusFailed, types.OperationStatusTimedOut:
			// TimedOut is handled identically to Failed here - both are
			// non-success terminal statuses whose CallbackDetails.Error
			// (populated by the backend for a real timeout exactly like
			// an explicit failure - see CallbackTimedOutDetails.Error in
			// the official API reference) callbackError reads uniformly;
			// callbackError itself distinguishes the two for the
			// returned *CallbackFailedError.Timeout flag. See this
			// function's own doc and callbackError's doc for the bug
			// this closes (TimedOut used to fall through this switch
			// entirely, silently treated as neither success nor
			// failure).
			resultCh <- CallbackResult[T]{Err: callbackError(existing, id)}
			close(resultCh)
			return resultCh, callbackID, nil
		case types.OperationStatusStarted, types.OperationStatusPending:
			// Already registered with the backend, still awaiting the
			// external system's response - block for it, exactly like
			// Wait blocking on an in-flight WAIT operation.
			c.ExecManager().Register() // see awaitCallback's doc: must register before spawning
			go awaitCallback[T](c, callbackStepID, id, cfg.serdes, resultCh)
			return resultCh, callbackID, nil
		}
	}

	callbackOptions := (*types.CallbackOptions)(nil)
	if cfg.timeoutSeconds != nil || cfg.heartbeatSeconds != nil {
		callbackOptions = &types.CallbackOptions{
			TimeoutSeconds:          cfg.timeoutSeconds,
			HeartbeatTimeoutSeconds: cfg.heartbeatSeconds,
		}
	}

	dinfo := operationKindDispatchInfo{ID: callbackStepID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeCallback, SubType: subTypeCallback}
	dispatchOperationStart(c, dinfo, 0)

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:              callbackStepID,
		ParentID:        c.ParentStepID(),
		Type:            types.OperationTypeCallback,
		Name:            id,
		SubType:         subTypeCallback,
		Action:          types.OperationActionStart,
		CallbackOptions: callbackOptions,
	}); err != nil {
		return nil, "", fmt.Errorf("callback %q (id %s): checkpointing start: %w", id, callbackStepID, err)
	}

	// The checkpoint acknowledgment (handled by checkpoint.Manager.
	// PutOperation, called from sendBatch after a successful Checkpoint
	// call - see checkpoint/manager.go) has by this point already
	// recorded the backend-assigned CallbackDetails.CallbackID into
	// execManager's in-memory operation map, so it's available via
	// GetOperation immediately after Enqueue returns without a separate
	// round trip.
	callbackID := ""
	if op, found := c.ExecManager().GetOperation(callbackStepID); found && op.CallbackDetails != nil {
		callbackID = op.CallbackDetails.CallbackID
	}

	c.ExecManager().Register() // see awaitCallback's doc: must register before spawning
	go awaitCallback[T](c, callbackStepID, id, cfg.serdes, resultCh)
	return resultCh, callbackID, nil
}

// awaitCallback blocks (via ExecManager.WaitForOperation, the same
// suspend-coordinated primitive Wait uses) until the callback identified
// by callbackStepID reaches a terminal status, then delivers the result on
// resultCh. Runs on its own goroutine so CreateCallback can return
// immediately per its documented "returns immediately, do other
// processing" contract - matching the confirmed flowchart's explicit
// OtherProcessing step between ReturnCallbackId and AwaitCallbackPromise.
//
// The CALLER must call execmgr.Manager.Register() for this goroutine
// BEFORE spawning it (see both call sites in CreateCallback) - this
// function does not register itself on entry, only deregisters (via
// WaitForOperation) once it actually blocks. An earlier version of
// CreateCallback spawned this goroutine WITHOUT registering it first, a
// real accounting bug: WaitForOperation's internal Deregister() call
// would then drop the active count on behalf of a goroutine that was
// never counted as active to begin with, silently borrowing "credit"
// from whichever other goroutine happened to be registered at the time.
// This was invisible in the single-top-level-callback case (the calling
// goroutine's own un-tracked block on resultCh happened to compensate for
// it by coincidence), but became a real, deterministically wrong
// suspension-detection bug once a callback was created from inside a
// Parallel/Map branch - found via a genuine-suspension test written
// while hardening those operations' own concurrency this session.
func awaitCallback[T any](c *dcontext.Context, callbackStepID, name string, serdes types.Serdes, resultCh chan<- CallbackResult[T]) {
	op, ok := c.ExecManager().WaitForOperation(callbackStepID)
	if !ok {
		// Execution suspended before the callback resolved. No one is
		// listening on resultCh in this invocation (the handler goroutine
		// that would have received it is itself being abandoned - see
		// durable.WithDurableExecution's suspend-or-complete race), so
		// there is nothing useful to send; leave the channel open and
		// let it be garbage collected along with this invocation's
		// goroutines.
		return
	}
	dinfo := operationKindDispatchInfo{ID: callbackStepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeCallback, SubType: subTypeCallback}
	if op.Status == types.OperationStatusFailed || op.Status == types.OperationStatusTimedOut {
		// See CreateCallback's own replay-branch switch (same
		// Failed/TimedOut pairing) and callbackError's doc for why
		// TimedOut is handled here rather than falling through to the
		// deserializeCallbackResult success path below.
		err := callbackError(op, op.Name)
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
		resultCh <- CallbackResult[T]{Err: err}
	} else {
		val, err := deserializeCallbackResult[T](serdes, op, callbackStepID)
		if err != nil {
			dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
		} else {
			serialized, _ := utils.DefaultSerdes().Serialize(val, callbackStepID, "")
			dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, serialized, nil)
		}
		resultCh <- CallbackResult[T]{Value: val, Err: err}
	}
	close(resultCh)
}

// deserializeCallbackResult deserializes a succeeded CALLBACK operation's
// result, returning the zero value of T (not an error) when
// CallbackDetails.Result is absent.
//
// # Bug this fixes
//
// Before this fix, a nil op.CallbackDetails.Result was treated
// identically to a malformed/corrupt checkpoint: this function
// unconditionally returned a generic "succeeded but no result recorded"
// error, even though a genuinely payload-less success is a real,
// legitimate outcome an external system can produce (calling
// SendDurableExecutionCallbackSuccess with no Payload at all - the
// conformance suite's own requirement 7-15 exercises exactly this via
// its CallbackActions Operation: success with no Payload field).
// Contrast with Step's own analogous nil-Result guard
// (deserializeStepResult in step.go, which this function's own doc used
// to say it "matches"): a Step's Result can never actually be nil on a
// real success, because executeAndCheckpoint always sends a non-nil
// `Payload: &serialized` pointer (even a Go step returning nil/no value
// still serializes to the JSON string "null" and gets checkpointed as
// that - see step_1_5.go's own handler and doc), so Step's nil check is
// purely a defensive guard against a genuinely malformed backend
// response. A CALLBACK's Result, by contrast, is set entirely by the
// EXTERNAL system's own SendDurableExecutionCallbackSuccess call, which
// can legitimately omit a payload - so treating "no Result" as always an
// error was conflating "malformed data" with "the external system simply
// didn't send one," a real, distinct, and reachable case. This function
// now returns T's zero value (nil for a pointer type, "" for a string,
// etc.) for that case instead, matching how every other "null/undefined
// result" requirement in this suite's sibling suites (e.g. step's own
// 1-5) models an absent result as the zero value of a pointer type
// rather than as an error.
func deserializeCallbackResult[T any](serdes types.Serdes, op types.Operation, callbackStepID string) (T, error) {
	var zero T
	if op.CallbackDetails == nil || op.CallbackDetails.Result == nil {
		return zero, nil
	}
	val, err := serdes.Deserialize(*op.CallbackDetails.Result, callbackStepID, "")
	if err != nil {
		return zero, newSerdesError("deserialize", callbackStepID, op.Name, err)
	}
	typed, ok := val.(T)
	if ok {
		return typed, nil
	}
	// Fall back to a JSON re-marshal/unmarshal round trip, matching
	// Step's deserializeStepResult - see that function's doc for why.
	b, marshalErr := json.Marshal(val)
	if marshalErr != nil {
		return zero, newSerdesError("deserialize", callbackStepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, marshalErr))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", callbackStepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	return typed, nil
}

// callbackError builds a *CallbackFailedError for a CALLBACK operation
// that ended in a non-success terminal state - either an explicit
// external-system failure (op.Status == Failed) or a backend-enforced
// timeout (op.Status == TimedOut, covering BOTH the overall
// CallbackOptions.TimeoutSeconds and the HeartbeatTimeoutSeconds cases -
// the real backend distinguishes these two timeout flavors only via
// CallbackDetails.Error.Payload.ErrorType's string value ("Callback.
// Timeout" vs "Callback.Heartbeat", confirmed against this suite's own
// test-requirements/callback/4-3.yaml and 4-4.yaml
// ExpectedExecutionHistory blocks), not via a distinct OperationStatus,
// so Timeout is set to true for either.
//
// # Bug this fixes
//
// Before this fix, Timeout was hard-coded false in every call site (see
// CallbackFailedError's own doc, which used to say "Always false
// currently" pending heartbeat-timeout enforcement) because nothing
// upstream (CreateCallback's replay switch, awaitCallback) ever reached
// this function for a TimedOut operation in the first place - a TimedOut
// status fell through to the SUCCESS deserialize path instead (silently
// wrong: deserializeCallbackResult would find a nil CallbackDetails.
// Result and return a generic "succeeded but no result recorded" error,
// masking the real timeout entirely). Now that both call sites route
// TimedOut here explicitly, this function sets Timeout based on the
// actual op.Status rather than a hard-coded constant.
func callbackError(op types.Operation, name string) error {
	timeout := op.Status == types.OperationStatusTimedOut
	if op.CallbackDetails != nil && op.CallbackDetails.Error != nil {
		return &CallbackFailedError{
			OperationError: &OperationError{Kind: OperationKindCallback, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.CallbackDetails.Error.ErrorMessage)},
			Timeout:        timeout,
		}
	}
	return &CallbackFailedError{
		OperationError: &OperationError{Kind: OperationKindCallback, ID: op.ID, Name: name, Err: fmt.Errorf("failed with no recorded error")},
		Timeout:        timeout,
	}
}

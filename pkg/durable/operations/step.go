// Package operations provides the durable operations available on a
// DurableContext: Step, Wait, CreateCallback, WaitForCallback,
// WaitForCondition, Invoke, RunInChildContext, Map, and Parallel.
//
// The operation set and execution semantics (checkpointing, replay,
// hierarchical step IDs, retry/backoff) are intentionally identical to the
// Java SDK's ctx.step()/ctx.wait()/etc. family, since the checkpoint/replay
// protocol is fixed by the Lambda Durable Functions backend and must be
// consistent across all language SDKs. The API surface itself (generics,
// functional options, context.Context) is idiomatic Go rather than a
// direct syntactic port of Java or TypeScript. See
// docs/checkpoint-replay-design.md for the cross-SDK research this
// implementation is based on.
package operations

import (
	"encoding/json"
	"fmt"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// StepOption configures a Step invocation.
type StepOption[T any] func(*stepConfig[T])

type stepConfig[T any] struct {
	retryStrategy func(err error, attempt int) types.RetryDecision
	semantics     types.StepSemantics
	serdes        types.Serdes
}

// WithStepRetryStrategy sets a custom retry strategy for a step. The
// strategy function is invoked on failure with the error and the current
// attempt number (starting at 1) and returns whether to retry and after
// what delay.
func WithStepRetryStrategy[T any](fn func(err error, attempt int) types.RetryDecision) StepOption[T] {
	return func(c *stepConfig[T]) { c.retryStrategy = fn }
}

// WithStepSemantics sets the checkpoint/retry semantics for a step. See
// types.StepSemantics for details.
func WithStepSemantics[T any](sem types.StepSemantics) StepOption[T] {
	return func(c *stepConfig[T]) { c.semantics = sem }
}

// WithStepSerdes sets a custom serializer for the step's checkpointed
// result. Defaults to JSON.
func WithStepSerdes[T any](s types.Serdes) StepOption[T] {
	return func(c *stepConfig[T]) { c.serdes = s }
}

// Step executes fn as a durable, checkpointed operation identified by id.
//
// On first execution, fn runs and its result (or error) is checkpointed to
// the Lambda Durable Functions backend. On replay, if this step has already
// completed, fn is NOT re-executed; the checkpointed result is returned
// directly. This is the fundamental unit of durable, replay-safe work -
// non-deterministic operations (I/O, timestamps, random values) must be
// placed inside a Step rather than in code that runs on every replay.
//
// id must be unique within the enclosing context (root or child) and
// should be stable across deployments of the same logical step; changing
// it changes the step's position in the replay sequence.
//
// # Implementation notes
//
// This follows the replay-skip / checkpoint state machine described in
// docs/checkpoint-replay-design.md §3 and §6, ported from the JS SDK's
// step-handler.ts and the Java SDK's StepOperation:
//
//  1. Claim this context's next step ID and look it up in the operation
//     log (dc.ExecManager().GetOperation).
//  2. If found and terminal (Succeeded/Failed), return the checkpointed
//     result/error without running fn - this is the replay-skip path.
//  3. If found and Started under AtMostOncePerRetry semantics, this is an
//     interrupted attempt (crashed after the pre-execution checkpoint but
//     before completion): synthesize an interruption error and consult
//     the retry strategy on it, exactly as the JS/Java SDKs do.
//  4. Otherwise, run fn, checkpoint the outcome, and on failure consult
//     the retry strategy to decide whether to re-run or return the
//     terminal error. A retry suspends the invocation for the configured
//     delay via the same WaitForOperation mechanism Wait uses (see
//     retryOrFail) - re-execution happens either later in this same
//     process (if the delay resolves synchronously, e.g. a test runner's
//     SkipTime mode) or via a fresh invocation once the backend's
//     NextAttemptDelaySeconds timer elapses (the OperationStatusPending
//     case below is what picks it back up then).
func Step[T any](dc types.DurableContext, id string, fn func(sc types.StepContext) (T, error), opts ...StepOption[T]) (T, error) {
	cfg := &stepConfig[T]{
		semantics: types.StepSemanticsAtLeastOncePerRetry,
		serdes:    utils.DefaultSerdes(),
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.retryStrategy == nil {
		// utils.Presets.Default(), NOT NoRetry() - see that preset's own
		// doc comment. Confirmed via the JS reference SDK's own
		// step-handler.ts: both of its retry call sites fall back to
		// retryPresets.default (6 attempts, 5s initial delay, 60s max
		// delay, 2x backoff, full jitter) whenever no retryStrategy
		// option is supplied - this Go SDK previously fell back to
		// NoRetry() instead, a genuine, real divergence from the
		// reference SDK's own documented "no explicit config" behavior
		// (conformance requirement 1-13's own DefaultRetryParameters
		// describe exactly this Default preset's values), not a
		// deliberate Go-specific design choice - there is no Go idiom
		// that prefers a silent no-op default over matching a
		// cross-language protocol's own established default. Found via
		// direct comparison against step-handler.ts after the earlier
		// conclusion (this was a genuine SDK feature gap, not fixable)
		// was re-examined per explicit user request.
		cfg.retryStrategy = utils.Presets.Default()
	}

	c, ok := dc.(*dcontext.Context)
	if !ok {
		var zero T
		return zero, fmt.Errorf("operations.Step: dc must be created by this SDK's runtime (got %T)", dc)
	}

	stepID := c.NextStepID()
	return runStep(c, stepID, id, fn, cfg)
}

func runStep[T any](c *dcontext.Context, stepID, name string, fn func(sc types.StepContext) (T, error), cfg *stepConfig[T]) (T, error) {
	var zero T

	if existing, found := c.ExecManager().GetOperation(stepID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// STEP before reading StepDetails off of it below. A plain Step
		// doesn't distinguish by SubType (WaitForCondition, which does,
		// has its own copy of this check in wait_for_condition.go), so
		// expectedSubType is "" here - see checkReplayConsistency's doc
		// for why that's still a meaningful, narrower check than skipping
		// SubType comparison entirely would be.
		if err := checkReplayConsistency(existing, types.OperationTypeStep, "", OperationKindStep, stepID, name); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeStepResult[T](cfg.serdes, existing, stepID)

		case types.OperationStatusFailed:
			return zero, stepError(existing, name)

		case types.OperationStatusStarted:
			if cfg.semantics == types.StepSemanticsAtMostOncePerRetry {
				// Interrupted: checkpointed START but never reached a
				// terminal state. Treat like any other failure so the
				// configured retry strategy decides what happens next.
				return retryOrFail(c, stepID, name, fn, cfg,
					fmt.Errorf("step %q (id %s): interrupted after checkpointing start under AtMostOncePerRetry semantics", name, stepID),
					stepAttempt(existing)+1)
			}
			// AtLeastOncePerRetry: safe to simply re-run fn - but
			// WITHOUT re-checkpointing START. This step was already
			// checkpointed STARTED in a prior invocation (e.g. it
			// crashed the process after START but before completion,
			// exactly like AtMostOncePerRetry's own interruption case,
			// just under the semantics that tolerates re-running fn
			// rather than treating it as a retryable failure). Bug
			// found and fixed this session: this branch used to fall
			// through to the function's own final `return
			// executeAndCheckpoint(c, stepID, name, fn, cfg, 0)` below,
			// which unconditionally re-sent a STEP/START checkpoint
			// before running fn again - confirmed via a real deployed
			// conformance-harness function (a step nested inside a
			// RunInChildContext call, crashing on its first attempt
			// with default AtLeastOncePerRetry semantics) that the real
			// backend REJECTS a second START checkpoint on an
			// already-STARTED step outright:
			// InvalidParameterValueException: "Invalid current STEP
			// state to start." (a genuine, real 400 response from
			// CheckpointDurableExecution, not a client-side guess) -
			// this then got wrapped and checkpointed as a spurious
			// STEP/CONTEXT failure instead of the step actually
			// succeeding on its second, real attempt. executeResume
			// below re-runs fn and checkpoints SUCCEED/FAIL directly,
			// with NO START checkpoint of its own - correct, because
			// the START checkpoint already on record from the prior
			// invocation is the one covering this attempt; this
			// mirrors how retryOrFail's own post-delay re-execution
			// path (`executeAndCheckpoint(c, stepID, name, fn, cfg,
			// attempt)` after a WaitForOperation wakeup, further down
			// in this file) is likewise reached without any additional
			// START checkpoint - retryOrFail's retry path just happens
			// to have gotten there via an explicit RETRY checkpoint
			// rather than a bare crash, so it never needed this same
			// fix. AtMostOncePerRetry is unaffected: it never reaches
			// this branch at all (it returns via retryOrFail above,
			// which checkpoints FAIL or RETRY, never START, before its
			// own eventual re-entry into executeAndCheckpoint).
			return executeResume(c, stepID, name, fn, cfg, stepAttempt(existing))

		case types.OperationStatusPending, types.OperationStatusReady:
			// A retry delay was checkpointed in a prior invocation and
			// this is a FRESH invocation picking it back up - by the fact
			// that the backend re-invoked at all, it has already decided
			// the delay elapsed and the retry is eligible now. Re-execute
			// directly; there is no further waiting to do in THIS
			// invocation (retryOrFail's WaitForOperation call is what
			// suspended the PRIOR invocation that got us here in the
			// first place - see that function's doc).
			//
			// READY is included alongside PENDING as of the
			// WaitForCondition live-deployment investigation documented
			// in wait_for_condition.go's package-level bug writeup: the
			// real backend flips a RETRY-checkpointed operation to READY
			// (not PENDING) once the delay elapses and it re-invokes -
			// this was invisible for Step specifically because Step has
			// no carried-state-loss failure mode when this case is
			// missing (executeAndCheckpoint always re-runs fn fresh
			// regardless of what "attempt" argument it's given wrong),
			// but is fixed here too for correctness and consistency with
			// the confirmed real Java reference SDK's own
			// `case STARTED, READY -> resumeCheckLoop(...)` handling,
			// rather than leaving a latent, currently-benign-only-by-luck
			// gap in place.
			return executeAndCheckpoint(c, stepID, name, fn, cfg, stepAttempt(existing))
		}
	}

	return executeAndCheckpoint(c, stepID, name, fn, cfg, 0)
}

// executeAndCheckpoint runs fn once, checkpoints the outcome, and on
// failure either retries (recursing back into executeAndCheckpoint after
// checkpointing the retry delay) or returns the terminal error - mirroring
// the JS/Java step handlers' retry loop.
//
// START is checkpointed unconditionally, before fn runs, regardless of
// StepSemantics - see step-started-checkpoint fix (docs/remaining-work.md):
// this used to be conditional on cfg.semantics ==
// StepSemanticsAtMostOncePerRetry only, which meant the default
// AtLeastOncePerRetry semantics (the common case) never produced a
// StepStarted history event at all, only the terminal
// StepSucceeded/StepFailed - confirmed as a genuine, scoped bug via a
// real deployed conformance-harness function (requirement 1-1's actual
// GetDurableExecutionHistory response was ExecutionStarted ->
// StepSucceeded, with NO preceding StepStarted, contradicting the
// language-neutral conformance suite's own universal expectation that
// every operation checkpoints STARTED before its terminal event) and by
// cross-checking a freshly-invoked real execution of the deployed Java
// reference SDK's own simple-step-example, which DOES emit StepStarted
// for its plain, no-special-configuration steps. A grep across every
// other operation kind in this package (Wait, CreateCallback, Invoke,
// RunInChildContext, Map, Parallel, and all MAP_ITERATION/
// PARALLEL_BRANCH batch items) confirmed Step was the SOLE operation
// type in this codebase with conditional START checkpointing - not part
// of any broader, deliberate cross-operation design choice.
//
// This does mean every default-semantics step now pays one additional,
// fully synchronous (Enqueue blocks until backend-acknowledged - see
// checkpoint.Manager.Enqueue's own doc) checkpoint round trip before fn
// runs, which it did not pay before. That cost is real, but it is
// exactly the same cost every other operation kind in this codebase
// (Wait/Callback/Invoke/Context/Map/Parallel) already unconditionally
// pays today, and exactly the cost AtMostOncePerRetry steps already
// opted into - this change makes Step's cost profile CONSISTENT with
// the rest of the SDK's own established behavior, not a new outlier.
// AtMostOncePerRetry's own semantic guarantee (a crash between START and
// completion is observable as Started on replay, treated as an
// interruption rather than blindly re-run - see the
// OperationStatusStarted case in runStep) is unaffected: it already
// checkpointed START synchronously before fn; it now simply shares that
// same code path with the default semantics instead of gating it.
func executeAndCheckpoint[T any](c *dcontext.Context, stepID, name string, fn func(sc types.StepContext) (T, error), cfg *stepConfig[T], priorAttempt int) (T, error) {
	var zero T
	attempt := priorAttempt + 1

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       stepID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeStep,
		Name:     name,
		SubType:  subTypeStep,
		Action:   types.OperationActionStart,
	}); err != nil {
		return zero, fmt.Errorf("step %q (id %s): checkpointing start: %w", name, stepID, err)
	}

	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeStep}
	if attempt == 1 {
		// OnOperationStart fires once, for the operation's first attempt
		// only - OnOperationAttemptStart (below, inside
		// runFnAndCheckpointOutcome) fires for EVERY attempt, including
		// this first one, so firing OnOperationStart again on a later
		// retry attempt would be a duplicate "the operation started"
		// notification for an operation that, from a plugin's
		// perspective, already started on attempt 1.
		dispatchOperationStart(c, dinfo, attempt)
	}

	return runFnAndCheckpointOutcome(c, stepID, name, fn, cfg, attempt)
}

// executeResume re-runs fn for a step that was ALREADY checkpointed
// STARTED in a prior invocation (an AtLeastOncePerRetry step that
// crashed after START but before completion - see runStep's
// OperationStatusStarted case for the real, confirmed backend rejection
// this fixes: re-sending STEP/START on an already-STARTED step gets a
// genuine 400 InvalidParameterValueException, "Invalid current STEP
// state to start"). Unlike executeAndCheckpoint, this does NOT send its
// own START checkpoint - the one already on record from the interrupted
// attempt covers this resumed attempt too - it only runs fn and
// checkpoints the terminal SUCCEED/FAIL outcome, sharing that terminal-
// checkpoint logic with executeAndCheckpoint via
// runFnAndCheckpointOutcome.
func executeResume[T any](c *dcontext.Context, stepID, name string, fn func(sc types.StepContext) (T, error), cfg *stepConfig[T], priorAttempt int) (T, error) {
	attempt := priorAttempt + 1
	if attempt == 1 {
		dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeStep}
		dispatchOperationStart(c, dinfo, attempt)
	}
	return runFnAndCheckpointOutcome(c, stepID, name, fn, cfg, attempt)
}

// runFnAndCheckpointOutcome runs fn once (at the given attempt number)
// and checkpoints its terminal outcome - the shared tail of both
// executeAndCheckpoint (which checkpoints START first) and executeResume
// (which does not, because a START checkpoint already exists on record
// from a prior, interrupted invocation).
func runFnAndCheckpointOutcome[T any](c *dcontext.Context, stepID, name string, fn func(sc types.StepContext) (T, error), cfg *stepConfig[T], attempt int) (T, error) {
	var zero T

	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeStep}
	dispatchOperationAttemptStart(c, dinfo, attempt)

	sc := dcontext.NewStepContext(c, stepID, name, attempt)
	wrapped, err := wrapOperationAttemptFn(c, dinfo, attempt, func() (any, error) {
		return fn(sc)
	})
	var result T
	if wrapped != nil {
		result, _ = wrapped.(T)
	}
	if err != nil {
		dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", err)
		return retryOrFail(c, stepID, name, fn, cfg, err, attempt)
	}

	serialized, serErr := cfg.serdes.Serialize(result, stepID, c.ExecutionARN())
	if serErr != nil {
		dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", serErr)
		return zero, newSerdesError("serialize", stepID, name, serErr)
	}

	// docs/remaining-work.md §6 task 16: reject an oversized single-step
	// result BEFORE checkpointing it, rather than sending it and letting
	// the raw backend call fail unhelpfully (or, for a custom Serdes with
	// its own internal batching, silently misbehaving in some
	// unconfirmed way) - see checkResultSize's doc in errors.go for the
	// full research this is based on.
	if err := checkResultSize(serialized, OperationKindStep, stepID, name); err != nil {
		dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", err)
		return zero, err
	}

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       stepID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeStep,
		Name:     name,
		SubType:  subTypeStep,
		Action:   types.OperationActionSucceed,
		Payload:  &serialized,
	}); err != nil {
		dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", err)
		return zero, fmt.Errorf("step %q (id %s): checkpointing success: %w", name, stepID, err)
	}

	dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeSucceeded, serialized, nil)
	dispatchOperationEnd(c, dinfo, attempt, plugin.OperationStatusSucceeded, serialized, nil)

	// Return the serdes ROUND-TRIP of the result, not the raw in-memory
	// value - see roundTripSerialized's doc for why (matches the JS
	// reference SDK's step-handler.ts, which returns
	// safeDeserialize(serdes, serializedResult) here; conformance 1-6).
	return roundTripSerialized[T](cfg.serdes, serialized, stepID, name, c.ExecutionARN())
}

func retryOrFail[T any](c *dcontext.Context, stepID, name string, fn func(sc types.StepContext) (T, error), cfg *stepConfig[T], stepErr error, attempt int) (T, error) {
	var zero T
	decision := cfg.retryStrategy(stepErr, attempt)

	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeStep}

	opErr := checkpointErrorObject(stepErr)
	if !decision.ShouldRetry {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       stepID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeStep,
			Name:     name,
			SubType:  subTypeStep,
			Action:   types.OperationActionFail,
			Error:    opErr,
		}); err != nil {
			return zero, fmt.Errorf("step %q (id %s): checkpointing failure: %w", name, stepID, err)
		}
		dispatchOperationEnd(c, dinfo, attempt, plugin.OperationStatusFailed, "", stepErr)
		return zero, &StepFailedError{
			OperationError: &OperationError{Kind: OperationKindStep, ID: stepID, Name: name, Err: stepErr},
			Attempt:        attempt,
		}
	}

	delaySeconds := 1
	if decision.Delay != nil {
		delaySeconds = decision.Delay.Days*86400 + decision.Delay.Hours*3600 + decision.Delay.Minutes*60 + decision.Delay.Seconds
	}

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:          stepID,
		ParentID:    c.ParentStepID(),
		Type:        types.OperationTypeStep,
		Name:        name,
		SubType:     subTypeStep,
		Action:      types.OperationActionRetry,
		Error:       opErr,
		StepOptions: &types.StepOptions{NextAttemptDelaySeconds: &delaySeconds},
	}); err != nil {
		return zero, fmt.Errorf("step %q (id %s): checkpointing retry: %w", name, stepID, err)
	}

	// Suspend for the retry delay exactly like Wait suspends for its
	// duration: a real backend enforces NextAttemptDelaySeconds itself
	// (the checkpointed retry becomes eligible for re-execution only once
	// the delay elapses - see StepDetails.NextAttemptTimestamp), so this
	// invocation has nothing useful to do in the meantime. Blocking via
	// WaitForOperation lets execmgr.Manager decide: if this is the only
	// active goroutine, the whole invocation suspends (returns PENDING)
	// and the backend re-invokes later once the delay has passed. A test
	// runner with SkipTime enabled resolves the retry synchronously
	// instead (see inmemory_client.go's OperationActionRetry handling),
	// so WaitForOperation returns immediately in that case rather than
	// actually suspending.
	//
	// A zero-second delay is the one case that skips this entirely:
	// there is nothing to wait out, so re-executing immediately in this
	// same invocation is both correct and strictly better than paying a
	// suspend/re-invoke round trip for no reason. This is the documented
	// "retry immediately" idiom used throughout this SDK's own tests
	// (types.Duration{Seconds: 0}, e.g. utils.Presets.FixedDelay's
	// zero-delay form) and is a real, observable distinction a caller
	// can rely on - not just a test convenience.
	if delaySeconds > 0 {
		if _, ok := c.ExecManager().WaitForOperation(stepID); !ok {
			return zero, errSuspended
		}

		// Re-fetch the checkpointed state after waking: a test runner may
		// have re-invoked the handler from the top (driveToCompletion's
		// re-invocation loop) rather than resolving this WaitForOperation
		// call in-process, in which case runStep's OperationStatusPending
		// branch - not this code path - is what actually re-executes fn
		// on the next invocation. Only fall through to a direct
		// re-execution here if WaitForOperation resolved in THIS process
		// without a fresh invocation (e.g. a future backend/runner that
		// flips Pending to Started/eligible without a full re-invoke).
		if existing, found := c.ExecManager().GetOperation(stepID); found {
			switch existing.Status {
			case types.OperationStatusSucceeded:
				return deserializeStepResult[T](cfg.serdes, existing, stepID)
			case types.OperationStatusFailed:
				return zero, stepError(existing, name)
			}
		}
	}
	return executeAndCheckpoint(c, stepID, name, fn, cfg, attempt)
}

func deserializeStepResult[T any](serdes types.Serdes, op types.Operation, stepID string) (T, error) {
	var zero T
	if op.StepDetails == nil || op.StepDetails.Result == nil {
		return zero, fmt.Errorf("step %s: succeeded but no result recorded", stepID)
	}
	val, err := serdes.Deserialize(*op.StepDetails.Result, stepID, "")
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, err)
	}
	typed, ok := val.(T)
	if ok {
		return typed, nil
	}
	// Fall back to a JSON re-marshal/unmarshal round trip for serdes
	// implementations (like the default JSON one) that return an
	// untyped any (typically map[string]any) rather than the concrete T.
	b, err := json.Marshal(val)
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	return typed, nil
}

func stepError(op types.Operation, name string) error {
	if op.StepDetails != nil && op.StepDetails.Error != nil {
		return &StepFailedError{
			OperationError: &OperationError{Kind: OperationKindStep, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.StepDetails.Error.ErrorMessage)},
			Attempt:        stepAttempt(op),
		}
	}
	return &StepFailedError{
		OperationError: &OperationError{Kind: OperationKindStep, ID: op.ID, Name: name, Err: fmt.Errorf("failed with no recorded error")},
		Attempt:        stepAttempt(op),
	}
}

// stepAttempt safely extracts the current attempt count from a
// checkpointed step operation, defaulting to 0 if StepDetails is absent
// (should not happen for a STEP-type operation, but avoids a nil-pointer
// panic if the backend ever omits it).
func stepAttempt(op types.Operation) int {
	if op.StepDetails == nil {
		return 0
	}
	return op.StepDetails.Attempt
}

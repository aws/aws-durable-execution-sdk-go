package operations

import (
	"encoding/json"
	"fmt"

	dcontext "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// ConditionOption configures a WaitForCondition invocation.
type ConditionOption[TState any] func(*conditionConfig[TState])

type conditionConfig[TState any] struct {
	retryStrategy func(err error, attempt int) types.RetryDecision
	serdes        types.Serdes
}

// WithConditionRetryStrategy sets the polling backoff strategy used
// between checks while the condition has not yet been met. Named
// "retry" rather than "wait" strategy to match the confirmed real-backend
// flowchart exactly (internal SDK Operation Diagrams design doc,
// "WaitForCondition"): WaitForCondition checkpoints
// STEP/WAIT_FOR_CONDITION RETRY with a backoff, structurally identical to
// Step's own retry loop, just triggered by "conditionMet=false" rather
// than "checkFn returned an error." Reuses types.RetryDecision (the same
// type Step's retry strategies return) rather than introducing a
// separate WaitStrategyResult type, since the two are functionally
// identical.
func WithConditionRetryStrategy[TState any](fn func(err error, attempt int) types.RetryDecision) ConditionOption[TState] {
	return func(c *conditionConfig[TState]) { c.retryStrategy = fn }
}

// WithConditionSerdes sets a custom serializer for the polled state.
func WithConditionSerdes[TState any](s types.Serdes) ConditionOption[TState] {
	return func(c *conditionConfig[TState]) { c.serdes = s }
}

// ConditionResult is returned by a WaitForCondition check function on
// each poll: the current state, and whether the condition has been met
// (ending the poll loop successfully). Introduced as an explicit result
// type - the original scaffold's checkFn signature (state, error) had no
// way to distinguish "not yet met, keep polling" from "met," which is a
// real, distinct outcome from "the check itself failed" per the
// confirmed flowchart's CheckResult branch ("Did the check function
// succeed with conditionMet=true?").
type ConditionResult[TState any] struct {
	State        TState
	ConditionMet bool
}

// WaitForCondition repeatedly invokes checkFn, checkpointing the returned
// state after each call, until checkFn reports ConditionMet: true or the
// configured retry strategy gives up. Polling delays between checks
// suspend the whole invocation exactly like Step's retry delays do (see
// step.go's retryOrFail doc) when the delay is non-zero; a zero-second
// delay re-polls immediately in-process instead.
//
// initialState seeds the first invocation of checkFn and must be provided
// explicitly (there is no zero-value inference, since a valid "not yet
// started" state is domain-specific).
//
// # Implementation notes
//
// Mirrors the confirmed real-backend flowchart exactly (internal SDK
// Operation Diagrams design doc, "WaitForCondition"): checkpoints
// STEP/WAIT_FOR_CONDITION START, runs checkFn, and on
// ConditionMet: true checkpoints STEP/WAIT_FOR_CONDITION SUCCEED; on
// ConditionMet: false (or an error) consults the retry strategy exactly
// like Step's own retry loop, checkpointing
// STEP/WAIT_FOR_CONDITION RETRY or FAILED. This is intentionally a very
// close structural port of Step (see step.go) - the flowchart documents
// it as literally the same state machine with the SubType changed and
// the retry trigger being "conditionMet=false" instead of "checkFn threw
// an error," rather than a distinct primitive.
func WaitForCondition[TState any](dc types.DurableContext, id string, checkFn func(sc types.StepContext, state TState) (ConditionResult[TState], error), initialState TState, opts ...ConditionOption[TState]) (TState, error) {
	cfg := &conditionConfig[TState]{serdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.retryStrategy == nil {
		cfg.retryStrategy = utils.Presets.NoRetry()
	}

	c, ok := dc.(*dcontext.Context)
	if !ok {
		return initialState, fmt.Errorf("operations.WaitForCondition: dc must be created by this SDK's runtime (got %T)", dc)
	}

	stepID := c.NextStepID()
	return runWaitForCondition(c, stepID, id, checkFn, initialState, cfg)
}

// errConditionNotMet is a sentinel wrapped into the error passed to the
// retry strategy when checkFn succeeds but reports ConditionMet: false,
// so the SAME retry-strategy function signature Step uses
// (func(err error, attempt int) types.RetryDecision) can drive
// WaitForCondition's polling too, without needing a second strategy
// shape - matching this operation's design as "Step's retry loop with a
// different trigger," not a parallel state machine.
type errConditionNotMet struct{}

func (errConditionNotMet) Error() string { return "condition not yet met" }

func runWaitForCondition[TState any](c *dcontext.Context, stepID, name string, checkFn func(sc types.StepContext, state TState) (ConditionResult[TState], error), initialState TState, cfg *conditionConfig[TState]) (TState, error) {
	if existing, found := c.ExecManager().GetOperation(stepID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// STEP with SubType WAIT_FOR_CONDITION before reading StepDetails
		// off of it below - a plain Step (SubType=="") checkpointed at
		// this same step ID in a prior deployment would otherwise be
		// silently misread here exactly like the reverse case (Wait
		// reading a former Step's data) that motivated this task.
		if err := checkReplayConsistency(existing, types.OperationTypeStep, subTypeWaitForCondition, OperationKindCondition, stepID, name); err != nil {
			return initialState, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeConditionState[TState](cfg.serdes, existing, stepID)
		case types.OperationStatusFailed:
			return initialState, conditionError(existing, name)
		case types.OperationStatusPending, types.OperationStatusReady:
			// SECOND real bug, confirmed via the SAME live deployment
			// that surfaced this file's RETRY/Payload+Error bug (see
			// this file's package-level writeup): the real backend does
			// NOT re-invoke with this operation still checkpointed as
			// PENDING once a poll delay elapses - it flips the
			// checkpointed operation to READY (types.OperationStatus's
			// eighth, backend-confirmed value - see wire.go's own doc on
			// how that value was discovered) and re-invokes with THAT
			// status instead. Before this fix, READY fell through this
			// switch entirely (there was no case for it) and hit this
			// function's own final fallback statement below -
			// `pollAndCheckpoint(..., initialState, cfg, 0)` - silently
			// RESTARTING the poll loop from the caller's initialState
			// and attempt 0 on every single re-invocation, rather than
			// resuming from the last-checkpointed state and attempt
			// count. This was invisible against every local test double
			// in this repo BEFORE this fix (fakeClient/inMemoryClient
			// both only ever produced PENDING, never READY, for a
			// RETRY-checkpointed operation) and invisible in step.go's
			// own structurally-identical switch (which has the exact
			// same missing-READY-case gap) purely because a plain Step
			// has no carried state to lose when it silently restarts
			// attempt numbering - WaitForCondition's carried poll state
			// is what makes this bug's effect observable at all, and it
			// was: a real deployment of this exact code polled the
			// simulated carrier status every 5 seconds for the full
			// 300-second durable-execution timeout, every single poll
			// logging pollCount:1/attempt:1, because each fresh
			// invocation's READY status was silently ignored and the
			// poll state reset to zero every time - the condition
			// (pollCount >= 3) could never be reached. Both test doubles
			// have since been fixed to model the real READY transition
			// (inMemoryClient's skipTime-driven RETRY handling in
			// testing/inmemory_client.go) so this exact class of bug is
			// now caught locally, under `go test`, not just in
			// production - see that file's own comment for the fix.
			// Confirmed correct against the Java reference SDK's actual
			// source (WaitForConditionOperation.java's `replay`
			// override): `case STARTED, READY -> resumeCheckLoop(existing);`
			// treats READY identically to STARTED (resume using the
			// checkpointed state), and `case PENDING -> pollReadyAndResumeCheckLoop(existing)`
			// treats PENDING as "poll until READY, THEN resume" - i.e.
			// Java's own model confirms PENDING and READY both
			// ultimately resume from the checkpointed state, just at
			// different points in the READY-polling cycle; this Go SDK
			// receives a fresh invocation only once the backend ITSELF
			// has already finished that READY-polling on its own side,
			// so by the time this code runs, both statuses mean exactly
			// the same thing: "resume now, using what's checkpointed."
			lastState, err := deserializeConditionState[TState](cfg.serdes, existing, stepID)
			if err != nil {
				return initialState, err
			}
			return pollAndCheckpoint(c, stepID, name, checkFn, lastState, cfg, conditionAttempt(existing))
		}
		// STARTED: an interrupted attempt from a prior invocation that
		// crashed before completion. WaitForCondition, like Step's
		// AtLeastOncePerRetry semantics (its only supported mode - there
		// is no AtMostOnce equivalent here, since checkFn is expected to
		// be a read-only, idempotent check by nature, unlike a Step body
		// which may have side effects), simply re-runs checkFn against
		// the last-checkpointed state - WITHOUT re-checkpointing START,
		// via resumeAfterInterruption below. This mirrors step.go's own
		// AtLeastOncePerRetry STARTED branch (runStep) and the exact,
		// real, confirmed-via-live-deployment bug documented there: a
		// second START checkpoint against an already-STARTED operation
		// is rejected outright by the real backend
		// (InvalidParameterValueException: "Invalid current STEP state
		// to start"). This particular branch was not itself exercised by
		// this suite's 13 requirements (none of them crash mid-poll), but
		// now that pollAndCheckpoint unconditionally checkpoints START
		// (see that function's own bug writeup, found via THIS suite),
		// leaving this branch falling through to pollAndCheckpoint
		// directly - as it did before that fix, when pollAndCheckpoint
		// sent no START at all - would have silently introduced the exact
		// same class of regression here, for a path this session's own
		// testing cannot reach. Fixed proactively rather than left as a
		// latent trap, mirroring Step's own already-fixed precedent
		// exactly.
		lastState, err := deserializeConditionState[TState](cfg.serdes, existing, stepID)
		if err != nil {
			return initialState, err
		}
		return resumeAfterInterruption(c, stepID, name, checkFn, lastState, cfg, conditionAttempt(existing))
	}

	return pollAndCheckpoint(c, stepID, name, checkFn, initialState, cfg, 0)
}

// resumeAfterInterruption re-runs checkFn for a poll attempt that was
// ALREADY checkpointed STARTED in a prior invocation (a crash between
// START and completion) - mirroring step.go's executeResume exactly (see
// that function's own doc, and pollAndCheckpoint's bug writeup above for
// why this needed its own entry point once pollAndCheckpoint itself
// started unconditionally checkpointing START). Delegates directly to
// checkAndCheckpointOutcome - the same outcome-checkpointing tail
// pollAndCheckpoint itself uses after its own START checkpoint - without
// sending a START checkpoint of its own.
func resumeAfterInterruption[TState any](c *dcontext.Context, stepID, name string, checkFn func(sc types.StepContext, state TState) (ConditionResult[TState], error), state TState, cfg *conditionConfig[TState], priorAttempt int) (TState, error) {
	attempt := priorAttempt + 1
	if attempt == 1 {
		dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeWaitForCondition}
		dispatchOperationStart(c, dinfo, attempt)
	}
	return checkAndCheckpointOutcome(c, stepID, name, checkFn, state, cfg, attempt)
}

// pollAndCheckpoint checkpoints START, runs checkFn once against state,
// checkpoints the outcome, and either returns (condition met or retries
// exhausted) or recurses to poll again (condition not yet met and the
// retry strategy says to continue) - mirroring step.go's
// executeAndCheckpoint/retryOrFail loop structure exactly, since this
// operation is that same state machine with a different termination
// predicate (see this file's top-level doc).
//
// REAL BUG, found and fixed via this conformance suite (requirements
// 6-1 through 6-13, every one of which independently surfaced the same
// symptom): this function used to skip the START checkpoint entirely,
// going straight from "runWaitForCondition/pollAndCheckpoint entered"
// to checkFn's first call with NO StepStarted event ever checkpointed -
// for the FIRST attempt as well as every subsequent poll. This was not
// merely a cosmetic gap: because errConditionNotMet (checkFn succeeding
// with ConditionMet: false) drives the SAME retry-strategy machinery a
// genuine checkFn error would, and the zero-option default retry
// strategy is utils.Presets.NoRetry() (see this file's cfg
// initialization) - EVERY WaitForCondition call with no explicit retry
// strategy configured (e.g. this suite's own 6-1, 6-3, 6-4, 6-9, 6-10,
// 6-12, 6-13 requirements) failed immediately on its very first poll
// with a StepFailed carrying errConditionNotMet's own "condition not
// yet met" message, rather than ever reaching a second attempt - a real
// deployment against a live account (730758745077, us-east-1) confirmed
// this directly: 6-1's actual GetDurableExecutionHistory response
// showed StepFailed as EventId 2 (immediately after ExecutionStarted),
// not the StepStarted/StepSucceeded/.../StepStarted/StepSucceeded
// sequence every requirement's own YAML documents.
//
// Every other operation kind in this package (Step, Wait, CreateCallback,
// Invoke, RunInChildContext, Map/Parallel's batch items) unconditionally
// checkpoints START before doing its own work - WaitForCondition was the
// sole exception, and unlike Step (which correctly re-checkpoints START
// on every fresh cross-invocation retry attempt via executeAndCheckpoint,
// confirmed by cross-checking the Step suite's own 1-11 requirement,
// which asserts a SECOND StepStarted, EventId 5, after its retry delay
// elapses - see step.go's retryOrFail tail, which re-enters
// executeAndCheckpoint rather than executeResume), this function had NO
// START checkpoint at any point at all, for any attempt. Confirmed
// correct against the same Java reference SDK source already cited
// elsewhere in this file (WaitForConditionOperation.java): its own
// checkLoop equivalent checkpoints a START-equivalent event before every
// single check invocation, not just the first - i.e. per-poll START
// checkpointing, exactly matching what this suite's own YAMLs (6-1
// through 6-13, e.g. 6-1's EventId 2/5/8 all being StepStarted under the
// same operation Id) independently demonstrate. Fixed by checkpointing
// START unconditionally at the top of this function - since
// pollAndCheckpoint is both the fresh-first-attempt entry point AND the
// recursive per-poll re-entry point (called again after each RETRY
// checkpoint's suspend/resume, whether resolved in-process or via a
// fresh invocation's OperationStatusPending/Ready replay branch in
// runWaitForCondition above), this one change correctly produces exactly
// one StepStarted immediately before every StepSucceeded/StepFailed
// pair, for every attempt, matching every requirement's own
// ExpectedExecutionHistory.
func pollAndCheckpoint[TState any](c *dcontext.Context, stepID, name string, checkFn func(sc types.StepContext, state TState) (ConditionResult[TState], error), state TState, cfg *conditionConfig[TState], priorAttempt int) (TState, error) {
	attempt := priorAttempt + 1

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       stepID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeStep,
		Name:     name,
		SubType:  subTypeWaitForCondition,
		Action:   types.OperationActionStart,
	}); err != nil {
		return state, fmt.Errorf("waitForCondition %q (id %s): checkpointing start: %w", name, stepID, err)
	}

	if attempt == 1 {
		// OnOperationStart fires once, for the first poll only - see
		// step.go's executeAndCheckpoint for why (OnOperationAttemptStart,
		// fired unconditionally inside checkAndCheckpointOutcome below,
		// covers every individual poll attempt including this one).
		dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeWaitForCondition}
		dispatchOperationStart(c, dinfo, attempt)
	}

	return checkAndCheckpointOutcome(c, stepID, name, checkFn, state, cfg, attempt)
}

// checkAndCheckpointOutcome runs checkFn once (at the given attempt
// number) against state and checkpoints its terminal/continue outcome -
// the shared tail of both pollAndCheckpoint (which checkpoints START
// first) and resumeAfterInterruption (which does not, because a START
// checkpoint already exists on record from a prior, interrupted
// invocation) - mirroring step.go's runFnAndCheckpointOutcome/
// executeAndCheckpoint/executeResume split exactly.
func checkAndCheckpointOutcome[TState any](c *dcontext.Context, stepID, name string, checkFn func(sc types.StepContext, state TState) (ConditionResult[TState], error), state TState, cfg *conditionConfig[TState], attempt int) (TState, error) {
	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeStep, SubType: subTypeWaitForCondition}
	dispatchOperationAttemptStart(c, dinfo, attempt)

	sc := dcontext.NewStepContext(c, stepID, name, attempt)
	wrapped, err := wrapOperationAttemptFn(c, dinfo, attempt, func() (any, error) {
		return checkFn(sc, state)
	})
	var result ConditionResult[TState]
	if wrapped != nil {
		result, _ = wrapped.(ConditionResult[TState])
	}

	if err == nil && result.ConditionMet {
		serialized, serErr := cfg.serdes.Serialize(result.State, stepID, c.ExecutionARN())
		if serErr != nil {
			dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", serErr)
			return state, newSerdesError("serialize", stepID, name, serErr)
		}
		// docs/remaining-work.md §6 task 16: reject an oversized
		// condition-state result before checkpointing it - see
		// checkResultSize's doc in errors.go.
		if err := checkResultSize(serialized, OperationKindCondition, stepID, name); err != nil {
			dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", err)
			return state, err
		}
		if ckErr := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       stepID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeStep,
			Name:     name,
			SubType:  subTypeWaitForCondition,
			Action:   types.OperationActionSucceed,
			Payload:  &serialized,
		}); ckErr != nil {
			dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", ckErr)
			return state, fmt.Errorf("waitForCondition %q (id %s): checkpointing success: %w", name, stepID, ckErr)
		}
		dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeSucceeded, serialized, nil)
		dispatchOperationEnd(c, dinfo, attempt, plugin.OperationStatusSucceeded, serialized, nil)
		// Return the serdes ROUND-TRIP of the final state, not the raw
		// in-memory value - see roundTripSerialized's doc (matches the
		// JS reference SDK's wait-for-condition-handler.ts, which
		// returns its own deserializedState here).
		return roundTripSerialized[TState](cfg.serdes, serialized, stepID, name, c.ExecutionARN())
	}

	checkErr := err
	if checkErr == nil {
		// checkFn succeeded but ConditionMet was false - drive the SAME
		// retry-strategy function Step uses via the errConditionNotMet
		// sentinel (see that type's doc for why).
		checkErr = errConditionNotMet{}
	}
	dispatchOperationAttemptEnd(c, dinfo, attempt, plugin.AttemptOutcomeFailed, "", checkErr)

	decision := cfg.retryStrategy(checkErr, attempt)

	// Serialize the latest state up front - RETRY needs it as Payload
	// (see below), and computing it once here keeps both branches
	// (RETRY/FAIL) consistent even though FAIL itself no longer sends it
	// over the wire (see that branch's own comment for why).
	serialized, serErr := cfg.serdes.Serialize(result.State, stepID, c.ExecutionARN())
	if serErr != nil {
		return state, newSerdesError("serialize", stepID, name, serErr)
	}

	// docs/remaining-work.md §6 task 16: reject an oversized
	// condition-state result before checkpointing it here too - see
	// checkResultSize's doc in errors.go.
	if err := checkResultSize(serialized, OperationKindCondition, stepID, name); err != nil {
		return state, err
	}

	if !decision.ShouldRetry {
		// IMPORTANT: do NOT set Payload on this terminal FAIL checkpoint,
		// even though `serialized` (the last-observed state) is already
		// computed above. This mirrors all three reference SDKs'
		// confirmed real behavior for their own FAIL-equivalent
		// checkpoint on this operation (TypeScript's
		// wait-for-condition-handler.ts, Python's
		// operation/wait_for_condition.py's create_wait_for_condition_fail,
		// Java's WaitForConditionOperation.handleCheckFailure): none of
		// them send a payload alongside Error on the terminal failure
		// path - only Error. The official API Reference's own wording
		// for OperationUpdate.Payload ("The payload for successful
		// operations") is consistent with this: a FAIL is not a
		// successful operation, so Payload has no defined meaning there
		// either. This was not independently confirmed to be REJECTED
		// by the real backend the way RETRY+Payload+Error was (that
		// specific combination is what actually failed against the live
		// endpoint this session - see this file's package-level bug
		// writeup) - but sending it would deviate from every reference
		// SDK's own established, working behavior for no confirmed
		// benefit, and the state is inherently unrecoverable-and-moot
		// once the operation has terminally failed anyway (there is no
		// further poll attempt to seed with it - conditionError/
		// deserializeConditionState are never called against a FAILED
		// operation's state on this path).
		opErr := checkpointErrorObject(checkErr)
		if ckErr := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       stepID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeStep,
			Name:     name,
			SubType:  subTypeWaitForCondition,
			Action:   types.OperationActionFail,
			Error:    opErr,
		}); ckErr != nil {
			return state, fmt.Errorf("waitForCondition %q (id %s): checkpointing failure: %w", name, stepID, ckErr)
		}
		dispatchOperationEnd(c, dinfo, attempt, plugin.OperationStatusFailed, "", checkErr)
		// CheckErr distinguishes "checkFn itself errored" from
		// "checkFn succeeded but the condition was never met" - see
		// ConditionFailedError's doc. errConditionNotMet is an
		// SDK-internal sentinel (see its own doc) never surfaced as
		// CheckErr; a genuine checkFn error is.
		var checkFnErr error
		if err != nil {
			checkFnErr = err
		}
		return result.State, &ConditionFailedError{
			OperationError: &OperationError{Kind: OperationKindCondition, ID: stepID, Name: name, Err: checkErr},
			Attempt:        attempt,
			CheckErr:       checkFnErr,
		}
	}

	delaySeconds := 1
	if decision.Delay != nil {
		delaySeconds = decision.Delay.Days*86400 + decision.Delay.Hours*3600 + decision.Delay.Minutes*60 + decision.Delay.Seconds
	}

	// IMPORTANT: do NOT set Error on this RETRY checkpoint, even though
	// checkErr is non-nil here (either a genuine checkFn error being
	// retried, or the errConditionNotMet sentinel - see this function's
	// call site above). This was a REAL bug, confirmed via a live
	// deployment failure (examples/map-with-condition-and-callback-go
	// invoked against a real Lambda function, account 730758745077,
	// us-east-1): the real backend rejects a RETRY action that carries
	// both Payload and Error with
	// "InvalidParameterValueException: Cannot provide both an Error and
	// Payload for RETRY action." This operation fundamentally needs
	// Payload on RETRY (to carry the polled state forward to the next
	// attempt - see the OperationStatusPending replay branch above,
	// which reads it back via deserializeConditionState), so Error is
	// what has to be dropped, not Payload - the reverse of Step's own
	// RETRY checkpoint (step.go's retryOrFail), which sends Error but
	// never Payload, since a plain Step has no state to carry between
	// retries at all.
	//
	// Confirmed correct against all three reference SDKs' actual source
	// (not guessed): TypeScript's wait-for-condition-handler.ts, Python's
	// operation/wait_for_condition.py, and Java's
	// WaitForConditionOperation.java ALL send Payload (the serialized
	// state) but OMIT Error entirely on their RETRY-equivalent
	// checkpoint call for this operation - "condition not yet met" is
	// never modeled as an Error at the wire level for WaitForCondition,
	// unlike a genuine checkFn exception, which all three (and this SDK,
	// see the terminal FAIL path below) correctly DO checkpoint with
	// Error set (and no Payload) once the retry strategy gives up rather
	// than retrying again. This is also consistent with the official AWS
	// API Reference's own wording for OperationUpdate.Payload ("The
	// payload for successful operations") and StepDetails.Result ("The
	// JSON response payload") - the backend's RETRY/Payload combination
	// is specifically how WaitForCondition's carried-state design is
	// meant to work, and Error is reserved for FAIL (and, for plain
	// Step, RETRY too, since Step has no payload to send instead).
	//
	// checkErr itself (the retry strategy's input) is still used above
	// to decide ShouldRetry/Delay, and is preserved for the CALLER via
	// ConditionFailedError.CheckErr on the eventual terminal FAILURE
	// path (see below) - it is only omitted from THIS particular
	// wire-level checkpoint call, not lost from the SDK's own error
	// reporting.
	if ckErr := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:          stepID,
		ParentID:    c.ParentStepID(),
		Type:        types.OperationTypeStep,
		Name:        name,
		SubType:     subTypeWaitForCondition,
		Action:      types.OperationActionRetry,
		Payload:     &serialized,
		StepOptions: &types.StepOptions{NextAttemptDelaySeconds: &delaySeconds},
	}); ckErr != nil {
		return state, fmt.Errorf("waitForCondition %q (id %s): checkpointing retry: %w", name, stepID, ckErr)
	}

	// Suspend for the poll delay exactly like Step's retry delay suspends
	// (see step.go's retryOrFail doc for the full reasoning) - a real
	// backend enforces NextAttemptDelaySeconds itself, so this invocation
	// has nothing useful to do until it elapses. A zero-second delay
	// skips suspension entirely and re-polls immediately in-process,
	// matching Step's same special case.
	if delaySeconds > 0 {
		if _, ok := c.ExecManager().WaitForOperation(stepID); !ok {
			return state, errSuspended
		}
		if existing, found := c.ExecManager().GetOperation(stepID); found {
			switch existing.Status {
			case types.OperationStatusSucceeded:
				return deserializeConditionState[TState](cfg.serdes, existing, stepID)
			case types.OperationStatusFailed:
				return state, conditionError(existing, name)
			}
		}
	}
	return pollAndCheckpoint(c, stepID, name, checkFn, result.State, cfg, attempt)
}

func deserializeConditionState[TState any](serdes types.Serdes, op types.Operation, stepID string) (TState, error) {
	var zero TState
	if op.StepDetails == nil || op.StepDetails.Result == nil {
		return zero, fmt.Errorf("waitForCondition %s: no state recorded", stepID)
	}
	val, err := serdes.Deserialize(*op.StepDetails.Result, stepID, "")
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, err)
	}
	typed, ok := val.(TState)
	if ok {
		return typed, nil
	}
	// Fall back to a JSON re-marshal/unmarshal round trip, matching
	// Step's deserializeStepResult - see that function's doc for why.
	b, err := json.Marshal(val)
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed state to %T: %w", zero, err))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed state to %T: %w", zero, err))
	}
	return typed, nil
}

func conditionError(op types.Operation, name string) error {
	if op.StepDetails != nil && op.StepDetails.Error != nil {
		return &ConditionFailedError{
			OperationError: &OperationError{Kind: OperationKindCondition, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.StepDetails.Error.ErrorMessage)},
			Attempt:        conditionAttempt(op),
		}
	}
	return &ConditionFailedError{
		OperationError: &OperationError{Kind: OperationKindCondition, ID: op.ID, Name: name, Err: fmt.Errorf("failed with no recorded error")},
		Attempt:        conditionAttempt(op),
	}
}

// conditionAttempt safely extracts the current attempt count from a
// checkpointed WAIT_FOR_CONDITION operation, mirroring step.go's
// stepAttempt.
func conditionAttempt(op types.Operation) int {
	if op.StepDetails == nil {
		return 0
	}
	return op.StepDetails.Attempt
}

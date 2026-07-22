package operations

import (
	"encoding/json"
	"errors"
	"fmt"

	dcontext "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// InvokeOption configures an Invoke call.
type InvokeOption[TIn, TOut any] func(*invokeConfig[TIn, TOut])

type invokeConfig[TIn, TOut any] struct {
	serdes   types.Serdes
	tenantID string
}

// WithInvokeSerdes sets a custom serializer for the invocation's input and
// output payloads.
func WithInvokeSerdes[TIn, TOut any](s types.Serdes) InvokeOption[TIn, TOut] {
	return func(c *invokeConfig[TIn, TOut]) { c.serdes = s }
}

// WithInvokeTenantID sets ChainedInvokeOptions.TenantID on this call's
// CHAINED_INVOKE/START checkpoint, for tenant-isolated invocation of the
// target function. The wire plumbing for this already existed end-to-end
// before this option was added (types.ChainedInvokeOptions.TenantID,
// awssdk/client.go's marshaling, and client_test.go's own coverage of it)
// - only the public InvokeOption to actually set it from a caller was
// missing (see this function's own prior doc note, now resolved: "not yet
// exposed as an InvokeOption - no example or use case has needed it yet",
// added for conformance requirement invoke/5-8, which exercises TenantId
// appearing verbatim in ChainedInvokeStartedDetails).
func WithInvokeTenantID[TIn, TOut any](tenantID string) InvokeOption[TIn, TOut] {
	return func(c *invokeConfig[TIn, TOut]) { c.tenantID = tenantID }
}

// Invoke calls another Lambda function - durable or standard - identified
// by functionARN, passing input, and durably waits for its result. The
// call is idempotent across replays: on replay, if the invocation already
// completed, the checkpointed result is returned without re-invoking the
// target function.
//
// # Implementation notes
//
// Mirrors the confirmed real-backend flowchart exactly (internal SDK
// Operation Diagrams design doc, "Invoke"): checkpoint
// CHAINED_INVOKE/CHAINED_INVOKE START (a single checkpoint - the backend
// itself is responsible for actually calling the target function and
// reporting the outcome back onto this operation; the SDK's job is only
// to register the request and await the result), then block via
// ExecManager.WaitForOperation exactly like Wait/CreateCallback. Per the
// flowchart, the awaited promise can resolve as Succeeded, Failed,
// TimedOut, or Stopped - all four are terminal outcomes for a
// CHAINED_INVOKE operation (see execmgr.Manager.WaitForOperation's doc,
// fixed this session to treat all of types.OperationStatus.IsTerminal()
// as terminal rather than hardcoding just Succeeded/Failed, specifically
// because Invoke needed TimedOut/Stopped handled correctly here).
//
// TenantID (ChainedInvokeOptions.TenantID) is exposed via
// WithInvokeTenantID below, for tenant-isolated invocation of the target
// function.
func Invoke[TIn, TOut any](dc types.DurableContext, id string, functionARN string, input TIn, opts ...InvokeOption[TIn, TOut]) (TOut, error) {
	cfg := &invokeConfig[TIn, TOut]{serdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}

	var zero TOut
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return zero, fmt.Errorf("operations.Invoke: dc must be created by this SDK's runtime (got %T)", dc)
	}

	stepID := c.NextStepID()

	if existing, found := c.ExecManager().GetOperation(stepID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CHAINED_INVOKE before reading ChainedInvokeDetails off of it
		// below.
		if err := checkReplayConsistency(existing, types.OperationTypeChainedInvoke, "", OperationKindInvoke, stepID, id); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeInvokeResult[TOut](cfg.serdes, existing, stepID)
		case types.OperationStatusFailed, types.OperationStatusTimedOut, types.OperationStatusStopped, types.OperationStatusCancelled:
			return zero, invokeError(existing, id)
		case types.OperationStatusStarted, types.OperationStatusPending:
			// Already checkpointed, still awaiting the target function's
			// result - block for it, exactly like Wait/CreateCallback
			// blocking on an in-flight operation.
			result, ok := c.ExecManager().WaitForOperation(stepID)
			if !ok {
				return zero, errSuspended
			}
			resumeInfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeChainedInvoke}
			if result.Status != types.OperationStatusSucceeded {
				invokeErr := invokeError(result, id)
				dispatchOperationEnd(c, resumeInfo, 0, plugin.OperationStatusFailed, "", invokeErr)
				return zero, invokeErr
			}
			typed, err := deserializeInvokeResult[TOut](cfg.serdes, result, stepID)
			if err != nil {
				dispatchOperationEnd(c, resumeInfo, 0, plugin.OperationStatusFailed, "", err)
				return zero, err
			}
			serialized := ""
			if result.ChainedInvokeDetails != nil && result.ChainedInvokeDetails.Result != nil {
				serialized = *result.ChainedInvokeDetails.Result
			}
			dispatchOperationEnd(c, resumeInfo, 0, plugin.OperationStatusSucceeded, serialized, nil)
			return typed, nil
		}
	}

	serializedInput, serErr := cfg.serdes.Serialize(input, stepID, c.ExecutionARN())
	if serErr != nil {
		return zero, newSerdesError("serialize", stepID, id, serErr)
	}

	// docs/remaining-work.md §6 task 16: reject an oversized invoke input
	// before checkpointing it - see checkResultSize's doc in errors.go.
	if err := checkResultSize(serializedInput, OperationKindInvoke, stepID, id); err != nil {
		return zero, err
	}

	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeChainedInvoke}
	dispatchOperationStart(c, dinfo, 0)

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:                   stepID,
		ParentID:             c.ParentStepID(),
		Type:                 types.OperationTypeChainedInvoke,
		Name:                 id,
		Action:               types.OperationActionStart,
		Payload:              &serializedInput,
		ChainedInvokeOptions: &types.ChainedInvokeOptions{FunctionName: functionARN, TenantID: cfg.tenantID},
	}); err != nil {
		return zero, fmt.Errorf("invoke %q (id %s): checkpointing start: %w", id, stepID, err)
	}

	result, ok := c.ExecManager().WaitForOperation(stepID)
	if !ok {
		return zero, errSuspended
	}
	if result.Status != types.OperationStatusSucceeded {
		invokeErr := invokeError(result, id)
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", invokeErr)
		return zero, invokeErr
	}
	typed, err := deserializeInvokeResult[TOut](cfg.serdes, result, stepID)
	if err != nil {
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
		return zero, err
	}
	serialized := ""
	if result.ChainedInvokeDetails != nil && result.ChainedInvokeDetails.Result != nil {
		serialized = *result.ChainedInvokeDetails.Result
	}
	dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, serialized, nil)
	return typed, nil
}

func deserializeInvokeResult[TOut any](serdes types.Serdes, op types.Operation, stepID string) (TOut, error) {
	var zero TOut
	if op.ChainedInvokeDetails == nil || op.ChainedInvokeDetails.Result == nil {
		return zero, fmt.Errorf("invoke %s: succeeded but no result recorded", stepID)
	}
	val, err := serdes.Deserialize(*op.ChainedInvokeDetails.Result, stepID, "")
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, err)
	}
	typed, ok := val.(TOut)
	if ok {
		return typed, nil
	}
	// Fall back to a JSON re-marshal/unmarshal round trip, matching
	// Step's deserializeStepResult - see that function's doc for why.
	b, err := json.Marshal(val)
	if err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", stepID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	return typed, nil
}

func invokeError(op types.Operation, name string) error {
	timeout := op.Status == types.OperationStatusTimedOut
	if op.ChainedInvokeDetails != nil && op.ChainedInvokeDetails.Error != nil {
		return &InvokeFailedError{
			OperationError: &OperationError{Kind: OperationKindInvoke, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.ChainedInvokeDetails.Error.ErrorMessage)},
			Status:         op.Status,
			Timeout:        timeout,
		}
	}
	return &InvokeFailedError{
		OperationError: &OperationError{Kind: OperationKindInvoke, ID: op.ID, Name: name, Err: fmt.Errorf("failed with status %s and no recorded error", op.Status)},
		Status:         op.Status,
		Timeout:        timeout,
	}
}

// ChildOption configures a RunInChildContext call.
type ChildOption[T any] func(*childConfig[T])

type childConfig[T any] struct {
	serdes types.Serdes
	// subType is the wire SubType checkpointed for this child context's
	// CONTEXT operation. Deliberately unexported - not part of the
	// public ChildOption API - since it exists purely so WaitForCallback
	// (callback.go, same package) can make its own wrapping child
	// context report SubType WaitForCallback instead of the default
	// RunInChildContext, matching the language-neutral conformance
	// suite's own expectation (test-requirements/wait_for_callback/*.yaml
	// universally expects ContextStarted/ContextSucceeded with SubType
	// WaitForCallback for this exact wrapper) - not something a regular
	// RunInChildContext caller should ever need or be able to override,
	// since RunInChildContext itself has exactly one real SubType.
	subType string
}

// WithChildSerdes sets a custom serializer for the child context's return
// value.
func WithChildSerdes[T any](s types.Serdes) ChildOption[T] {
	return func(c *childConfig[T]) { c.serdes = s }
}

// withChildSubType overrides the default subTypeRunInChildContext value -
// unexported, for this package's own internal use only (see
// childConfig.subType's doc for why).
func withChildSubType[T any](subType string) ChildOption[T] {
	return func(c *childConfig[T]) { c.subType = subType }
}

// RunInChildContext runs fn in an isolated child DurableContext with its
// own hierarchical step-ID namespace and checkpoint log, nested under id.
// Use this to group related operations (e.g. the branches of a Map or a
// logical sub-workflow) so their step IDs don't collide with sibling
// operations and so they can be reasoned about/replayed independently.
//
// # Implementation notes
//
// Mirrors the real backend's confirmed CONTEXT/RUN_IN_CHILD_CONTEXT
// operation lifecycle (see docs/checkpoint-replay-design.md and the
// internal SDK Operation Diagrams design doc, which document this
// flowchart identically for all three reference SDKs):
//
//  1. Claim this context's next step ID and look it up in the operation
//     log, exactly like Step's replay-skip check.
//  2. If found and terminal (Succeeded/Failed), return the checkpointed
//     result/error without running fn - the replay-skip path. This means
//     fn itself is NEVER re-invoked once its child context has completed,
//     even though fn typically issues its OWN nested checkpoints (steps,
//     waits, further child contexts) that are individually replay-safe;
//     the child-context-level checkpoint is a second, coarser barrier on
//     top of those.
//  3. Otherwise, checkpoint CONTEXT/START (mirroring
//     CheckpointDurableExecution CONTEXT/RUN_IN_CHILD_CONTEXT START in the
//     confirmed flowchart), create a genuine child Context (its own
//     step-ID namespace prefixed by this operation's ID, sharing the
//     parent's execManager/checkpoint Manager), and run fn with it.
//  4. Checkpoint CONTEXT/SUCCEED with the serialized result, or
//     CONTEXT/FAIL with the error, matching the flowchart's two terminal
//     branches.
//
// # Goroutine-registration ordering
//
// fn runs synchronously on the calling goroutine, not a new one - this
// mirrors the reference SDKs' RunInChildContext (unlike Map/Parallel,
// which explicitly fan out into concurrent branches, RunInChildContext by
// itself is a sequential, isolating wrapper). Because of this, no
// execmgr.Register/Deregister call is needed here: the calling goroutine
// was already registered active before entering RunInChildContext, and
// remains the same goroutine actually executing fn, so the active count
// is correctly unaffected. This matters for Map/Parallel's future
// implementation (which WILL spawn new goroutines per branch, each
// wrapped in a RunInChildContext call): the register-before-spawn
// ordering described in docs/remaining-work.md's task #1 note applies to
// THOSE call sites' own goroutine spawning, not to RunInChildContext
// itself, which has none.
func RunInChildContext[T any](dc types.DurableContext, id string, fn func(child types.DurableContext) (T, error), opts ...ChildOption[T]) (T, error) {
	cfg := &childConfig[T]{serdes: utils.DefaultSerdes(), subType: subTypeRunInChildContext}
	for _, opt := range opts {
		opt(cfg)
	}

	var zero T
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return zero, fmt.Errorf("operations.RunInChildContext: dc must be created by this SDK's runtime (got %T)", dc)
	}

	contextID := c.NextStepID()

	if existing, found := c.ExecManager().GetOperation(contextID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CONTEXT operation with SubType RUN_IN_CHILD_CONTEXT before
		// reading ContextDetails off of it below - Map/Parallel's own
		// batch items also checkpoint CONTEXT operations, but with
		// SubType MAP_ITERATION/PARALLEL_BRANCH (see batch.go's
		// runBatchItem), and Map/Parallel themselves checkpoint CONTEXT
		// with SubType MAP/PARALLEL - all four are otherwise
		// indistinguishable from a bare CONTEXT type check alone, so the
		// SubType comparison is what actually catches e.g. "this used to
		// be a Map call, now it's a RunInChildContext call at the same
		// step ID."
		if err := checkReplayConsistency(existing, types.OperationTypeContext, cfg.subType, OperationKindChildContext, contextID, id); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			if existing.ContextDetails != nil && existing.ContextDetails.ReplayChildren {
				// ReplayChildren protocol (conformance requirement 3-11;
				// see the write-side's own doc, a few dozen lines below
				// this function, for the full writeup): this context's
				// own real result was too large to durably checkpoint,
				// so the terminal SUCCEED checkpoint on record carries
				// an intentionally EMPTY Payload plus this flag - NOT a
				// genuinely empty/zero result. deserializeContextResult
				// would incorrectly decode that empty Payload as this
				// type's own zero value. Instead, re-run fn one more
				// time to reconstruct the real result purely in memory,
				// exactly matching the JS reference SDK's own confirmed,
				// working ReplaySucceededContext behavior
				// (run-in-child-context-handler.ts's
				// handleCompletedChildContext) - see this function's own
				// call below for why this does NOT re-checkpoint
				// anything (the context is already terminally
				// SUCCEEDED; only fn's own body needs to run again, not
				// this context's own START/SUCCEED lifecycle).
				return replayChildrenResult(c, contextID, id, cfg.serdes, fn)
			}
			return deserializeContextResult[T](cfg.serdes, existing, contextID)
		case types.OperationStatusFailed:
			return zero, contextError(existing, id, OperationKindChildContext)
		}
		// STARTED/PENDING: an interrupted attempt from a prior invocation
		// that crashed after the START checkpoint but before completion.
		// Unlike Step, RunInChildContext has no retry-strategy concept -
		// re-running fn from scratch is always safe here because fn's own
		// nested operations are independently replay-safe (that's the
		// entire point of the child context's isolated step-ID
		// namespace): whatever fn already checkpointed under this
		// context's prefix will itself replay-skip correctly when fn
		// re-executes and re-issues those same calls in the same order.
	} else {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       contextID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     id,
			Action:   types.OperationActionStart,
			SubType:  cfg.subType,
		}); err != nil {
			return zero, fmt.Errorf("child context %q (id %s): checkpointing start: %w", id, contextID, err)
		}
		dinfo := operationKindDispatchInfo{ID: contextID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeContext, SubType: cfg.subType}
		dispatchOperationStart(c, dinfo, 0)
	}

	child := c.NewChildWithName(contextID, id)
	dinfo := operationKindDispatchInfo{ID: contextID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeContext, SubType: cfg.subType}
	wrapped, err := wrapChildContextFn(c, dinfo, func() (any, error) {
		return fn(child)
	})
	var result T
	if wrapped != nil {
		result, _ = wrapped.(T)
	}
	if err != nil {
		if errors.Is(err, errSuspended) {
			// fn itself suspended (e.g. a Step inside it hit a
			// non-zero retry delay - see step.go's retryOrFail) rather
			// than genuinely failing. Propagate the sentinel as-is:
			// checkpointing a FAIL here would be wrong (this context
			// hasn't actually failed, it's just paused) and would
			// corrupt replay by permanently marking it Failed for what
			// should resume as Pending on a later invocation.
			return zero, errSuspended
		}
		opErr := checkpointErrorObject(err)
		if ckErr := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       contextID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     id,
			Action:   types.OperationActionFail,
			SubType:  cfg.subType,
			Error:    opErr,
		}); ckErr != nil {
			return zero, fmt.Errorf("child context %q (id %s): checkpointing failure: %w", id, contextID, ckErr)
		}
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
		return zero, &ChildContextFailedError{
			OperationError: &OperationError{Kind: OperationKindChildContext, ID: contextID, Name: id, Err: err},
			Reconstructed:  false,
			Original:       err,
		}
	}

	serialized, serErr := cfg.serdes.Serialize(result, contextID, c.ExecutionARN())
	if serErr != nil {
		return zero, newSerdesError("serialize", contextID, id, serErr)
	}

	// docs/remaining-work.md §6 task 16 (superseded for RunInChildContext
	// specifically by the ReplayChildren protocol below - see that
	// section's own doc for the full history): an oversized
	// child-context result is no longer unconditionally rejected with a
	// client-side *ResultTooLargeError. Confirmed via the conformance
	// suite's own requirement 3-11 ("Child context large payload
	// (ReplayChildren mode)") and the JS reference SDK's own actual,
	// working implementation (run-in-child-context-handler.ts's
	// executeChildContext/handleCompletedChildContext) that the real,
	// intended protocol for RunInChildContext specifically (Step/Wait/
	// Callback/Invoke/Map/Parallel are unaffected - checkResultSize
	// still unconditionally rejects an oversized result for all of
	// those, since none of them have a ReplayChildren wire field to use
	// instead) is:
	//
	//  1. When the real, in-memory result serializes larger than the
	//     single-operation checkpoint threshold, checkpoint
	//     ContextSucceeded with an EMPTY Payload and
	//     ContextOptions.ReplayChildren: true, instead of a
	//     *ResultTooLargeError.
	//  2. Return the REAL, already-computed in-memory result to the
	//     caller on THIS invocation - it was never actually too large to
	//     use, only too large to durably checkpoint; this invocation
	//     already has it in memory and needs nothing further from the
	//     backend to hand it back right now.
	//  3. On any LATER invocation that replays past this now-terminally-
	//     SUCCEEDED context (see the OperationStatusSucceeded branch in
	//     runInChildContext's replay-skip check above), detect
	//     ContextDetails.ReplayChildren and, instead of the ordinary
	//     replay-skip (deserializing an empty Payload, which would
	//     produce a wrong/empty zero value), RE-EXECUTE fn one more time
	//     to reconstruct the real result purely in memory - see
	//     replayChildrenResult's own doc below for that side.
	if len(serialized) <= resultTooLargeThresholdBytes {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       contextID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     id,
			Action:   types.OperationActionSucceed,
			SubType:  cfg.subType,
			Payload:  &serialized,
		}); err != nil {
			return zero, fmt.Errorf("child context %q (id %s): checkpointing success: %w", id, contextID, err)
		}
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, serialized, nil)
		// Return the serdes ROUND-TRIP, not the raw in-memory value -
		// see roundTripSerialized's doc (matches the JS reference SDK's
		// run-in-child-context-handler.ts, whose own doc comment says
		// the value handed back "has always passed through the serdes
		// round-trip, regardless of payload size"; conformance 3-14).
		return roundTripSerialized[T](cfg.serdes, serialized, contextID, id, c.ExecutionARN())
	}

	empty := ""
	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       contextID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeContext,
		Name:     id,
		Action:   types.OperationActionSucceed,
		SubType:  cfg.subType,
		Payload:  &empty,
		ContextOptions: &types.ContextOptions{
			ReplayChildren: true,
		},
	}); err != nil {
		return zero, fmt.Errorf("child context %q (id %s): checkpointing ReplayChildren success: %w", id, contextID, err)
	}

	dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, "", nil)
	// Same serdes round-trip as the small-payload branch above - the JS
	// reference SDK's own doc comment is explicit that this applies
	// "regardless of payload size" (large: re-execute then round-trip),
	// so the ReplayChildren branch round-trips too, using the ALREADY
	// serialized bytes this branch computed for its size check.
	return roundTripSerialized[T](cfg.serdes, serialized, contextID, id, c.ExecutionARN())
}

// replayChildrenResult re-executes fn to reconstruct a child context's real
// result purely in memory, for a context whose terminal SUCCEED checkpoint
// is already on record with ContextDetails.ReplayChildren=true (see
// RunInChildContext's own OperationStatusSucceeded case, immediately
// above, for why this is needed instead of the ordinary replay-skip
// deserialize path).
//
// Deliberately does NOT enqueue any checkpoint of its own: the context
// itself is already terminally SUCCEEDED on record - re-running fn here
// exists purely to reconstruct T in this invocation's own memory, not to
// re-decide or re-record the context's own outcome. fn's OWN nested
// operations (its Steps/Waits/further child contexts) still checkpoint
// and replay-skip completely normally through the same child Context
// this creates - they are unaffected by ReplayChildren, which is a
// property of THIS context's own outer checkpoint only (confirmed
// against conformance requirement 3-11's own ExpectedExecutionHistory:
// the inner Step's StepStarted/StepSucceeded events appear exactly once,
// never a second time on replay, because that Step's own checkpoint is
// independently already SUCCEEDED and replay-skips normally when fn
// re-executes and re-issues that same call).
//
// fn running a second time is only safe/correct because ReplayChildren
// mode's entire premise (confirmed against the JS reference SDK's own
// implementation) is that fn is expected to be side-effect-free/
// idempotent with respect to reconstructing its own return value - the
// SAME assumption RunInChildContext's own top-level doc already states
// for its ordinary crash-resume case ("re-running fn from scratch is
// always safe here because fn's own nested operations are independently
// replay-safe").
func replayChildrenResult[T any](c *dcontext.Context, contextID, id string, serdes types.Serdes, fn func(child types.DurableContext) (T, error)) (T, error) {
	var zero T
	child := c.NewChildWithName(contextID, id)
	result, err := fn(child)
	if err != nil {
		if errors.Is(err, errSuspended) {
			return zero, errSuspended
		}
		// fn failing on replay, when its own context is already
		// checkpointed SUCCEEDED, would mean fn is not actually
		// deterministic/idempotent the way ReplayChildren mode requires.
		// Reconstructed stays false (not true) here even though this is
		// a replay - per ChildContextFailedError.Reconstructed's own
		// doc, that field specifically distinguishes "rebuilt from a
		// checkpointed Error string, Original is nil" from "fn's actual
		// returned error within the same invocation that ran it,
		// Original is non-nil" - and this branch is the latter: err is
		// fn's REAL, live Go error value from THIS invocation's own
		// live re-execution, not a persisted error string being
		// reconstructed from the operation log (there is no persisted
		// Error at all here - this context checkpointed SUCCEEDED, not
		// FAILED). The distinct, unusual circumstance (a context already
		// on record as SUCCEEDED nonetheless failing when fn reruns) is
		// captured in the wrapped error message text instead.
		return zero, &ChildContextFailedError{
			OperationError: &OperationError{Kind: OperationKindChildContext, ID: contextID, Name: id, Err: fmt.Errorf("ReplayChildren re-execution failed (context already checkpointed SUCCEEDED): %w", err)},
			Reconstructed:  false,
			Original:       err,
		}
	}
	// Round-trip the re-executed result through the serdes before
	// returning, exactly like the fresh-execution path - the JS
	// reference SDK's run-in-child-context-handler.ts doc comment spells
	// this out per-path: "large: re-execute then ROUND-TRIP" (emphasis
	// ours). Serialize can fail here even though the original execution
	// serialized fine (fn is only REQUIRED to be deterministic - a
	// non-deterministic fn violating that is exactly the case the error
	// message above also covers), so treat a failure like any other
	// serdes failure.
	serialized, serErr := serdes.Serialize(result, contextID, c.ExecutionARN())
	if serErr != nil {
		return zero, newSerdesError("serialize", contextID, id, serErr)
	}
	return roundTripSerialized[T](serdes, serialized, contextID, id, c.ExecutionARN())
}

func deserializeContextResult[T any](serdes types.Serdes, op types.Operation, contextID string) (T, error) {
	var zero T
	if op.ContextDetails == nil || op.ContextDetails.Result == nil {
		return zero, fmt.Errorf("child context %s: succeeded but no result recorded", contextID)
	}
	val, err := serdes.Deserialize(*op.ContextDetails.Result, contextID, "")
	if err != nil {
		return zero, newSerdesError("deserialize", contextID, op.Name, err)
	}
	typed, ok := val.(T)
	if ok {
		return typed, nil
	}
	// Fall back to a JSON re-marshal/unmarshal round trip, matching
	// Step's deserializeStepResult - see that function's doc for why.
	b, err := json.Marshal(val)
	if err != nil {
		return zero, newSerdesError("deserialize", contextID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", contextID, op.Name, fmt.Errorf("converting checkpointed result to %T: %w", zero, err))
	}
	return typed, nil
}

// contextError reconstructs an error for a checkpointed CONTEXT operation
// that ended Failed, on replay (i.e. from data read back off
// types.Operation, not from an in-hand Go error value - see
// ChildContextFailedError's doc for why this is necessarily a
// "Reconstructed: true," "Original: nil" error: the checkpointed
// ErrorObject is only ever a message + type-name string, never a
// serialized Go error value, so the concrete original error is gone by
// the time this runs). kind distinguishes a bare RunInChildContext
// failure (OperationKindChildContext) from a Map/Parallel batch item's
// otherwise-identical CONTEXT/FAIL shape (OperationKindBatchItem, via
// batch.go's own wrapping - see that file's contextError call sites).
func contextError(op types.Operation, name string, kind OperationKind) error {
	if op.ContextDetails != nil && op.ContextDetails.Error != nil {
		return &ChildContextFailedError{
			OperationError: &OperationError{Kind: kind, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.ContextDetails.Error.ErrorMessage)},
			Reconstructed:  true,
		}
	}
	return &ChildContextFailedError{
		OperationError: &OperationError{Kind: kind, ID: op.ID, Name: name, Err: fmt.Errorf("failed with no recorded error")},
		Reconstructed:  true,
	}
}

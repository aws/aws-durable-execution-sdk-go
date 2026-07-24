package dcontext

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// This file implements the name-based task-ID seam consumed by the
// experimental DAG feature (pkg/durable/dag). It is additive: it layers a
// deterministic, NAME-derived operation-ID scheme on top of the existing
// positional NextStepID counter without editing that counter or any other
// core ID machinery. See docs/DAG_SPEC_GO.md §4 (Route A) and §7.

// DagNodeDelimiter is the reserved token embedded in a DAG task's derived
// entity ID: a task named "fetch" nested under prefix P mints the raw
// entity ID "P-DAG_NODE_T_fetch" (or "DAG_NODE_T_fetch" when P is empty),
// which is then hashed via HashOperationID to form the task's real
// operation ID. Because DAG task names are validated to exclude both "-"
// and the substring "DAG_NODE_T_" (see dag.validate), the mapping from
// (parentPrefix, name) to entity ID is injective, guaranteeing distinct,
// replay-stable IDs regardless of task registration/completion order.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
const DagNodeDelimiter = "DAG_NODE_T_"

// TaskEntityID returns the deterministic raw entity ID for a DAG task with
// the given name nested under parentPrefix. With an empty parentPrefix the
// result is "DAG_NODE_T_{name}"; otherwise it is
// "{parentPrefix}-DAG_NODE_T_{name}". The value is a pure function of its
// inputs, so it is identical across replays and independent of the order
// in which sibling tasks are registered or complete. Nested DAGs compose
// by feeding one task's entity ID (hashed) back in as the parentPrefix of
// its children, yielding IDs like "...-DAG_NODE_T_a-DAG_NODE_T_b".
//
// The returned string is the hash INPUT, never stored raw on the wire: the
// caller passes it to HashOperationID to obtain the SHA-256 operation ID
// actually used for checkpointing and replay-skip lookups.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func TaskEntityID(parentPrefix, name string) string {
	if parentPrefix == "" {
		return DagNodeDelimiter + name
	}
	return parentPrefix + "-" + DagNodeDelimiter + name
}

// HashOperationID exposes this SDK's canonical operation-ID hashing
// (SHA-256, matching the Java reference SDK) for the DAG package, which
// mints operation IDs from names rather than the positional counter and so
// must hash them the same way NextStepID does internally. It is a pure
// function: the same rawID always hashes to the same 64-char hex string.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func HashOperationID(rawID string) string { return hashOperationID(rawID) }

// Prefix returns this context's step-ID namespace prefix - the hash input
// that this context's own NextStepID/PeekStepID calls build on. The DAG
// scheduler uses it as the parentPrefix argument to TaskEntityID so that a
// DAG's task IDs are derived relative to the DAG's own child-context
// prefix (giving nested-DAG ID recursion for free).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (c *Context) Prefix() string { return c.prefix }

// NewNamedChild returns a child Context whose step-ID namespace is rooted
// at the already-final (hashed) entityID rather than at a positional ID
// claimed from this context's counter. It is the name-based counterpart of
// NewChildWithName: callers derive entityID via
// HashOperationID(TaskEntityID(c.Prefix(), name)) and pass it here so the
// child - and every operation nested within it - mints deterministic,
// name-derived IDs that are stable across replays and independent of
// sibling scheduling order (DAG_SPEC_GO.md §4, Route A).
//
// The child shares this context's ExecManager and Checkpoint managers, so
// operations issued inside it reuse the existing per-operation checkpoint
// fast-path (ExecManager().GetOperation + replay-consistency) unchanged; no
// core edit to the ID counter is involved.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (c *Context) NewNamedChild(entityID, name string) *Context {
	return c.NewChildWithName(entityID, name)
}


// DagContextSubType is the CONTEXT-operation SubType checkpointed for the
// scope/task container contexts the DAG feature materializes via
// NewMaterializedChild/FinishMaterializedChild. These containers ARE
// genuine child contexts (they group a task's - or a whole DAG's - nested
// operations under their own hashed operation ID), so they reuse the same
// wire SubType RunInChildContext already checkpoints, a value the real
// backend is known to accept; the DAG never distinguishes its containers
// from a plain RunInChildContext at the SubType level, so no dedicated
// value is warranted.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
const DagContextSubType = "RunInChildContext"

// NewMaterializedChild is NewNamedChild plus a durably-checkpointed
// CONTEXT/START operation for the returned child. It exists because a bare
// NewNamedChild only builds an IN-MEMORY child context: the child's own
// operation ID (entityID) is never checkpointed, so any operation nested
// inside it is sent to the backend with a ParentID pointing at a context
// that was never materialized, which the real CheckpointDurableExecution
// API rejects with "Invalid parent operation id" (an in-memory test client
// silently accepts the orphan, which is why the gap only surfaced against
// the real backend). Materializing a CONTEXT/START here - exactly as
// operations.RunInChildContext and runBatchItem do for their own child
// contexts - gives every nested task operation a valid, already-recorded
// parent.
//
// The START is idempotent across replays: if an operation with entityID is
// already on record from a prior invocation it is NOT re-checkpointed, and
// the returned bool reports whether that prior operation was already
// terminal (SUCCEEDED/FAILED) so the caller can likewise skip the matching
// FinishMaterializedChild. entityID is a name-derived, replay-stable hash
// (HashOperationID(TaskEntityID(...))), so the materialized CONTEXT op's ID
// is identical across replays and independent of sibling scheduling order,
// preserving the DAG's order-independence guarantee.
//
// ParentID is c.ParentStepID() - c being the ENCLOSING context - so the
// materialized CONTEXT op nests under c's own (already-materialized)
// parent. Nested DAGs therefore recurse for free: a SubDag task's own
// container is materialized under its parent DAG's scope, and its inner
// scope under it, each level supplying a valid parent for the next.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (c *Context) NewMaterializedChild(entityID, name, subType string) (*Context, bool, error) {
	child := c.NewChildWithName(entityID, name)

	if existing, found := c.execManager.GetOperation(entityID); found {
		switch existing.Status {
		case types.OperationStatusSucceeded, types.OperationStatusFailed:
			// Already terminal on a prior invocation: skip re-checkpointing
			// START, and tell the caller to skip the terminal transition too.
			return child, true, nil
		default:
			// STARTED/PENDING: an interrupted attempt from a prior
			// invocation. Do NOT re-checkpoint START; the caller re-runs
			// the body (its nested operations are independently
			// replay-safe under this child's own step-ID prefix) and
			// finishes the container normally.
			return child, false, nil
		}
	}

	if err := c.checkpoint.Enqueue(types.OperationUpdate{
		ID:       entityID,
		ParentID: c.parentID,
		Type:     types.OperationTypeContext,
		Name:     name,
		Action:   types.OperationActionStart,
		SubType:  subType,
	}); err != nil {
		return nil, false, fmt.Errorf("dag: checkpointing context start %q (id %s): %w", name, entityID, err)
	}
	return child, false, nil
}

// FinishMaterializedChild checkpoints the terminal CONTEXT transition for a
// child previously created by NewMaterializedChild: CONTEXT/FAIL (carrying
// opErr) when opErr != nil, otherwise CONTEXT/SUCCEED with an EMPTY payload
// and ContextOptions.ReplayChildren=true. The empty-payload+ReplayChildren
// shape is the confirmed-valid "this context's real result lives in its
// nested child operations, not in this SUCCEED payload" form (identical to
// operations.RunInChildContext's own ReplayChildren protocol): the DAG
// never deserializes a container's payload (it always re-runs each task
// body on replay so the task's own typed inner operation fast-paths with a
// correctly-typed result), so storing no payload here avoids any
// any-typed round-trip fidelity loss while still recording a valid
// terminal state for the container.
//
// Callers MUST skip this when NewMaterializedChild reported alreadyTerminal
// (the container is already terminal on record) or when the body suspended
// (operations.ErrSuspended): re-recording a terminal transition, or
// recording one for a merely-paused context, would corrupt replay - the
// same reasoning runBatchItem/RunInChildContext apply to their own suspend
// handling.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (c *Context) FinishMaterializedChild(entityID, name, subType string, opErr error) error {
	upd := types.OperationUpdate{
		ID:       entityID,
		ParentID: c.parentID,
		Type:     types.OperationTypeContext,
		Name:     name,
		SubType:  subType,
	}
	if opErr != nil {
		upd.Action = types.OperationActionFail
		upd.Error = &types.ErrorObject{
			ErrorMessage: opErr.Error(),
			ErrorType:    fmt.Sprintf("%T", opErr),
		}
	} else {
		empty := ""
		upd.Action = types.OperationActionSucceed
		upd.Payload = &empty
		upd.ContextOptions = &types.ContextOptions{ReplayChildren: true}
	}
	if err := c.checkpoint.Enqueue(upd); err != nil {
		return fmt.Errorf("dag: checkpointing context finish %q (id %s): %w", name, entityID, err)
	}
	return nil
}
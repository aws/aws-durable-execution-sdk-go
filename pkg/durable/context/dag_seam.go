package dcontext

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

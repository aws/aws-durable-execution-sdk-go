// Package dcontext provides the concrete implementation of
// types.DurableContext and types.StepContext, wiring together step-ID
// generation, replay-skip lookups, and suspension coordination via
// execmgr.Manager.
//
// This is intentionally a separate internal package from the public
// operations package: operations.Step et al. are generic, user-facing
// entry points; dcontext holds the (non-generic) runtime state and logic
// shared by all of them, mirroring the JS SDK's split between
// durable-context.ts (user-facing dispatch) and execution-context.ts
// (shared runtime state).
package dcontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/execmgr"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// Context is the concrete, mutable implementation of types.DurableContext.
// A root Context is created once per invocation; child contexts (from
// RunInChildContext, Map, Parallel) each get their own Context sharing the
// same execManager and checkpoint Manager but with an independent step
// counter scoped under their own ID prefix (see docs/checkpoint-replay-design.md §1).
type Context struct {
	execCtx  context.Context
	execARN  string
	logger   types.Logger
	loggerMu sync.Mutex

	// loggerCfg is the LoggerConfig in effect for this Context's own
	// Logger() calls (docs/remaining-work.md §5 tasks 13/14) - guarded by
	// loggerMu alongside logger, since ConfigureLogger updates both
	// together and Logger() reads them together to build the returned
	// utils.ContextLogger.
	loggerCfg types.LoggerConfig

	// baseFields are this Context's own contextual log fields (executionArn
	// always; contextId/contextName/parentId additionally for a child
	// context - see NewChild). Merged into every Logger() call's output by
	// the utils.ContextLogger built in Logger(). Immutable after
	// construction (unlike logger/loggerCfg, which ConfigureLogger may
	// change later), so no mutex needed for this field specifically.
	baseFields map[string]any

	execManager *execmgr.Manager
	checkpoint  *checkpoint.Manager

	// prefix is this context's step-ID namespace, used as the hash-input
	// prefix for every ID minted within it (see NextStepID/hashOperationID
	// below) - mirroring the Java reference SDK's OperationIdGenerator
	// exactly (docs/remaining-work.md's "SHA-256 operation ID hashing"
	// writeup has the full cross-SDK justification for matching Java
	// specifically here). For the ROOT context this is the EXECUTION
	// operation's own ID (a backend-assigned value, NOT minted by this
	// counter - see NewRoot's rootOperationID parameter and
	// durable.go's WithDurableExecution, which passes executionOp.ID). For
	// a CHILD context (RunInChildContext, a Map iteration, a Parallel
	// branch) it is the enclosing operation's own step ID - which, since
	// NextStepID now returns a SHA-256 hash rather than a plain
	// hierarchical string, is itself ALREADY a hash by the time it
	// becomes a child Context's prefix (see NewChild's parentStepID
	// parameter/doc). This is exactly what makes the hashing compose
	// correctly across nesting levels without this field needing any
	// special-casing: prefix is simply "whatever hash input this
	// context's own operations should be minted under," regardless of
	// whether that value happens to be a raw backend-assigned ID (root)
	// or a previously-computed hash (child) - NextStepID/PeekStepID
	// don't need to know or care which.
	prefix string

	// parentID is the value ParentStepID() returns - the real
	// types.OperationUpdate.ParentID to checkpoint for every operation
	// issued directly against this Context. This is DELIBERATELY a
	// separate field from prefix, even though prior to this task's
	// SHA-256-hashing change the two were always identical values (see
	// git history): prefix is now "the hash-INPUT this context's
	// NextStepID/PeekStepID calls build on" (the EXECUTION operation's
	// own raw, unhashed ID for the root context - see NewRoot), whereas
	// parentID is "the ParentID to report on the wire" (empty/omitted
	// for the root context, matching the official API reference's
	// "Required: No" on Operation/OperationUpdate.ParentId - a root-level
	// operation genuinely has no parent operation, even though its ID IS
	// still derived from a hash that happens to incorporate the
	// EXECUTION operation's ID as an input). Conflating these two after
	// introducing hashing would have been a real bug: it would have
	// wrongly reported the EXECUTION operation's ID as the ParentID of
	// every root-level Step/Wait/Context operation, when the confirmed
	// Java reference SDK design (and the pre-existing, already-shipped
	// ParentID fix this task builds on - see docs/remaining-work.md §0)
	// both treat root-level operations as having NO parent at all. For a
	// CHILD context, parentID equals prefix (both are the enclosing
	// operation's own already-hashed step ID) - see NewChild, which sets
	// both fields to the same parentStepID argument.
	parentID string

	// counter is the next step ID to mint within this context, guarded by
	// mu since concurrent branches (Map/Parallel) may each hold a
	// reference to a shared parent's counter... actually each branch gets
	// its OWN child Context (see RunInChildContext), so this counter is
	// only ever touched by the single logical sequence of calls within
	// this context. It is still mutex-guarded because a context's
	// operations may be issued in overlapping goroutines that share this
	// Context value directly (e.g. concurrent Step calls the caller chose
	// not to isolate into child contexts) - matching the JS/Java SDKs'
	// requirement that concurrent durable operations use child contexts
	// for determinism, while still not corrupting the counter if that
	// rule is violated.
	mu      sync.Mutex
	counter int

	replaying bool

	// replayState is shared (by pointer) across the root Context and
	// every child/descendant Context created from it via NewChild - see
	// that type's doc for why replay-skip suppression (task 13) needs a
	// single mutable flag shared across the whole invocation's Context
	// tree, not a value copied per-Context like replaying/prefix are.
	replayState *replayState

	// plugins is this invocation's EXPERIMENTAL instrumentation plugins
	// (see pkg/durable/plugin's own package doc for scope/status),
	// propagated unchanged from the root Context to every
	// descendant via NewChild/newChildWithName - the same slice value is
	// shared (not copied per-Context) since plugins are configured once,
	// for the whole invocation, via Config.Plugins.
	plugins []plugin.InstrumentationPlugin
}

// replayState tracks, for a single durable-function invocation, whether
// execution is still within the "replaying already-checkpointed
// operations without doing real work" portion of the handler's re-run, or
// has moved past it into real (first-time or genuinely-retried) execution
// - the distinction docs/remaining-work.md §5 task 13 calls out explicitly
// as "is the code path currently replay-skipping," not "is this invocation
// a replay of a previous one" globally.
//
// # Why this needs to be mutable, shared, one-way state
//
// Per the confirmed official SDK reference doc ("Replay log suppression"):
// "When the SDK replays, it runs your handler from the start until it
// reaches the next incomplete operation. It does not re-emit log entries
// encountered before that point... Logs inside a retrying step body always
// emit, because the step has not completed yet."
//
// That means suppression is not a static, per-invocation property (a
// simple "IsReplaying() bool" read once) - it is a FRONTIER that the
// invocation's single logical execution path crosses exactly once, forever,
// the moment it reaches the first operation that does real work instead of
// replay-skipping. Every DurableContext.Logger()/StepContext.Logger() call
// made before that frontier (i.e. while every operation encountered so far
// has replay-skipped) is suppressed when ModeAware is on; every call made
// at or after it is not, even though c.replaying (this invocation's static
// "did it start with a non-empty operation log" flag) never changes for the
// rest of the invocation.
//
// This needs to be:
//   - Mutable: markRealExecution() flips it exactly once, permanently.
//   - Shared across the whole Context tree: a RunInChildContext/Map/
//     Parallel branch's nested Steps must observe the SAME frontier the
//     enclosing root context's Logger() calls do - "reached the next
//     incomplete operation" is a property of the invocation's overall
//     replay position, not scoped to one child context's own step-ID
//     namespace. Hence the *replayState pointer is copied (not
//     recreated) into every Context returned by NewChild.
//   - NOT reset or newly allocated per-Context: only NewRoot allocates
//     one, seeded from that invocation's isReplaying flag.
type replayState struct {
	mu sync.Mutex
	// crossedFrontier is true once markRealExecution has been called at
	// least once during this invocation - i.e. once ANY operation has
	// actually executed real work rather than replay-skipped. Starts
	// false; only ever transitions false -> true, never back.
	crossedFrontier bool
	// initiallyReplaying is this invocation's static replay flag (whether
	// InitialExecutionState.Operations was non-empty at invocation start -
	// see durable.WithDurableExecution). If false (a genuinely fresh
	// execution, never replayed at all), nothing is ever suppressed:
	// there is no prior progress to skip past, so the frontier is
	// trivially already "crossed" from the very first operation.
	initiallyReplaying bool
}

func newReplayState(initiallyReplaying bool) *replayState {
	return &replayState{initiallyReplaying: initiallyReplaying}
}

// markRealExecution records that real (non-replay-skipped) execution has
// been reached. Called from the operations package at every point where an
// operation is about to actually run its body (fn/checkFn) rather than
// return a checkpointed result - see step.go's executeAndCheckpoint and
// wait_for_condition.go's pollAndCheckpoint, the two current call sites,
// both already the established "this is the real-execution path, not the
// replay-skip path" boundary per this package's own doc comments predating
// this task.
func (rs *replayState) markRealExecution() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.crossedFrontier = true
}

// isSuppressing reports whether a log call happening right now should be
// considered "during replay-skip" for ModeAware suppression purposes: this
// invocation started as a replay (initiallyReplaying) AND no real execution
// has been observed yet (the frontier hasn't been crossed).
func (rs *replayState) isSuppressing() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.initiallyReplaying && !rs.crossedFrontier
}

var _ types.DurableContext = (*Context)(nil)

// NewRoot creates the root Context for a durable execution invocation.
// rootOperationID is the backend-assigned ID of this invocation's own root
// EXECUTION operation (durable.go's WithDurableExecution passes
// executionOp.ID - a dynamically-assigned value confirmed from a live
// invocation to look UUID-like, NOT something this SDK's own counter
// mints - see that call site's comment). It becomes this Context's
// prefix, exactly matching the Java reference SDK's OperationIdGenerator
// ("For root contexts the prefix is the EXECUTION operation ID" - see
// NextStepID's doc for the full hashing scheme this feeds into): every ID
// minted directly off the root context is SHA-256(rootOperationID + "-" +
// counter), so two different executions (different rootOperationID
// values) always produce different hashed IDs for "the same" step number,
// even for a structurally-identical handler - see
// TestSHA256Hashing_DifferentContextIDsProduceDifferentHashes in
// dcontext_test.go for this exact property, which is the whole point of
// keying the hash off a per-EXECUTION value rather than a global
// constant.
//
// cfg's CustomLogger (if set) becomes this Context's base logger; cfg's
// ModeAware controls replay-skip log suppression (docs/remaining-work.md
// §5 task 13) for every Logger() call made through this Context or any of
// its descendants (see replayState's doc). Callers that only care about
// supplying a custom logger and are content with Go's zero-value-bool
// default for ModeAware can pass types.LoggerConfig{CustomLogger: x};
// durable.WithDurableExecution instead applies its own documented
// "default true when no LoggerConfig was configured at all" rule before
// calling this - see that function and types.LoggerConfig.ModeAware's doc
// for why the defaulting decision belongs there, one layer up, rather
// than here.
func NewRoot(execCtx context.Context, execARN string, execManager *execmgr.Manager, checkpointMgr *checkpoint.Manager, cfg types.LoggerConfig, isReplaying bool, rootOperationID string, plugins []plugin.InstrumentationPlugin) *Context {
	logger := cfg.CustomLogger
	if logger == nil {
		logger = utils.DefaultLogger{}
	}
	return &Context{
		execCtx:     execCtx,
		execARN:     execARN,
		logger:      logger,
		loggerCfg:   cfg,
		baseFields:  map[string]any{"executionArn": execARN},
		execManager: execManager,
		checkpoint:  checkpointMgr,
		prefix:      rootOperationID,
		// parentID intentionally left at its zero value ("") - the root
		// Context's own operations have no parent (see parentID field's
		// doc for why this must NOT be rootOperationID, unlike prefix).
		replaying:   isReplaying,
		replayState: newReplayState(isReplaying),
		plugins:     plugins,
	}
}

// NewChild creates a child Context nested under parentStepID, sharing the
// parent's execManager, checkpoint Manager, and replayState (see that
// type's doc for why replay-skip suppression must be shared across the
// whole Context tree, not copied per-Context) but with its own step-ID
// namespace and its own contextual log fields layered on top of the
// parent's.
//
// name and contextName are the child context's own step ID and the
// user-supplied name passed to RunInChildContext/the Map-item/
// Parallel-branch it represents (empty for a Map/Parallel branch that
// wasn't given an explicit name - see batch.go). Populates the
// contextId/contextName/parentId fields the confirmed cross-SDK
// "DurableContext (child)" execution metadata documents: contextId is
// this child's own operation ID, parentId is the ENCLOSING context's
// operation ID (empty string for a child directly under the root, which
// has no operation ID of its own to report), and operationId is set equal
// to contextId - the official reference doc lists both names for what is,
// for a child DurableContext (as opposed to a StepContext), the same
// value.
func (c *Context) NewChild(parentStepID string) *Context {
	return c.newChildWithName(parentStepID, "", parentStepID)
}

// NewChildWithName is like NewChild but additionally records contextName
// for the child's logger fields. Kept as a separate method (rather than
// changing NewChild's signature) so every existing caller in the
// operations package - most of which don't have a meaningful name to pass
// (Map/Parallel branches are only optionally named) - is unaffected; only
// RunInChildContext, which always has the caller's id in scope, needs to
// call this variant instead.
func (c *Context) NewChildWithName(parentStepID, contextName string) *Context {
	return c.newChildWithName(parentStepID, contextName, parentStepID)
}

// NewVirtualChildWithName is NewChildWithName's counterpart for FLAT
// nesting mode (see operations.WithMapNesting/WithParallelNesting's own
// doc for the full feature writeup - this is the low-level primitive
// those options build on). A "virtual" child still gets its own genuine,
// unique step-ID namespace (contextID, used as prefix below, so nested
// operations inside it don't collide with sibling items/branches'
// operations) but does NOT get its own CONTEXT-operation identity in the
// checkpoint hierarchy: its own ParentID (and thus every nested
// operation's ParentID) is reportedParentID - the ENCLOSING (grandparent)
// operation's own ParentStepID(), not contextID itself - exactly matching
// the JS reference SDK's own confirmed virtualContext mechanism
// (run-in-child-context-handler.ts's executeChildContext: `isVirtual ?
// parentId : entityId` as the value passed to createChildContext's own
// parentId parameter). This is what makes a virtual child's own nested
// Step/Wait/etc. checkpoints appear as direct children of the
// GRANDPARENT Map/Parallel context in the real, checkpointed operation
// log, instead of under an intermediate MAP_ITERATION/PARALLEL_BRANCH
// context - the entire point of FLAT nesting's own ~30% cost reduction
// (skipping that intermediate CONTEXT/START and CONTEXT/SUCCEED
// checkpoint pair, which the CALLER - runBatchItem - is responsible for
// actually skipping; this constructor only handles the resulting child
// Context's own ParentID/prefix values, not whether any checkpoint is
// sent for contextID itself).
func (c *Context) NewVirtualChildWithName(contextID, contextName, reportedParentID string) *Context {
	return c.newChildWithName(contextID, contextName, reportedParentID)
}

func (c *Context) newChildWithName(parentStepID, contextName, reportedParentID string) *Context {
	c.loggerMu.Lock()
	parentLogger, parentCfg := c.logger, c.loggerCfg
	c.loggerMu.Unlock()

	childFields := make(map[string]any, len(c.baseFields)+3)
	for k, v := range c.baseFields {
		childFields[k] = v
	}
	childFields["contextId"] = parentStepID
	childFields["operationId"] = parentStepID
	if contextName != "" {
		childFields["contextName"] = contextName
	}
	// parentId is THIS (the enclosing) context's own operation ID within
	// ITS parent, if any - i.e. c.baseFields["contextId"], which is unset
	// (no key) for the root context, matching the reference doc's
	// description of parentId as identifying "the operation ID of the
	// current child context" from the perspective of an operation nested
	// one level further in. A root-level child (this call) has no such
	// grandparent, so parentId is intentionally omitted rather than set
	// to an empty string, keeping a fresh Map/Parallel/RunInChildContext
	// call directly off the root indistinguishable-by-omission from one
	// with no parent, rather than a misleading explicit "".
	if parentContextID, ok := c.baseFields["contextId"]; ok {
		childFields["parentId"] = parentContextID
	}

	return &Context{
		execCtx:     c.execCtx,
		execARN:     c.execARN,
		logger:      parentLogger,
		loggerCfg:   parentCfg,
		baseFields:  childFields,
		execManager: c.execManager,
		checkpoint:  c.checkpoint,
		prefix:      parentStepID,
		// parentID equals parentStepID for an ordinary (non-virtual)
		// child context: parentStepID is the enclosing operation's own
		// already-hashed step ID (see NextStepID's doc - every ID this
		// SDK mints is now a SHA-256 hash by construction, so
		// parentStepID is never a plain string here), and per Java's
		// design (and this SDK's own established ParentID semantics
		// predating this task - see docs/remaining-work.md §0) that
		// value is used AS-IS for ParentID, with no additional hashing.
		//
		// For a VIRTUAL child (NewVirtualChildWithName), reportedParentID
		// is instead the GRANDPARENT's own ParentStepID() - see that
		// constructor's own doc for why: a virtual child has no
		// CONTEXT-operation identity of its own to report as ParentID,
		// so every nested operation created under it reports the
		// enclosing Map/Parallel's own parent instead, skipping this
		// virtual level entirely in the checkpointed hierarchy.
		parentID:    reportedParentID,
		replaying:   c.replaying,
		replayState: c.replayState,
		plugins:     c.plugins,
	}
}

func (c *Context) Context() context.Context { return c.execCtx }
func (c *Context) ExecutionARN() string     { return c.execARN }
func (c *Context) IsReplaying() bool        { return c.replaying }

// Plugins returns this invocation's EXPERIMENTAL instrumentation plugins
// (see pkg/durable/plugin's package doc), shared unchanged across the
// whole Context tree for this invocation. Used by the operations package
// to dispatch OnOperationStart/OnOperationEnd (and, in future increments,
// the rest of the hook surface) without needing its own separate plumbing
// for plugin configuration. Returns nil when no plugins are configured -
// callers should pass this directly to plugin.Dispatch, which is a no-op
// for a nil/empty slice.
func (c *Context) Plugins() []plugin.InstrumentationPlugin { return c.plugins }

// MarkRealExecution records that real (non-replay-skipped) execution has
// been reached in this invocation - see replayState's doc. Exposed for the
// operations package to call from the same places that already
// distinguish "replay-skip early return" from "actually run fn"
// (step.go's executeAndCheckpoint, wait_for_condition.go's
// pollAndCheckpoint), rather than duplicating that distinction here.
func (c *Context) MarkRealExecution() { c.replayState.markRealExecution() }

// Logger returns this Context's replay-aware, contextually-scoped logger,
// freshly wrapped on every call from the current logger/loggerCfg/
// baseFields (docs/remaining-work.md §5 tasks 13/14). It is intentionally
// NOT cached across calls - ConfigureLogger may run between two Logger()
// calls on the same Context (a legitimate sequence: log something with the
// default logger, then switch to a custom one for the rest of the
// handler), and the returned utils.ContextLogger's own doc explains why
// capturing modeAware/suppressed by value at construction time (rather
// than re-deriving them lazily) is exactly what makes that sequencing work
// correctly: a logger value obtained before a later ConfigureLogger call
// keeps behaving the way it did when obtained, matching JS/Java's
// documented "not retroactive" behavior for configureLogger.
func (c *Context) Logger() types.Logger {
	c.loggerMu.Lock()
	base, cfg := c.logger, c.loggerCfg
	c.loggerMu.Unlock()
	return utils.NewContextLogger(base, c.enrichedBaseFields(), cfg.ModeAware, c.replayState.isSuppressing)
}

// enrichedBaseFields returns c.baseFields merged with every configured
// EXPERIMENTAL instrumentation plugin's own EnrichLogContext() output
// (pkg/durable/plugin's package doc) - a no-op copy of baseFields alone
// when c has no configured plugins. Plugins are merged in configuration
// order, with LATER plugins' fields overriding EARLIER ones' on a key
// collision (consistent with plugin.WrapChain's own "first plugin is
// outermost" ordering: a later plugin's enrichment is a more specific,
// closer-to-the-log-call layer) - and c's OWN baseFields (executionArn,
// contextId, etc.) always take precedence over any plugin-contributed
// field of the same name, since those are this SDK's own core, always-
// present identifying fields, not something a plugin should be able to
// accidentally shadow.
func (c *Context) enrichedBaseFields() map[string]any {
	if len(c.plugins) == 0 {
		return c.baseFields
	}
	merged := make(map[string]any, len(c.baseFields))
	for _, p := range c.plugins {
		func() {
			defer func() { _ = recover() }() // see plugin.Dispatch's own doc on swallowing a misbehaving plugin's panic
			for k, v := range p.EnrichLogContext() {
				merged[k] = v
			}
		}()
	}
	for k, v := range c.baseFields {
		merged[k] = v
	}
	return merged
}

func (c *Context) ConfigureLogger(cfg types.LoggerConfig) {
	c.loggerMu.Lock()
	defer c.loggerMu.Unlock()
	if cfg.CustomLogger != nil {
		c.logger = cfg.CustomLogger
	}
	// ModeAware is taken as given, including its zero value (false) if
	// the caller's cfg didn't set it - see types.LoggerConfig.ModeAware's
	// doc for why an explicit ConfigureLogger call takes the value at
	// face value rather than applying durable.WithDurableExecution's
	// "default true when no LoggerConfig was configured at all" rule,
	// which only applies to the ROOT Context's initial construction, not
	// to a later ConfigureLogger call on it.
	c.loggerCfg = cfg
}

// hashOperationID returns the lowercase hex-encoded SHA-256 digest of
// rawID, with NO truncation (64 hex characters) - matching the confirmed
// real Java reference SDK's OperationIdGenerator.hashOperationId exactly
// (source read directly from
// raw.githubusercontent.com/aws/aws-durable-execution-sdk-java/main/sdk/src/main/java/software/amazon/lambda/durable/execution/OperationIdGenerator.java):
//
//	MessageDigest.getInstance("SHA-256").digest(rawId.getBytes(UTF_8))
//	HexFormat.of().formatHex(hash)
//
// Go's crypto/sha256 + encoding/hex reproduce this bit-for-bit: sha256.Sum256
// operates on the UTF-8 byte representation of a Go string (Go source
// strings are UTF-8 by definition, so no separate encoding step is
// needed the way Java's getBytes(StandardCharsets.UTF_8) requires), and
// hex.EncodeToString lowercases exactly like Java's HexFormat.of()
// default (HexFormat's default delimiter/prefix/suffix are all empty and
// its default case is lowercase - confirmed via the JDK's own
// HexFormat.of() javadoc, "returns a hex formatter with no delimiter and
// lowercase characters").
//
// # Why Java specifically, not JS or Python too
//
// JS and Python each hash operation IDs too, but with DIFFERENT
// algorithms/encodings from each other and from Java (confirmed by
// reading all three SDKs' actual source this session) - the three
// reference SDKs do not agree on one canonical hashing scheme. This Go
// SDK picks Java's as its one reference point rather than attempting to
// reconcile three mutually-inconsistent schemes into a fourth
// compromise; see docs/remaining-work.md's "SHA-256 operation ID
// hashing" section for the full reasoning. Nothing about this choice is
// wire-visible to a caller of ANY SDK reading back a Go SDK's own
// checkpointed IDs - the backend treats Id/ParentId as opaque
// caller-assigned strings matching [a-zA-Z0-9-_]+ regardless of which
// SDK produced them, so there is no cross-SDK ID-compatibility
// requirement this needs to satisfy; the requirement is only "hash
// instead of exposing a plain hierarchical string," and Java's own
// concrete scheme is as good a canonical choice as any of the three.
func hashOperationID(rawID string) string {
	sum := sha256.Sum256([]byte(rawID))
	return hex.EncodeToString(sum[:])
}

// hashInputPrefix returns the prefix fed into hashOperationID ahead of
// "-" + counter, mirroring Java's OperationIdGenerator constructor
// exactly: `this.operationIdPrefix = contextId != null ? contextId + "-" : ""`.
// Go has no null string, so the equivalent condition is prefix != "" -
// the empty string already means "no context ID at all" in both
// languages (Go's own zero value for a string IS "", so this is not a
// behavioral gap, just the natural Go phrasing of the same null-check).
// In practice, prefix is only ever "" here for a root Context constructed
// directly (e.g. by a test) without a real rootOperationID - see
// NewRoot's doc: durable.go's WithDurableExecution always supplies a
// real, non-empty rootOperationID from the backend's InitialExecutionState,
// so a genuine invocation's root Context's prefix is never actually
// empty in practice, but this function still needs to handle it
// correctly for the (test-only) case where it is, exactly as Java's own
// constructor does for its contextId == null case.
func hashInputPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	return prefix + "-"
}

// mintOrder records, process-globally, the order in which every hashed
// step ID this process has ever minted was produced by NextStepID -
// i.e. c.counter's value is deterministic and call-order-based (see
// NextStepID's own doc for why), but that information is otherwise lost
// the instant it's hashed away. This exists PURELY to let
// testing.EventSignatures reconstruct a deterministic sibling ordering
// (its Index field) now that operation IDs are opaque SHA-256 hashes
// with no numeric structure of their own to sort by (see that package's
// signature.go for the full consumer-side writeup) - it has NO bearing
// on this SDK's real runtime behavior (replay-skip lookups, checkpoint
// content, etc. all still work purely off the returned hash string
// itself, exactly as documented on NextStepID).
//
// # Why mint order, not checkpoint-arrival order
//
// An earlier draft of the EventSignatures fix (see
// docs/remaining-work.md's "SHA-256 operation ID hashing" writeup)
// tried stamping an ordinal at CHECKPOINT-arrival time instead (in
// testing/inmemory_client.go's Checkpoint method) rather than at
// mint time here. That was a real, deterministically-reproducible bug,
// caught immediately by the pre-existing map-parallel-go example's own
// golden-file test: Map/Parallel's runBatch (batch.go) already claims
// every branch/item's step ID deterministically BY INDEX on the single
// parent goroutine before spawning any branch goroutine (see that
// function's own doc, bug #3, for why - the exact same "must be
// deterministic, not goroutine-scheduling-order-dependent" property this
// mintOrder registry now also depends on) - but each branch's START
// CHECKPOINT is still enqueued later, from INSIDE that branch's own
// concurrently-running goroutine (runBatchItem). Checkpoint ARRIVAL
// order under concurrency is therefore NOT guaranteed to match mint/index
// order - confirmed by two branches' PARALLEL_BRANCH signatures
// swapping places (branch 1 arriving before branch 0) in a real, easily
// reproducible test run. Mint order does not have this problem: every ID
// affected by concurrency (Map/Parallel branches/items) is ALREADY
// minted sequentially, by index, on one single goroutine, specifically
// because production correctness (replay-safe step-ID assignment)
// already required that property - this registry just also records it.
//
// # Why process-global, not per-Context
//
// A per-Context registry would need to be threaded through NewChild
// exactly like replayState already is (see that type's doc) to remain
// visible to a child context's own NextStepID calls, for no real benefit
// over one shared map: SHA-256 hash collisions between genuinely
// different mint events are cryptographically negligible, so a single
// global map keyed by the hash itself carries no meaningful cross-test/
// cross-execution ambiguity risk in practice, and avoids adding yet
// another field to Context purely for a testing-package concern with
// zero runtime relevance (see this variable's own opening paragraph).
var (
	mintOrderMu sync.Mutex
	mintOrder   = make(map[string]int64)
	nextMintSeq int64
)

// recordMintOrder assigns id its global mint-order sequence number the
// FIRST time it's seen (PeekStepID may preview the same id's would-be
// hash repeatedly without ever calling this - see PeekStepID's own doc -
// so only NextStepID, which actually claims/advances the counter, calls
// this). A later call for an already-recorded id (which should not
// normally happen, since NextStepID's counter only ever advances, never
// revisits a prior value, for a given Context - but IS possible in
// principle across two entirely different invocations replaying the
// exact same execution, which legitimately mint the identical hash again
// by design - see NextStepID's determinism doc) is a harmless no-op,
// preserving the FIRST invocation's mint order rather than a later
// replay's.
func recordMintOrder(id string) {
	mintOrderMu.Lock()
	defer mintOrderMu.Unlock()
	if _, seen := mintOrder[id]; seen {
		return
	}
	mintOrder[id] = nextMintSeq
	nextMintSeq++
}

// MintOrder returns id's global mint-order sequence number (see
// mintOrder's doc) and whether one was ever recorded for it. Exported
// for pkg/durable/testing's EventSignatures to consume (that package
// does not otherwise import dcontext's internals) - see that package's
// signature.go for the consumer side of this same "how do we order
// siblings now that IDs are opaque hashes" problem.
func MintOrder(id string) (int64, bool) {
	mintOrderMu.Lock()
	defer mintOrderMu.Unlock()
	seq, ok := mintOrder[id]
	return seq, ok
}

// NextStepID mints and returns the next step ID in this context's
// namespace: SHA-256(hashInputPrefix(prefix) + counter) - i.e. the full
// 64-character lowercase hex digest of prefix + "-" + counter (or just
// counter if prefix is empty), never a plain/unhashed string. Mirrors the
// confirmed real Java reference SDK's OperationIdGenerator.nextOperationId
// exactly (see hashOperationID's doc for the source-verified algorithm and
// why Java specifically was chosen as this Go SDK's one reference point).
//
// # Hashing happens HERE, not deferred to checkpoint-send time
//
// This is a deliberate design choice matching Java (and diverging from
// the JS reference SDK, which defers its own hashing to checkpoint-send
// time instead - confirmed by reading its source this session): the
// value returned by this method IS the real, final, checkpointed Id from
// the moment it is minted. No plain/unhashed form of it is ever
// constructed, stored, logged as a "step ID," or exposed anywhere
// downstream of this function - not in types.OperationUpdate.ID (every
// call site across step.go/wait.go/callback.go/invoke.go/batch.go/
// wait_for_condition.go just uses this return value directly, unchanged
// from before this task), not in a child Context's own prefix (see
// NewChild's doc), and not in any log field this SDK emits (baseFields'
// contextId/operationId are populated from this same already-hashed
// return value too - see newChildWithName/NewStepContext). This matters
// for correctness, not just tidiness: if a raw, unhashed value ever
// escaped this function even transiently, ANY code that captured it
// (logging, an external system, a bug) would have observed
// backend-checkpoint-shape information this design is specifically meant
// to keep opaque.
//
// # Determinism across replay (the single most important correctness
// property here)
//
// Exactly like the pre-hashing plain-string scheme this replaces,
// NextStepID's Nth call within a given Context, across ANY number of
// replayed invocations of the exact same handler against the exact same
// execution, must always return the exact IDENTICAL value - this is what
// lets every operation's replay-skip lookup (c.ExecManager().GetOperation(id))
// find its own prior checkpoint by ID on a later invocation.
// hashOperationID is a pure function of its input string, and that input
// string (hashInputPrefix(c.prefix) + strconv form of c.counter) is
// itself fully determined by two things that are THEMSELVES guaranteed
// stable across replay: c.prefix (fixed at Context-construction time -
// see NewRoot/NewChild) and c.counter's value at the Nth call (guaranteed
// by this same mutex-guarded increment-then-read sequence every
// invocation takes, PROVIDED the handler's own operation-issuing order is
// itself deterministic across replay - a precondition this SDK has
// always required, predating this task, and unrelated to hashing
// specifically). See
// TestSHA256Hashing_SameHandlerReplayedTwiceProducesSameHashes in
// dcontext_test.go for a direct test of exactly this property.
func (c *Context) NextStepID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
	id := hashOperationID(fmt.Sprintf("%s%d", hashInputPrefix(c.prefix), c.counter))
	recordMintOrder(id)
	return id
}

// PeekStepID returns the step ID that the next NextStepID call would mint
// (the same SHA-256 hash NextStepID would return for this same next
// counter value), without advancing the counter. Used for the
// replay-lookup that must happen before an operation "claims" its ID (see
// docs/checkpoint-replay-design.md §3). Must stay byte-for-byte in sync
// with NextStepID's own hash-input construction - both share the exact
// same hashInputPrefix(c.prefix) + counter formula, differing only in
// whether the counter is read-after-increment (NextStepID, claiming this
// ID) or read-without-incrementing (PeekStepID, previewing the next one).
func (c *Context) PeekStepID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := c.counter + 1
	return hashOperationID(fmt.Sprintf("%s%d", hashInputPrefix(c.prefix), next))
}

// ParentStepID returns the operation ID that should populate ParentID on
// every types.OperationUpdate checkpointed directly against this
// Context - i.e. this Context's own parentID field (see that field's doc
// for why it is a separate field from prefix): empty for the root
// Context (a root-level operation has no parent, matching the official
// API reference's "Required: No" on Operation/OperationUpdate.ParentId -
// this remains true after this task's SHA-256-hashing change even though
// the root Context's prefix, used only as NextStepID's hash INPUT, is
// now the non-empty EXECUTION operation ID), or the enclosing operation's
// own ALREADY-HASHED step ID for a child Context returned by
// NewChild/NewChildWithName (RunInChildContext, a Map iteration, or a
// Parallel branch).
//
// No additional hashing happens here, matching the confirmed real Java
// reference SDK's own design exactly: Java's OperationIdGenerator only
// hashes when MINTING a new ID (nextOperationId) - an already-final ID
// (a child context's own previously-hashed prefix) is used verbatim as
// ParentID, never re-hashed a second time. c.parentID already holds
// exactly the right already-final value for this Context by
// construction (see NewRoot/NewChild), so this method itself needed no
// change for the hashing scheme - only the value fed INTO parentID
// upstream did (see NewChild, which now receives an already-hashed
// parentStepID argument instead of a plain hierarchical string).
//
// This is exactly the value the confirmed real-backend field is meant to
// carry - "the unique identifier of the parent operation, if this
// operation is running within a child context" - and every operation in
// pkg/durable/operations that constructs a types.OperationUpdate{} should
// set ParentID: c.ParentStepID() using the Context c it is checkpointing
// against, rather than leaving the field unset.
func (c *Context) ParentStepID() string { return c.parentID }

// ExecManager exposes the shared execmgr.Manager for use by the operations
// package (kept out of the public types.DurableContext interface since
// it's an SDK-internal coordination detail, not part of the user-facing
// API surface).
func (c *Context) ExecManager() *execmgr.Manager { return c.execManager }

// Checkpoint exposes the shared checkpoint.Manager for use by the
// operations package.
func (c *Context) Checkpoint() *checkpoint.Manager { return c.checkpoint }

// stepCtx is the concrete implementation of types.StepContext, passed into
// step/callback/condition-check bodies.
type stepCtx struct {
	ctx     context.Context
	logger  types.Logger
	attempt int
}

var _ types.StepContext = (*stepCtx)(nil)

func (s *stepCtx) Context() context.Context { return s.ctx }
func (s *stepCtx) Logger() types.Logger     { return s.logger }
func (s *stepCtx) Attempt() int             { return s.attempt }

// NewStepContext constructs a types.StepContext for a single operation
// attempt, auto-injecting operationId/operationName/attempt into c's own
// logger (docs/remaining-work.md §5 task 14, matching the confirmed
// cross-SDK "Operation context" execution metadata: operationId,
// operationName, attempt).
//
// Callers (step.go's executeAndCheckpoint, wait_for_condition.go's
// pollAndCheckpoint) call this ONLY from the real-execution path - never
// from a replay-skip early return - so the returned StepContext's logger
// is never subject to ModeAware suppression by construction (see
// types.StepContext.Logger's doc). This function itself also marks the
// enclosing Context's replayState as having crossed into real execution
// (task 13), since "a StepContext is about to run fn" and "real execution
// has been reached for ModeAware suppression purposes" are exactly the
// same event - see step.go/wait_for_condition.go's own doc comments on
// their replay-skip vs. real-execution branches, which this call site
// boundary was deliberately chosen to line up with.
func NewStepContext(c *Context, stepID, name string, attempt int) types.StepContext {
	c.MarkRealExecution()

	c.loggerMu.Lock()
	base, cfg := c.logger, c.loggerCfg
	c.loggerMu.Unlock()

	enriched := c.enrichedBaseFields()
	fields := make(map[string]any, len(enriched)+3)
	for k, v := range enriched {
		fields[k] = v
	}
	fields["operationId"] = stepID
	fields["operationName"] = name
	fields["attempt"] = attempt

	logger := utils.NewContextLogger(base, fields, cfg.ModeAware, c.replayState.isSuppressing)
	return &stepCtx{ctx: c.execCtx, logger: logger, attempt: attempt}
}

// CurrentTime returns the current wall-clock time. Exists as a named
// function (rather than inlining time.Now()) so the runtime's internal
// callers (e.g. computing NextAttemptTimestamp) have one obvious place to
// later swap in an injectable clock for testing, without implying this is
// the user-facing determinism helper — that is durable.CurrentTime, which
// is replay-safe. This function is NOT replay-safe and must only be used
// for SDK bookkeeping (e.g. computing a delay's absolute deadline right
// before checkpointing it), never returned to user code as an operation
// result.
func CurrentTime() time.Time {
	return time.Now()
}

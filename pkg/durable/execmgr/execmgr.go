// Package execmgr implements the Go analog of the Java SDK's
// ExecutionManager: it tracks the operation log for a single durable
// execution invocation, coordinates suspension when no goroutine can make
// forward progress, and owns the checkpoint queue.
//
// # Suspension model
//
// This mirrors the Java SDK's threaded suspension design (see
// docs/checkpoint-replay-design.md §4), which is the more direct analog for
// Go than the TypeScript SDK's single-threaded Promise.race design:
//
//   - Every running goroutine that is making forward progress on behalf of
//     this execution is "active." The handler goroutine registers itself
//     active on start.
//   - When a goroutine is about to block waiting on an operation that
//     hasn't completed (a Step still running on another goroutine, a Wait
//     timer, a pending Callback, an Invoke awaiting its target's result),
//     it deregisters itself as active *before* blocking, and re-registers
//     when unblocked.
//   - If deregistering drops the active count to zero, no goroutine
//     anywhere can make progress: the whole execution is suspended. The
//     Manager closes its Suspended channel.
//   - The top-level driver (durable.WithDurableExecution) races the
//     handler's completion against the Suspended channel. If suspension
//     wins, the Lambda invocation returns PENDING without waiting for the
//     handler goroutine, which remains blocked (harmlessly, since the
//     process is about to be frozen or recycled by the Lambda runtime).
package execmgr

import (
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// Manager coordinates a single durable execution invocation: the operation
// log, active-goroutine counting for suspension detection, and the
// checkpoint queue (see checkpoint.Manager, which this type delegates to).
type Manager struct {
	mu sync.Mutex

	// operations is the flat map of step ID -> checkpointed operation,
	// exactly mirroring the wire-protocol shape (types.Operation) so
	// lookups need no translation from what the backend returns.
	operations map[string]types.Operation

	// rootExecutionID is the operation ID of this execution's root
	// EXECUTION operation, set once via SetRootExecutionOperation. The
	// real backend assigns this dynamically per execution (confirmed via
	// a live invocation: it is a UUID-like value matching part of the
	// execution's ARN, not a fixed sentinel), so it must be looked up by
	// value rather than assumed.
	rootExecutionID string

	// activeCount is the number of goroutines currently able to make
	// forward progress on this execution's behalf. Guarded by mu.
	activeCount int

	// suspended is closed when activeCount drops to zero, and replaced
	// with a fresh, open channel as soon as activeCount rises above zero
	// again (see Register/Deregister) - mirroring
	// checkpoint.Manager's idleCh busy/idle channel-swap pattern, NOT a
	// permanent sync.Once-guarded close. This distinction is
	// load-bearing, not stylistic: a plain one-shot close would let ANY
	// transient zero-crossing of the active count permanently mark this
	// Manager's entire remaining lifetime as "suspended," even after
	// active count rises back above zero moments later. Map/Parallel's
	// scheduler goroutine hits exactly this transient zero-crossing in
	// the ordinary, successful case (deregistering while its own
	// branches finish, then intending to re-register and continue) -
	// found via a deterministically-reproducible test failure (not a
	// -race flake) while implementing those operations: a batch with
	// zero externally-blocking branches would still spuriously report
	// PENDING from WithDurableExecution's top-level select, because that
	// select races the handler's outcome channel against Suspended(),
	// and once Suspended() had EVER been closed by ANY past momentary
	// zero-crossing, it stayed "ready" forever afterward, regardless of
	// whether the execution was still actually suspended at the moment
	// the select actually ran.
	//
	// Guarded by the SAME mu as activeCount (not a separate lock): the
	// two are a single tightly-coupled invariant (suspended must be
	// closed if-and-only-if activeCount<=0), and guarding them with
	// separate locks would reopen a similar transient-inconsistency
	// window between updating one and updating the other.
	suspended chan struct{}

	// waiters maps a step ID to the set of channels to close when that
	// operation transitions to a terminal status. A single operation may
	// have multiple waiters if, e.g., both a direct getter and a
	// WaitForCondition poll are watching it (not expected in practice, but
	// the map supports it without special-casing).
	waiters map[string][]chan struct{}
}

// New creates a Manager seeded with previously checkpointed operations
// (e.g. from types.InitialExecutionState.Operations on replay). ops may be
// nil for a fresh execution with no prior history.
func New(ops []types.Operation) *Manager {
	m := &Manager{
		operations: make(map[string]types.Operation, len(ops)),
		suspended:  make(chan struct{}),
		waiters:    make(map[string][]chan struct{}),
	}
	for _, op := range ops {
		m.operations[op.ID] = op
	}
	return m
}

// Suspended returns the CURRENT channel that is closed when the
// execution has no remaining active goroutines - see the suspended
// field's doc for why this must be re-fetched via this method (never
// cached by a caller across a Register/Deregister transition) and why it
// is regenerated rather than closed exactly once. Select on the returned
// channel alongside the handler's completion signal to implement the
// suspend-or-complete race described in the package doc.
func (m *Manager) Suspended() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.suspended
}

// Register marks a new goroutine as active (able to make forward
// progress). Call this before spawning a goroutine that will do
// replay-safe work (e.g. a Step body, a child context), and on the
// top-level handler goroutine before invoking user code.
//
// If this transitions activeCount from zero back above zero, Register
// replaces the suspended channel with a fresh, open one (see that
// field's doc) - any goroutine already blocked selecting on the OLD
// suspended channel from a previous Suspended() call keeps observing
// that old (closed) channel, exactly matching how a real suspension
// that already started being observed should still resolve as
// suspended; only a NEW Suspended() call made after this Register
// returns will see the fresh, open replacement.
func (m *Manager) Register() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeCount++
	if m.activeCount == 1 {
		select {
		case <-m.suspended:
			// was closed (suspended) - replace with a fresh, open one
			m.suspended = make(chan struct{})
		default:
			// already open - nothing to do
		}
	}
}

// Deregister marks a goroutine as no longer active. If this drops the
// active count to zero, the execution is suspended: the CURRENT
// suspended channel (see Suspended) is closed. Unlike the original
// one-shot design, a LATER Register call that brings the count back
// above zero replaces this channel with a fresh one rather than leaving
// it permanently closed - see the suspended field's doc for why this
// matters.
//
// Callers MUST call Deregister only when about to block on an operation
// that has not yet completed, and MUST re-register (via Register) once
// unblocked, mirroring Java's register-before-submit /
// deregister-then-join pattern to avoid a race where the count reaches
// zero transiently between a completing goroutine's deregistration and a
// new one's registration.
func (m *Manager) Deregister() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeCount--
	if m.activeCount <= 0 {
		select {
		case <-m.suspended:
			// already closed - nothing to do
		default:
			close(m.suspended)
		}
	}
}

// GetOperation returns the checkpointed operation for id, if any. This is
// the replay-skip lookup: callers check the returned status before running
// an operation's body (see docs/checkpoint-replay-design.md §3).
func (m *Manager) GetOperation(id string) (types.Operation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op, ok := m.operations[id]
	return op, ok
}

// SetRootExecutionOperation records id as this execution's root EXECUTION
// operation, so RootExecutionOperation can retrieve it without callers
// needing to know or assume its ID (the real backend assigns this
// dynamically per execution - see the Manager.rootExecutionID doc).
func (m *Manager) SetRootExecutionOperation(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rootExecutionID = id
}

// RootExecutionOperation returns this execution's root EXECUTION
// operation, if SetRootExecutionOperation has been called and that
// operation is present in the log.
func (m *Manager) RootExecutionOperation() (types.Operation, bool) {
	m.mu.Lock()
	id := m.rootExecutionID
	m.mu.Unlock()
	if id == "" {
		return types.Operation{}, false
	}
	return m.GetOperation(id)
}

// PutOperation records or updates the checkpointed state for id. Called
// after a checkpoint API call acknowledges the update, so in-memory state
// always reflects the backend's last-known view.
//
// If this transitions id into a terminal status (Succeeded, Failed,
// Cancelled, Stopped, or TimedOut) OR into READY (see
// WaitForOperation's own doc for why READY is also treated as an
// immediate-return/wake condition, not just terminal - the
// SkipTime-driven synchronous retry case this exists for), PutOperation
// wakes any goroutines blocked in WaitForOperation on this id.
func (m *Manager) PutOperation(id string, op types.Operation) {
	m.mu.Lock()
	m.operations[id] = op
	var toNotify []chan struct{}
	if op.Status.IsTerminal() || op.Status == types.OperationStatusReady {
		toNotify = m.waiters[id]
		delete(m.waiters, id)
	}
	m.mu.Unlock()

	for _, ch := range toNotify {
		close(ch)
	}
}

// WaitForOperation blocks the calling goroutine until id reaches a
// terminal status (per types.OperationStatus.IsTerminal: Succeeded,
// Failed, Cancelled, Stopped, or TimedOut - not just Succeeded/Failed,
// since e.g. a CHAINED_INVOKE operation driven by operations.Invoke can
// reach TimedOut or Stopped directly, per the confirmed real-backend
// flowchart), coordinating suspension exactly like Java's
// BaseDurableOperation.waitForOperationCompletion():
//
//  1. If id is already terminal, return immediately without touching the
//     active-goroutine count (mirrors Java's "fast step" shortcut).
//  2. Otherwise, register a waiter channel, THEN deregister this goroutine
//     as active, THEN block on either the waiter channel or the
//     suspended channel.
//
// The registration-before-deregistration ordering (guarded by the same
// mutex as PutOperation) is what prevents the race Java's
// synchronized(completionFuture) block prevents: without it, the
// operation could complete and notify between the status check and
// deregistration, and this goroutine would deregister without ever being
// woken.
//
// Returns the terminal operation, or ok=false if the execution suspended
// before this operation completed (the caller should propagate
// suspension up rather than treat this as an error).
//
// # READY is also an immediate-return condition, not just terminal
//
// Found and fixed while investigating a genuine hang exposed by
// operations.Step's default retry strategy changing from NoRetry() to
// Presets.Default() (a real, nonzero-delay preset - see that preset's
// own doc): testing.inMemoryClient's SkipTime fast-forwarding
// (inmemory_client.go) resolves a STEP/RETRY checkpoint SYNCHRONOUSLY,
// within the SAME Enqueue call, by setting the operation's Status
// straight to READY (matching the real backend's own confirmed
// READY-not-PENDING transition once a retry delay elapses - see
// wait_for_condition.go's package doc for that finding) - but READY is
// deliberately NOT one of IsTerminal's five statuses (it specifically
// means "eligible for a FRESH invocation's replay entry to pick up," not
// "this in-process call is done" - see runStep's/runWaitForCondition's
// own `case OperationStatusPending, OperationStatusReady` replay-entry
// handling, which only ever runs at the TOP of a fresh invocation, never
// inside a blocking wait). Before this fix, retryOrFail's own call to
// WaitForOperation (right after checkpointing that same RETRY action)
// did not take the terminal fast path for READY, registered a waiter,
// and blocked forever: PutOperation only wakes waiters when the new
// status IsTerminal(), so a waiter registered against an
// already-READY-and-never-becoming-terminal operation is orphaned
// permanently - a real, deterministic hang, not a flaky race (confirmed
// via a real deployed example test, completion-config-go's
// TestHandler_ExceedsToleratedFailureThreshold, timing out completely
// under `go test -timeout`).
//
// Safe for the real (non-test) backend too: grepping pkg/durable/awssdk
// confirms its own checkpoint.Client implementation never produces
// OperationStatusReady synchronously from an Enqueue call at all - READY
// is only ever observed there via a later, FRESH invocation's own
// GetOperation lookup (exactly the replay-entry path this comment
// already describes), so this fast path can only ever trigger for the
// SkipTime-driven in-memory client's own synchronous retry simulation,
// never for a real execution.
func (m *Manager) WaitForOperation(id string) (op types.Operation, ok bool) {
	m.mu.Lock()
	if existing, found := m.operations[id]; found && (existing.Status.IsTerminal() || existing.Status == types.OperationStatusReady) {
		m.mu.Unlock()
		return existing, true
	}

	waitCh := make(chan struct{})
	m.waiters[id] = append(m.waiters[id], waitCh)
	m.mu.Unlock()

	m.Deregister()

	// Capture the CURRENT suspended channel via Suspended() (lock-guarded)
	// rather than reading m.suspended directly - the field can be
	// reassigned by a concurrent Register call (see that field's doc),
	// and reading it without the lock here would be a genuine data race
	// on top of the logic bug the field-swap itself fixes.
	select {
	case <-waitCh:
		m.Register()
		m.mu.Lock()
		op = m.operations[id]
		m.mu.Unlock()
		return op, true
	case <-m.Suspended():
		// Execution is suspending. Do not re-register: this goroutine is
		// intentionally abandoned (see package doc). Return ok=false so
		// the caller can propagate suspension instead of proceeding as if
		// it had a result.
		return types.Operation{}, false
	}
}

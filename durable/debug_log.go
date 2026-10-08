package durable

import (
	"context"
	"log/slog"
	"slices"
)

// Messages of the records the SDK writes at [slog.LevelDebug] about its own
// work. The handler level is the only gate: the records reach a handler
// only when it is enabled at [slog.LevelDebug].
const (
	debugMsgOperationClaimed     = "operation claimed"
	debugMsgReplayComplete       = "replay complete; executing live"
	debugMsgCheckpointEnqueued   = "checkpoint enqueued"
	debugMsgCheckpointFlushed    = "checkpoint flushed"
	debugMsgInvocationSuspending = "invocation suspending"
	debugMsgOperationCompleted   = "operation completed"
)

// Attribute names the SDK's Debug records carry beside the identity
// attributes every record of an invocation carries.
const (
	logKeyOperationSubtype = "operationSubtype"
	logKeyUpdateCount      = "updateCount"
	logKeyReason           = "reason"
	logKeyStatus           = "status"
)

// Values of the reason attribute of the invocation suspending record. Each
// names the kind of operation the invocation is pending on.
const (
	suspendReasonWait       = "wait"
	suspendReasonCallback   = "callback"
	suspendReasonInvoke     = "invoke"
	suspendReasonRetry      = "retry"
	suspendReasonCondition  = "condition"
	suspendReasonCombinator = "combinator"
)

// debugLog writes one SDK record at [slog.LevelDebug] through c's handler,
// with scope as the record's operation attributes in place of c's own, and
// attrs as its per-record attributes. scope nil writes the record with the
// execution attributes only.
//
// The record follows c's replay log mode, as a record from c's
// [Context.Logger] does: while c replays it is dropped under
// [ReplayLogModeSuppress] and written with the replay attribute under
// [ReplayLogModeEmit].
//
// Building the scoped handler costs allocations, so the level and the
// replay suppression are checked first. A handler that is not enabled at
// Debug costs one Enabled call per event.
func (c *execContext) debugLog(scope []slog.Attr, msg string, attrs ...slog.Attr) {
	c.debugLogReplaying(c.IsReplaying, scope, msg, attrs...)
}

// debugLogReplaying is debugLog with replaying as the record's replay
// predicate in place of c's replay state. A record about an operation
// outcome uses the outcome's own replay flag: an outcome is replayed when
// an earlier invocation already observed it, which can differ from
// whether c's code is replaying at the moment the outcome is reported.
func (c *execContext) debugLogReplaying(replaying func() bool, scope []slog.Attr, msg string, attrs ...slog.Attr) {
	cfg := c.logCfg.Load()
	if cfg == nil {
		return
	}
	if replaying() && !cfg.emitReplayed {
		return
	}
	if !cfg.handler.Enabled(c.Context, slog.LevelDebug) {
		return
	}
	newReplayLogger(c.scopedLogHandler(scope), replaying, c.emitReplayedLogs).
		LogAttrs(c.Context, slog.LevelDebug, msg, attrs...)
}

// invocationDebugLog writes one SDK record at [slog.LevelDebug] about the
// invocation itself through c's execution handler. The record reports the
// state of the invocation, not replayed handler code, so no replay
// suppression applies, as for the token-withdrawn warning.
func (c *execContext) invocationDebugLog(ctx context.Context, msg string, attrs ...slog.Attr) {
	cfg := c.logCfg.Load()
	if cfg == nil || !cfg.handler.Enabled(ctx, slog.LevelDebug) {
		return
	}
	slog.New(cfg.handler).LogAttrs(ctx, slog.LevelDebug, msg, attrs...)
}

// logOperationClaimed writes the operation claimed record for the operation
// with positional ID id, claimed on c.
func (c *execContext) logOperationClaimed(id, name, subType string) {
	c.debugLog(nil, debugMsgOperationClaimed, debugOperationAttrs(id, name, subType)...)
}

// logReplayComplete writes the record that c has left replay. It carries
// c's own scope, so a child context's record names the child operation.
func (c *execContext) logReplayComplete() {
	c.debugLog(c.logScope, debugMsgReplayComplete)
}

// logOperationCompleted writes the operation completed record for the
// operation info describes, which reached status on c. A status that is not
// terminal writes nothing. replaying is the record's replay predicate. For
// a checkpointed operation it is info.IsReplay: the outcome is replayed
// when an earlier invocation already observed it, whatever c's replay
// state is while the outcome is reported. A virtual child context records
// no outcome, so its record follows the child context's own replay state
// instead.
func (c *execContext) logOperationCompleted(replaying func() bool, info OperationHookInfo, status PluginOperationStatus) {
	var s string
	switch status {
	case PluginOperationSucceeded:
		s = string(PluginOperationSucceeded)
	case PluginOperationFailed, PluginOperationTimedOut, PluginOperationStopped, PluginOperationCancelled:
		s = string(PluginOperationFailed)
	default:
		return
	}
	attrs := append(debugOperationAttrs(info.ID, info.Name, info.SubType), slog.String(logKeyStatus, s))
	c.debugLogReplaying(replaying, nil, debugMsgOperationCompleted, attrs...)
}

// debugOperationAttrs are the operation attributes of the operation claimed
// and operation completed records: the wire ID, the wire subtype, and the
// name when the operation is named.
func debugOperationAttrs(id, name, subType string) []slog.Attr {
	attrs := []slog.Attr{
		slog.String(logKeyOperationID, hashID(id)),
		slog.String(logKeyOperationSubtype, subType),
	}
	if name != "" {
		attrs = append(attrs, slog.String(logKeyOperationName, name))
	}
	return attrs
}

// logCheckpoint writes a checkpoint record with updateCount updates. A
// request made by an operation carries that operation's context as ctx and
// follows its replay log mode. A request the invocation makes itself, a
// poll or the final write, carries the invocation context and is written
// without replay suppression.
func (c *execContext) logCheckpoint(ctx context.Context, msg string, updateCount int) {
	attr := slog.Int(logKeyUpdateCount, updateCount)
	if ec, ok := ctx.(*execContext); ok {
		ec.debugLog(nil, msg, attr)
		return
	}
	c.invocationDebugLog(ctx, msg, attr)
}

// suspendReasonFor returns the reason attribute for an invocation pending
// on the operation op: the kind of event the operation waits for.
func suspendReasonFor(op *operation) string {
	if op == nil {
		return suspendReasonCombinator
	}
	switch OperationType(op.opType) {
	case OperationTypeWait:
		return suspendReasonWait
	case OperationTypeCallback:
		return suspendReasonCallback
	case OperationTypeChainedInvoke:
		return suspendReasonInvoke
	case OperationTypeStep:
		if op.subType == OperationSubTypeWaitForCondition {
			return suspendReasonCondition
		}
		return suspendReasonRetry
	default:
		return suspendReasonCombinator
	}
}

// suspendReason returns the reason attribute of the invocation suspending
// record: the reason recorded when the signal fired, or, when it has not
// fired, the reason the watches state now. A watched operation decides it;
// when several are watched, the one with the least wire ID does, so the
// value does not depend on map order. With nothing watched, the invocation
// is pending on futures or on a commitment, and the reason is combinator.
func (s *suspendSignal) suspendReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firing {
		return s.firedReason
	}
	return s.watchReasonLocked()
}

// watchReasonLocked computes the reason from the current watches. Caller
// holds mu.
func (s *suspendSignal) watchReasonLocked() string {
	if len(s.watches) == 0 {
		return suspendReasonCombinator
	}
	ids := make([]string, 0, len(s.watches))
	for id := range s.watches {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	w := s.watches[ids[0]]
	if w.record == nil {
		return suspendReasonCombinator
	}
	return suspendReasonFor(w.record())
}

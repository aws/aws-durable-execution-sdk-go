// Package insight (continued - see record.go for the package-level doc).
package insight

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
)

// EmitMode controls when Workflow Insight sends a record to its
// configured exporters, matching the JS SDK's own emitMode option.
// Passing an unrecognized/zero EmitMode falls back to EmitModeOnComplete
// (this package's own default), matching the JS SDK's own "unrecognized
// value defaults, with a warning" convention (this Go port does not yet
// emit the warning - a smaller, separate follow-on).
type EmitMode string

const (
	// EmitModeOnComplete emits one record when the execution completes
	// (SUCCEEDED or FAILED). The default.
	EmitModeOnComplete EmitMode = "on-complete"

	// EmitModeOnFailure emits one record only when the execution ends in
	// FAILED - the lowest-overhead mode.
	EmitModeOnFailure EmitMode = "on-failure"

	// EmitModeOnChange emits a RUNNING snapshot on every operation change
	// (OnOperationStart/OnOperationEnd/OnOperationChange) IN ADDITION TO
	// the terminal SUCCEEDED/FAILED record at the end - real-time
	// monitoring, at the highest overhead of the three modes, matching
	// the JS SDK's own documented "on-change" behavior exactly.
	//
	// # Coalescing
	//
	// Matching the JS SDK's own documented "exports are coalesced: if
	// updates arrive faster than the exporter can handle, intermediate
	// snapshots are dropped" behavior: this Go port runs at most ONE
	// mid-flight dispatch at a time per Plugin instance (see
	// Plugin.scheduleOnChangeDispatch's own doc) - an operation change
	// that arrives while a dispatch for an EARLIER snapshot is still in
	// flight does not queue a second dispatch; it instead marks the
	// Plugin as needing ANOTHER dispatch once the current one finishes,
	// and that next dispatch always sends the LATEST snapshot at the
	// time it actually runs (never a stale, already-superseded one) -
	// since each record is a complete, self-contained snapshot, the
	// latest one is always sufficient and older ones carry no
	// information the latest lacks.
	EmitModeOnChange EmitMode = "on-change"
)

// OperationDetail controls which operations appear in a record's own
// Operations array, matching the JS SDK's own operationDetail option.
type OperationDetail string

const (
	// OperationDetailTopLevel includes only top-level operations
	// (anything with a non-empty ParentID is dropped) - produces the
	// SAME set of operations regardless of when a record is emitted, so
	// an execution that suspends and resumes never yields a partially-
	// populated tree. The default.
	OperationDetailTopLevel OperationDetail = "top-level"

	// OperationDetailFullTree includes every operation, including
	// children of contexts (parallel branches, map items, steps nested
	// inside a RunInChildContext, etc.).
	//
	// # Caveat: suspend/resume and pruned children
	//
	// Matching the JS SDK's own documented caveat exactly: by default
	// the backend prunes a finished context's children from the state
	// handed to later invocations (a performance optimization). So for
	// an execution that suspends/resumes, a full-tree record emitted
	// from a LATER invocation only contains the children of contexts
	// that were still active in THAT invocation - children of
	// already-finished contexts from an EARLIER invocation are missing
	// (this Go SDK's own plugin.OperationChangeInfo/OnOperationChange
	// mechanism, which this package's Plugin already uses to backfill
	// operations that finished in a prior invocation - see
	// Plugin.OnOperationChange's own doc - only ever reports UPDATED
	// operations, not an already-pruned child of an already-finished
	// context from further in the past). To keep the full tree across
	// resume, the JS SDK's own answer is a SEPARATE core-SDK setting
	// (pluginsConfig.childOperationsDepth on withDurableExecution) that
	// forces the backend to preserve children - this Go SDK has no
	// equivalent core-SDK option yet (durable.Config has no
	// PluginsConfig/ChildOperationsDepth field at all), so this
	// exporter-side caveat is currently UNMITIGATED in this Go port,
	// not just documented-but-worked-around - a real, honest gap
	// tracked as follow-on work at the core-SDK layer, not something
	// operationDetail alone can fix.
	OperationDetailFullTree OperationDetail = "full-tree"
)

// Config configures the Workflow Insight plugin, matching a subset of
// the JS SDK's own workflowInsight(config) options - see this package's
// own doc for what is and is not yet ported.
type Config struct {
	// EmitMode controls when a record is sent to Exporters. Defaults to
	// EmitModeOnComplete when left at its zero value.
	EmitMode EmitMode

	// OperationDetail controls which operations appear in the record's
	// own Operations array - see that type's own doc. Defaults to
	// OperationDetailTopLevel when left at its zero value, matching the
	// JS SDK's own default.
	OperationDetail OperationDetail

	// SamplingRate is the fraction of executions (0.0-1.0) that emit
	// records at all - matching the JS SDK's own samplingRate option.
	// Defaults to 1.0 (every execution) when left at its zero value,
	// exactly like the JS SDK's own default - Go's zero value for a
	// float64 (0.0) would otherwise mean "sample nothing," the opposite
	// of the documented default, so 0.0 specifically is treated as
	// "unset" here (matching this SDK's own established Go-idiom
	// precedent for a similar zero-value-collision problem - see
	// types.LoggerConfig.ModeAware's own doc in the core SDK for the
	// same reasoning applied there). To GENUINELY sample zero executions
	// (an unusual but valid configuration), use a very small positive
	// value instead (e.g. 1e-9) - this is the same tradeoff
	// LoggerConfig.ModeAware's own doc discusses for its own zero-value
	// collision, not a new inconsistency invented for this field.
	//
	// The decision is per-execution and all-or-nothing (an execution
	// that samples in emits every record it would otherwise emit; one
	// that samples out emits none), and deterministic across replays
	// (derived from a hash of ExecutionARN, which is stable across every
	// invocation of the same execution) - both matching the JS SDK's own
	// documented behavior exactly. Values outside [0, 1] are clamped
	// (matching the JS SDK's own "clamped/defaulted to 1.0" handling,
	// though this Go port does not yet emit the JS SDK's own
	// accompanying warning - a smaller, separate follow-on, matching
	// EmitMode's own doc on the same gap for an unrecognized value).
	SamplingRate float64

	// Exporters is where records are sent, in parallel - matching the JS
	// SDK's own "records are sent to all exporters in parallel; if one
	// fails, others still receive the record" contract. Defaults to a
	// single LambdaLogExporter when left nil/empty, matching the JS
	// SDK's own default.
	Exporters []Exporter

	// Content controls what data is included in emitted records - see
	// ContentConfig's own doc for exactly what this Go port implements.
	// Defaults to including everything (execution input/output as-is,
	// per-operation errors included, no operation exclusions) when left
	// at its zero value, matching the JS SDK's own documented default.
	Content ContentConfig
}

// arnPattern extracts the region, account ID, function name, and
// qualifier from a Lambda function ARN of the form
// "arn:aws:lambda:{region}:{account}:function:{name}:{qualifier}" (the
// qualifier segment, along with anything after it such as
// "/durable-execution/{id}/{invocationId}", is optional - a durable
// execution ARN's own extended form is handled by only requiring the
// first 7 colon-delimited segments to match, ignoring anything after).
var arnPattern = regexp.MustCompile(`^arn:aws:lambda:([^:]*):([^:]*):function:([^:]+)(?::([^:/]+))?`)

// parseFunctionARN extracts region/account/function name/qualifier from
// a Lambda function ARN, matching the JS SDK's own internal ARN parsing.
// Returns zero values for any component the ARN doesn't match (e.g. a
// malformed or unexpected ARN shape) rather than an error - this is
// best-effort enrichment of the emitted record, not a correctness-
// critical parse the plugin should ever fail an execution over.
func parseFunctionARN(arn string) (region, accountID, functionName, qualifier string) {
	m := arnPattern.FindStringSubmatch(arn)
	if m == nil {
		return "", "", "", ""
	}
	return m[1], m[2], m[3], m[4]
}

// Plugin is the EXPERIMENTAL Workflow Insight plugin.InstrumentationPlugin
// implementation. Construct via New; do not construct the zero value
// directly (its Exporters/EmitMode need defaulting, done in New).
type Plugin struct {
	plugin.NoopPlugin

	emitMode        EmitMode
	operationDetail OperationDetail
	samplingRate    float64
	exporters       []Exporter
	content         ContentConfig

	mu     sync.Mutex
	record *WorkflowInsightRecord
	byOpID map[string]int // operation ID -> index into record.Operations, for O(1) update-in-place

	// sampledIn is this invocation's own sampling decision, computed
	// once per OnInvocationStart (see that method's own doc for why a
	// fresh decision per invocation, not per Plugin instance, is
	// correct - the Plugin instance is reused across every invocation
	// of every execution the function handles, but the decision itself
	// is deterministic per-EXECUTION via ExecutionARN, so recomputing it
	// on every invocation of the SAME execution always reaches the same
	// answer, matching the JS SDK's own documented "deterministic across
	// replays" guarantee without needing to persist the decision
	// anywhere).
	sampledIn bool

	// onChangeDispatching/onChangeDirty implement EmitModeOnChange's own
	// documented coalescing behavior - see EmitModeOnChange's own doc for
	// the contract; see scheduleOnChangeDispatch for the implementation.
	// Guarded by the SAME mu as record/byOpID/sampledIn (not a separate
	// lock): a mid-flight dispatch reads record via a fresh shallow copy
	// taken while mu is held, exactly like OnInvocationEnd's own existing
	// pattern, so these two flags must be updated under that same
	// critical section to stay consistent with what "the latest
	// snapshot" actually means at any given instant.
	onChangeDispatching bool
	onChangeDirty       bool

	// onChangeWG lets OnInvocationEnd wait for any IN-FLIGHT on-change
	// dispatch (started by a PRIOR OnOperationStart/End/Change call) to
	// fully finish before sending its own final terminal record - two
	// concurrent dispatch() calls on the SAME Plugin instance are not
	// individually unsafe (each takes its own snapshot and each
	// exporter's own Export call is independently safe for concurrent
	// use - see every exporter's own doc), but the terminal
	// SUCCEEDED/FAILED record must always be observed AFTER any
	// preceding RUNNING snapshot, never interleaved with or overtaking
	// one, for a consumer tailing e.g. a single NDJSON file or a single
	// CloudWatch Logs stream to see a coherent order.
	onChangeWG sync.WaitGroup
}

// New constructs a Workflow Insight plugin.InstrumentationPlugin from
// cfg, matching the JS SDK's own workflowInsight(config) factory
// function. A zero-value Config (or Config{}) is valid and produces the
// JS SDK's own documented defaults: EmitModeOnComplete, SamplingRate 1.0
// (every execution), a single LambdaLogExporter.
func New(cfg Config) *Plugin {
	emitMode := cfg.EmitMode
	if emitMode != EmitModeOnFailure && emitMode != EmitModeOnChange {
		emitMode = EmitModeOnComplete
	}
	operationDetail := cfg.OperationDetail
	if operationDetail != OperationDetailFullTree {
		operationDetail = OperationDetailTopLevel
	}
	samplingRate := cfg.SamplingRate
	if samplingRate <= 0 || samplingRate > 1 {
		samplingRate = 1.0
	}
	exporters := cfg.Exporters
	if len(exporters) == 0 {
		exporters = []Exporter{NewLambdaLogExporter()}
	}
	return &Plugin{emitMode: emitMode, operationDetail: operationDetail, samplingRate: samplingRate, exporters: exporters, content: cfg.Content}
}

// filterTopLevel returns a NEW slice containing only ops with an empty
// ParentID (top-level operations - see OperationDetailTopLevel's own
// doc), preserving order. Never mutates ops itself, since ops is a
// slice living inside Plugin's own shared p.record.Operations, which
// must remain the FULL set for internal bookkeeping (upsertOperationLocked's
// own byOpID index) even when a particular emission's own rendered copy
// is filtered down to only top-level operations.
func filterTopLevel(ops []OperationRecord) []OperationRecord {
	filtered := make([]OperationRecord, 0, len(ops))
	for _, op := range ops {
		if op.ParentID == "" {
			filtered = append(filtered, op)
		}
	}
	return filtered
}

// sampledIn reports whether executionARN's own deterministic sampling
// decision, at the given rate, is "sample in" (true) or "sample out"
// (false) - matching the JS SDK's own documented "derived from a hash of
// the execution ARN, which is stable across replays" behavior. rate ==
// 1.0 always returns true without hashing anything (the common,
// unsampled case), avoiding needless work for the default configuration.
//
// Implementation: FNV-1a of executionARN, taken modulo 1e6 and compared
// against rate*1e6 - a simple, fast, non-cryptographic hash is
// appropriate here (this is a sampling decision, not a security
// boundary), and FNV-1a's own well-known good distribution for short
// ASCII strings (ARNs) avoids the kind of skew a weaker hash could
// introduce at extreme rates (e.g. 0.01).
func sampledIn(executionARN string, rate float64) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0 {
		return false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(executionARN))
	const buckets = 1_000_000
	bucket := h.Sum64() % buckets
	threshold := uint64(rate * float64(buckets))
	return bucket < threshold
}

// OnInvocationStart initializes this invocation's cumulative record (or,
// on a replay/resumed invocation, re-initializes it from scratch - see
// this method's own doc below for why that is correct here despite this
// SDK's plugin hooks not firing OnOperationStart/End again for
// already-completed operations on replay).
//
// # Why re-initializing per-invocation, rather than accumulating across
// # invocations, is correct
//
// A Plugin instance is constructed once per durable.Config (i.e. once
// per Lambda function, reused across every invocation of every
// execution that function handles - see durable.WithDurableExecution's
// own Config.Plugins field), NOT once per execution or per invocation.
// Naively accumulating this.record.Operations across calls would mix
// operations from UNRELATED executions together. Instead, OnInvocationStart
// resets the record fresh each call, then OnOperationChange (fired
// immediately after, by durable.WithDurableExecution, for a resumed
// invocation) repopulates it with every operation the backend reports as
// updated-or-otherwise-known so far - see that method's own doc for the
// full reasoning on why OnOperationChange, not OnOperationStart/End
// alone, is what makes a resumed invocation's record complete.
func (p *Plugin) OnInvocationStart(_ context.Context, info plugin.InvocationInfo) {
	region, accountID, functionName, qualifier := parseFunctionARN(info.ExecutionARN)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.sampledIn = sampledIn(info.ExecutionARN, p.samplingRate)
	p.record = &WorkflowInsightRecord{
		RecordType:        RecordType,
		SchemaVersion:     SchemaVersion,
		ExecutionARN:      info.ExecutionARN,
		FunctionName:      functionName,
		FunctionQualifier: qualifier,
		Region:            region,
		AccountID:         accountID,
		Status:            ExecutionStatusRunning,
		StartTime:         info.ExecutionStartTimestamp,
		Input:             info.ExecutionInput,
		Operations:        nil,
	}
	p.byOpID = make(map[string]int)
	for id, op := range info.UpdatedOperations {
		p.upsertOperationLocked(id, op)
	}
}

// OnOperationStart/OnOperationEnd record this operation's own current
// state into the cumulative record - see upsertOperationLocked for the
// shared upsert-by-ID logic both of these, and OnOperationChange, use.
// Under EmitModeOnChange, each call also schedules a mid-flight RUNNING
// snapshot dispatch - see scheduleOnChangeDispatch's own doc.
func (p *Plugin) OnOperationStart(ctx context.Context, info plugin.OperationInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

func (p *Plugin) OnOperationEnd(ctx context.Context, info plugin.OperationEndInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info.OperationInfo)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// OnOperationChange folds every externally-changed operation (a resumed
// Wait/Callback/Invoke becoming visible at the start of a NEW
// invocation) into the cumulative record. This is the mechanism that
// makes a MULTI-INVOCATION execution's final record complete: an
// operation that started and finished entirely within a PRIOR
// invocation (e.g. a Wait whose timer already elapsed before this
// invocation even began) never fires OnOperationStart/OnOperationEnd in
// THIS invocation at all - by the time this invocation's handler code
// re-runs past it, it is a pure replay-skip (see
// pkg/durable/plugin's own doc on why replay-skipped operations don't
// refire notification hooks). Without this method, such an operation
// would be silently missing from any record emitted by the invocation
// that actually completes the execution.
//
// Under EmitModeOnChange, this also schedules a mid-flight RUNNING
// snapshot dispatch, exactly like OnOperationStart/OnOperationEnd - see
// scheduleOnChangeDispatch's own doc.
func (p *Plugin) OnOperationChange(ctx context.Context, info plugin.OperationChangeInfo) {
	p.mu.Lock()
	for id, op := range info.UpdatedOperations {
		p.upsertOperationLocked(id, op)
	}
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// upsertOperationLocked inserts or updates, in place, the OperationRecord
// for operation id - called with p.mu already held. Skips the root
// EXECUTION operation itself (Type "Execution") - the JS SDK's own
// operations array never includes the execution's own root operation
// either, since WorkflowInsightRecord's own top-level fields (Status,
// StartTime, EndTime, Input, Output, Error) already cover it; including
// it a second time as a member of its own Operations array would be
// redundant and does not match the JS SDK's own documented schema/
// example record.
func (p *Plugin) upsertOperationLocked(id string, info plugin.OperationInfo) {
	if info.Type == "Execution" || info.Type == "EXECUTION" {
		return
	}
	rec := toOperationRecord(id, info)
	if idx, ok := p.byOpID[id]; ok {
		p.record.Operations[idx] = rec
		return
	}
	p.byOpID[id] = len(p.record.Operations)
	p.record.Operations = append(p.record.Operations, rec)
}

// toOperationRecord converts a plugin.OperationInfo into this package's
// own OperationRecord, computing DurationMs when both timestamps are
// present.
func toOperationRecord(id string, info plugin.OperationInfo) OperationRecord {
	rec := OperationRecord{
		ID:        id,
		Name:      info.Name,
		Type:      info.Type,
		SubType:   info.SubType,
		ParentID:  info.ParentID,
		Status:    string(info.Status),
		Attempt:   info.Attempt,
		rawResult: info.Result,
	}
	if !info.StartTimestamp.IsZero() {
		t := info.StartTimestamp
		rec.StartTime = &t
	}
	if !info.EndTimestamp.IsZero() {
		t := info.EndTimestamp
		rec.EndTime = &t
	}
	if rec.StartTime != nil && rec.EndTime != nil {
		d := rec.EndTime.Sub(*rec.StartTime).Milliseconds()
		rec.DurationMs = &d
	}
	if info.Error != nil {
		rec.Error = errorDetailFromErr(info.Error)
	}
	return rec
}

// errorDetailFromErr builds an ErrorDetail from a Go error, matching the
// JS SDK's own {name, message} error shape.
//
// # Message: innermost cause, not the SDK's own decorated wrapper text
//
// plugin.InvocationEndInfo.ExecutionError is deliberately the RAW Go
// error the handler returned (durable.go's dispatchInvocationEnd passes
// outcome.err unchanged) - appropriate for a plugin author debugging
// their own Go code, who wants the real, full error. But Workflow
// Insight's OWN record schema is meant to reflect the same clean,
// external-facing error the JS SDK's own {name, message} shape
// documents (and, in THIS SDK, exactly what durable.go's own
// outcomeToResponse/innermostErrorMessage already compute for the
// wire-level DurableExecutionOutput.Error.ErrorMessage - see that
// function's own extensive doc for why a plain, unconditional
// errors.Unwrap walk is WRONG here: it would also strip a HANDLER's own
// legitimate additional context, e.g. `fmt.Errorf("processing order
// %s: %w", orderID, err)`, not just this SDK's own internal
// decoration).
//
// This function applies the SAME narrow rule innermostErrorMessage does
// - unwrap ONLY when err ITSELF (not something merely reachable via a
// deeper search) implements `Cause() error` (operations.OperationError's
// own method, inherited by every operations-package failure type) -
// rather than duplicating durable.go's own unexported helper (which this
// separate Go module cannot import), since the reasoning is identical
// and the check is a few lines.
//
// # Name
//
// Go errors have no distinct "name" the way a JS Error's
// constructor.name does - this uses the OUTERMOST error's own dynamic
// Go type name (e.g. "StepFailedError" from
// "*operations.StepFailedError") as the closest equivalent, matching
// this SDK's own established precedent (durable.go's outcomeToResponse
// likewise uses outcome.err's own outer type for
// DurableExecutionOutput.ErrorType, deliberately NOT the unwrapped
// inner cause's type - see that function's own comment on why Message
// and ErrorType are unwrapped independently).
func errorDetailFromErr(err error) *ErrorDetail {
	if err == nil {
		return nil
	}
	name := typeName(err)
	msg := err.Error()
	if causer, ok := err.(interface{ Cause() error }); ok {
		if cause := causer.Cause(); cause != nil {
			msg = cause.Error()
		}
	}
	return &ErrorDetail{Name: name, Message: msg}
}

// typeName returns a short, human-readable type name for err (e.g.
// "StepFailedError" from "*operations.StepFailedError"), trimming the
// leading pointer marker and package-qualifier prefix - matching the
// spirit of a JS Error's own bare constructor.name (e.g. "TypeError",
// not "module.TypeError").
func typeName(err error) string {
	full := fmt.Sprintf("%T", err)
	full = strings.TrimPrefix(full, "*")
	if idx := strings.LastIndex(full, "."); idx >= 0 {
		return full[idx+1:]
	}
	if full == "" {
		return "Error"
	}
	return full
}

// OnInvocationEnd finalizes this invocation's cumulative record with the
// terminal outcome and, if EmitMode's own gate allows it (see
// shouldEmit), sends the record to every configured Exporter in
// parallel, then flushes any Exporter that also implements Flusher -
// matching the JS SDK's own "wrapInvocation drains all pending exports
// before the Lambda returns" contract, adapted to this SDK's own
// synchronous-hook (not Promise-based) model: this Go port dispatches
// AND flushes synchronously within OnInvocationEnd itself, rather than
// splitting the "schedule" and "drain" steps across two separate hooks
// the way the JS SDK's own onInvocationEnd + wrapInvocation split does -
// both hooks are already awaited by this SDK's own plugin.Dispatch
// contract, so there is no equivalent need for a separate drain step.
//
// A Pending/suspended invocation (Status other than Succeeded/Failed) is
// intentionally NOT finalized or emitted here - see this method's own
// early return: this SDK's plugin.InstrumentationPlugin interface has no
// distinct "invocation suspended" status value that maps to the JS SDK's
// own PluginInvocationStatus.PENDING (this port's plugin.InvocationStatus
// does define InvocationStatusPending, but durable.WithDurableExecution's
// own dispatch of OnInvocationEnd on suspension - see that function's own
// doc - passes an InvocationEndInfo with no ExecutionResult/
// ExecutionError, only Status: Pending), and a suspended invocation's
// record is, by definition, incomplete (more operations may still be
// pending) - the JS SDK's own "on-change" mode (not yet ported - see this
// package's doc) is the intended mechanism for mid-flight visibility, not
// a bare Pending status on invocation end.
func (p *Plugin) OnInvocationEnd(ctx context.Context, info plugin.InvocationEndInfo) {
	if info.Status == plugin.InvocationStatusPending || info.Status == plugin.InvocationStatusRetrying {
		return
	}

	p.mu.Lock()
	if p.record == nil {
		// Defensive: OnInvocationEnd fired without a preceding
		// OnInvocationStart having initialized p.record at all (should
		// never happen given durable.WithDurableExecution's own call
		// ordering, but avoids a nil-pointer panic if some future
		// caller ever violates that ordering).
		p.mu.Unlock()
		return
	}
	now := time.Now()
	p.record.EndTime = &now
	d := now.Sub(p.record.StartTime).Milliseconds()
	p.record.DurationMs = &d
	if info.Status == plugin.InvocationStatusSucceeded {
		p.record.Status = ExecutionStatusSucceeded
		p.record.Output = info.ExecutionResult
	} else {
		p.record.Status = ExecutionStatusFailed
		p.record.Error = errorDetailFromErr(info.ExecutionError)
	}
	record := *p.record // shallow copy - Operations slice is shared but never mutated again after this point (p.record is about to be dropped/reset on the NEXT OnInvocationStart)
	sampledIn := p.sampledIn
	if p.operationDetail != OperationDetailFullTree {
		record.Operations = filterTopLevel(record.Operations)
	}
	applyContent(&record, p.content)
	p.mu.Unlock()

	if !sampledIn || !p.shouldEmit(record.Status) {
		return
	}
	// Wait for any IN-FLIGHT on-change dispatch (from a prior
	// OnOperationStart/End/Change call) to fully finish before sending
	// this invocation's own final, terminal record - see onChangeWG's
	// own doc for why ordering matters here even though neither
	// dispatch() call is individually unsafe to run concurrently.
	p.onChangeWG.Wait()
	p.dispatch(ctx, record)
}

// shouldEmit applies EmitMode's own gating for the TERMINAL
// (SUCCEEDED/FAILED) record specifically: EmitModeOnFailure only emits
// for a FAILED execution; EmitModeOnComplete and EmitModeOnChange both
// emit for either terminal outcome (EmitModeOnChange's own ADDITIONAL
// mid-flight RUNNING snapshots are gated separately, in
// scheduleOnChangeDispatch, not here). Callers must ALSO separately
// check the invocation's own sampledIn decision (see OnInvocationEnd's
// own call site) - sampling and EmitMode are two independent gates, and
// an execution that sampled OUT must emit nothing regardless of what
// EmitMode would otherwise allow.
func (p *Plugin) shouldEmit(status ExecutionStatus) bool {
	if p.emitMode == EmitModeOnFailure {
		return status == ExecutionStatusFailed
	}
	return true
}

// scheduleOnChangeDispatch implements EmitModeOnChange's own documented
// mid-flight RUNNING-snapshot + coalescing behavior (see that constant's
// own doc for the full contract) - called from OnOperationStart/
// OnOperationEnd/OnOperationChange on every invocation, but only
// actually schedules anything when EmitMode is EmitModeOnChange AND this
// invocation's own sampling decision is sampled-in; a no-op (returns
// immediately, no lock contention beyond the gate check) otherwise.
//
// # Coalescing implementation
//
// If a dispatch is already running (p.onChangeDispatching), this call
// just sets p.onChangeDirty and returns - the ALREADY-RUNNING dispatch's
// own goroutine (see the loop below) is responsible for noticing
// onChangeDirty once it finishes its current Export/Flush round and
// looping to send another, fresher snapshot; this avoids spawning a
// second concurrent dispatch goroutine for the same Plugin instance
// entirely; only ONE dispatch goroutine is ever running at a time.
//
// onChangeWG.Add(1)/Done() brackets the ENTIRE loop (every iteration,
// not just the first), so OnInvocationEnd's own onChangeWG.Wait() call
// correctly blocks until every queued-up snapshot this loop sends has
// actually finished, not just the first one.
func (p *Plugin) scheduleOnChangeDispatch(ctx context.Context) {
	if p.emitMode != EmitModeOnChange {
		return
	}

	p.mu.Lock()
	if !p.sampledIn {
		p.mu.Unlock()
		return
	}
	if p.onChangeDispatching {
		p.onChangeDirty = true
		p.mu.Unlock()
		return
	}
	p.onChangeDispatching = true
	p.mu.Unlock()

	p.onChangeWG.Add(1)
	go func() {
		defer p.onChangeWG.Done()
		for {
			p.mu.Lock()
			p.onChangeDirty = false
			var record WorkflowInsightRecord
			hasRecord := p.record != nil
			if hasRecord {
				record = *p.record
				if p.operationDetail != OperationDetailFullTree {
					record.Operations = filterTopLevel(record.Operations)
				}
				applyContent(&record, p.content)
			}
			p.mu.Unlock()

			if hasRecord {
				p.dispatch(ctx, record)
			}

			p.mu.Lock()
			stillDirty := p.onChangeDirty
			if !stillDirty {
				p.onChangeDispatching = false
			}
			p.mu.Unlock()
			if !stillDirty {
				return
			}
		}
	}()
}

// dispatch sends record to every configured Exporter concurrently,
// swallowing each exporter's own error independently (an exporter
// failure never affects another exporter or the execution outcome -
// matching the JS SDK's own documented contract), then flushes every
// Exporter that also implements Flusher.
//
// Each exporter that implements SizeLimiter receives its OWN,
// INDEPENDENTLY truncated copy of record (see Truncate's own doc for the
// drop order, and SizeLimiter's own doc for why different exporters on
// the same Plugin can legitimately receive different copies of the same
// logical record) - the truncation itself happens HERE, per exporter,
// rather than once up front on a single shared copy, precisely so a
// smaller-limited exporter's own truncation never affects what a
// larger-limited (or unlimited) exporter receives.
func (p *Plugin) dispatch(ctx context.Context, record WorkflowInsightRecord) {
	var wg sync.WaitGroup
	wg.Add(len(p.exporters))
	for _, exp := range p.exporters {
		exp := exp
		go func() {
			defer wg.Done()
			defer func() { _ = recover() }() // a misbehaving exporter must never affect execution correctness or other exporters, matching plugin.Dispatch's own panic-recovery precedent
			toSend := record
			if rl, ok := exp.(RenderingSizeLimiter); ok {
				toSend = Truncate(record, rl.MaxRecordSizeBytes(), rl.Render)
			} else if limiter, ok := exp.(SizeLimiter); ok {
				toSend = Truncate(record, limiter.MaxRecordSizeBytes(), nil)
			}
			_ = exp.Export(ctx, toSend)
		}()
	}
	wg.Wait()

	for _, exp := range p.exporters {
		if f, ok := exp.(Flusher); ok {
			func() {
				defer func() { _ = recover() }()
				_ = f.Flush(ctx)
			}()
		}
	}
}

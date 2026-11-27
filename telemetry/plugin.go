package telemetry

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// OperationDetail controls which operations appear in a record's
// Operations array.
type OperationDetail string

const (
	// OperationDetailTopLevel includes only top-level operations
	// (anything with a non-empty ParentID is dropped).
	OperationDetailTopLevel OperationDetail = "top-level"

	// OperationDetailFullTree includes every operation, including
	// children of contexts.
	OperationDetailFullTree OperationDetail = "full-tree"
)

// Config configures the Durable Execution Telemetry plugin.
type Config struct {
	// EmitMode controls when a record is sent to Exporters. Defaults to
	// EmitOnComplete when left at its zero value.
	EmitMode EmitMode

	// OperationDetail controls which operations appear in the record.
	// Defaults to OperationDetailTopLevel.
	OperationDetail OperationDetail

	// SamplingRate is the fraction of executions (0.0-1.0) that emit
	// records. Defaults to 1.0 (every execution) when left at zero.
	// The decision is per-execution and deterministic (derived from a
	// hash of ExecutionArn).
	SamplingRate float64

	// Exporters is where records are sent in parallel. Defaults to a
	// single LambdaLogExporter when left nil/empty.
	Exporters []Exporter

	// Content controls what data is included in emitted records.
	Content ContentConfig
}

// TelemetryPlugin is the Durable Execution Telemetry plugin
// factory. Construct via New; do not construct the zero value
// directly.
type TelemetryPlugin struct {
	emitMode        EmitMode
	operationDetail OperationDetail
	samplingRate    float64
	exporters       []Exporter
	content         ContentConfig
}

// telemetryInvocation owns the mutable state of one invocation.
// The hooks and log enrichment can overlap within that invocation.
// So mu protects the record, the result cache, and dispatch bookkeeping.
//
// The plugin keeps no state of its own for an operation, except the result
// cache. Each hook receives the full set of checkpointed operations. The
// record's operation list is rebuilt from that set; see setOperationsLocked.
type telemetryInvocation struct {
	env *TelemetryPlugin

	mu     sync.Mutex
	record *Record

	// results maps an operation ID to the Result last recorded for it.
	// Several hooks report the same unchanged result for one operation.
	// So setOperationsLocked reuses the stored value when the name and
	// text match, and the Result transform runs only when one changes.
	results map[string]recordedResult

	// ended prevents new on-change dispatches once invocation finalization starts.
	// Protected by mu, so every WaitGroup Add precedes the final Wait.
	ended bool

	// onChangeDispatching/onChangeDirty implement EmitOnChange's
	// coalescing behavior: at most one dispatch goroutine runs at a
	// time per Plugin instance.
	onChangeDispatching bool
	onChangeDirty       bool

	// onChangeWG lets OnInvocationEnd wait for any in-flight on-change
	// dispatch to finish before sending the final terminal record.
	onChangeWG sync.WaitGroup

	// lastEmittedMs is the Unix millisecond of the last emitted record.
	// See nextEmittedAtLocked.
	lastEmittedMs int64
}

// recordedResult is the Result recorded for one operation, with the
// operation name and the checkpointed result text that produced it.
type recordedResult struct {
	name  string
	text  string
	value json.RawMessage
}

// New constructs a Durable Execution Telemetry plugin from cfg. A
// zero-value Config produces documented defaults: EmitOnComplete,
// SamplingRate 1.0, a single LambdaLogExporter.
func New(cfg Config) *TelemetryPlugin {
	emitMode := cfg.EmitMode
	if emitMode != EmitOnChange && emitMode != EmitAlways {
		emitMode = EmitOnComplete
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

	return &TelemetryPlugin{
		emitMode:        emitMode,
		operationDetail: operationDetail,
		samplingRate:    samplingRate,
		exporters:       exporters,
		content:         cfg.Content,
	}
}

// TelemetryPlugin implements durable.PluginFactory, so pass it to
// durable.WithPlugins directly.
var _ durable.PluginFactory = (*TelemetryPlugin)(nil)

// CreatePlugin returns hooks bound to fresh invocation-local state.
// Configuration and exporters remain shared on the factory. The record,
// mutex, and on-change dispatch state belong to the returned plugin alone.
//
// The sampling decision is computed here from the execution ARN. So the
// same execution has the same decision across invocations. A sampled-out
// invocation gets a plugin with EnrichLogContext only; see the comment in
// the function body.
func (p *TelemetryPlugin) CreatePlugin(_ context.Context, info durable.InvocationHookInfo) (*durable.Plugin, error) {
	region, accountID, functionName, qualifier := parseFunctionARN(info.ExecutionArn)
	inv := &telemetryInvocation{
		env:     p,
		record:  NewRecord(info.ExecutionArn),
		results: make(map[string]recordedResult),
	}
	inv.record.FunctionName = functionName
	inv.record.FunctionQualifier = qualifier
	inv.record.Region = region
	inv.record.AccountID = accountID
	inv.record.Status = StatusRunning

	// EnrichLogContext is registered for every invocation. So log records
	// carry the same fields whatever the sampling decision.
	plugin := &durable.Plugin{EnrichLogContext: inv.enrichLogContext}

	// A sampled-out invocation emits no record. The SDK tracks operation
	// changes and builds the Operations maps of OnOperationChange and
	// OnInvocationEnd only for a plugin that implements one of those
	// hooks. So a sampled-out invocation registers neither, and the SDK
	// skips that work for it. The SDK still builds the Operations map of
	// InvocationHookInfo, because it builds that map before CreatePlugin
	// runs. No transform runs for a sampled-out invocation either.
	if !sampledIn(info.ExecutionArn, p.samplingRate) {
		return plugin, nil
	}

	start := resolveExecutionStart(info)
	inv.record.StartTimestamp = &start
	inv.record.Input = p.content.inputValue(info.ExecutionInput)
	plugin.OnInvocationStart = inv.onInvocationStart
	plugin.OnInvocationEnd = inv.onInvocationEnd
	// Only EmitOnChange emits between invocation start and end. The SDK
	// builds a full operations map for each OnOperationChange call. So the
	// other emit modes do not register the hook.
	if p.emitMode == EmitOnChange {
		plugin.OnOperationChange = inv.onOperationChange
	}
	return plugin, nil
}

// arnPattern extracts region, account ID, function name, and qualifier
// from a Lambda function ARN in any partition.
var arnPattern = regexp.MustCompile(`^arn:[^:]+:lambda:([^:]*):([^:]*):function:([^:]+)(?::([^:/]+))?`)

// parseFunctionARN extracts region/account/function name/qualifier from
// a Lambda function ARN. Returns zero values for unmatched components.
func parseFunctionARN(arn string) (region, accountID, functionName, qualifier string) {
	m := arnPattern.FindStringSubmatch(arn)
	if m == nil {
		return "", "", "", ""
	}
	return m[1], m[2], m[3], m[4]
}

// resolveExecutionStart returns the execution start for a record. The
// wire payload normally reports it as ExecutionStartTimestamp. When it
// does not, the oldest operation start in the invocation snapshot is the
// next best replay-stable instant: on a resumed execution it predates this
// invocation, where time.Now would yield a duration covering only the
// final invocation. time.Now is the last resort, so the record always
// has a start and a terminal record always has a duration.
func resolveExecutionStart(info durable.InvocationHookInfo) time.Time {
	if !info.ExecutionStartTimestamp.IsZero() {
		return info.ExecutionStartTimestamp
	}
	var oldest time.Time
	for _, op := range info.Operations {
		if op.StartTimestamp.IsZero() {
			continue
		}
		if oldest.IsZero() || op.StartTimestamp.Before(oldest) {
			oldest = op.StartTimestamp
		}
	}
	if !oldest.IsZero() {
		return oldest
	}
	return time.Now()
}

// nextEmittedAtLocked returns the EmittedAt value for the next emitted
// record. Records serialize emittedAt at millisecond precision. The SQS
// FIFO deduplication ID and the default DynamoDB sort key are
// executionArn plus emittedAt. Two records of one invocation in the same
// millisecond, such as an on-change snapshot and the terminal record,
// would then make SQS drop the second message and DynamoDB overwrite the
// first item. So the returned value is at least one millisecond after the
// previous one. Called with p.mu held.
func (p *telemetryInvocation) nextEmittedAtLocked() time.Time {
	ms := time.Now().UnixMilli()
	if ms <= p.lastEmittedMs {
		ms = p.lastEmittedMs + 1
	}
	p.lastEmittedMs = ms
	return time.UnixMilli(ms).UTC()
}

// sampledIn reports whether executionArn's deterministic sampling
// decision at the given rate is "sample in" (true). Uses FNV-1a hash
// of the ARN for stable, non-cryptographic distribution.
func sampledIn(executionArn string, rate float64) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0 {
		return false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(executionArn))
	const buckets = 1_000_000
	bucket := h.Sum64() % buckets
	threshold := uint64(rate * float64(buckets))
	return bucket < threshold
}

// onInvocationStart emits the first running record of the invocation in
// EmitOnChange. The record lists the operations checkpointed before this
// invocation. The other emit modes emit only at invocation end, so the hook
// does nothing for them.
func (p *telemetryInvocation) onInvocationStart(ctx context.Context, info durable.InvocationHookInfo) {
	if p.env.emitMode != EmitOnChange {
		return
	}
	p.mu.Lock()
	p.setOperationsLocked(info.Operations)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// onOperationChange emits a running record after a checkpoint response
// changed the status of at least one operation. The record lists every
// operation checkpointed so far. The hook is registered only in
// EmitOnChange.
//
// The SDK dispatches the changes of one invocation one at a time, in the
// order the responses arrived. So a later call carries a newer set of
// operations, and replacing the whole list keeps the newest status.
func (p *telemetryInvocation) onOperationChange(ctx context.Context, info durable.OperationChangeHookInfo) {
	p.mu.Lock()
	p.setOperationsLocked(info.Operations)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// setOperationsLocked replaces the record's operation list with the
// operations of snapshot, the Operations map of a hook info. Called with
// p.mu held.
//
// The list is ordered by start timestamp, then ID; see
// orderedOperationIDs. The root EXECUTION operation is skipped: the
// record's top-level fields cover it.
func (p *telemetryInvocation) setOperationsLocked(snapshot map[string]durable.OperationHookInfo) {
	ops := make([]OperationRecord, 0, len(snapshot))
	for _, id := range orderedOperationIDs(snapshot) {
		info := snapshot[id]
		if info.Type == "EXECUTION" || info.Type == "Execution" {
			continue
		}
		rec := toOperationRecord(id, info)
		rec.Result = p.resultLocked(id, info)
		ops = append(ops, rec)
	}
	p.record.Operations = ops
}

// resultLocked returns the Result to record for an operation. Called with
// p.mu held.
//
// When the operation's name and result text equal those that produced the
// stored Result, the stored Result is returned and no transform runs. The
// name is compared because overrides are matched by name. Otherwise
// resultValue runs the transform, and the outcome is stored.
func (p *telemetryInvocation) resultLocked(id string, info durable.OperationHookInfo) json.RawMessage {
	if prev, seen := p.results[id]; seen && prev.name == info.Name && prev.text == info.Result {
		return prev.value
	}
	value := p.env.content.resultValue(info.Name, info.Result)
	p.results[id] = recordedResult{name: info.Name, text: info.Result, value: value}
	return value
}

// orderedOperationIDs orders a checkpoint snapshot for oldest-first truncation.
// Snapshot maps have no iteration order, but Truncate removes a slice prefix.
// Missing timestamps come last because those operations may not have started.
func orderedOperationIDs(ops map[string]durable.OperationHookInfo) []string {
	ids := make([]string, 0, len(ops))
	for id := range ops {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b string) int {
		ta, tb := ops[a].StartTimestamp, ops[b].StartTimestamp
		switch {
		case ta.IsZero() && !tb.IsZero():
			return 1
		case !ta.IsZero() && tb.IsZero():
			return -1
		}
		if order := ta.Compare(tb); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
	return ids
}

// onInvocationEnd builds the record's operation list from info.Operations,
// finalizes the record with the terminal outcome, and emits to exporters if
// the emit mode allows. A pending or retrying outcome
// means the execution continues in a later invocation, so it leaves the
// record running; EmitAlways and EmitOnChange still emit a record for it.
//
// info.Operations holds every operation checkpointed when the invocation
// ends. So the emitted record does not depend on the delivery of the last
// OnOperationChange, which the SDK drops when the invocation's context ends
// first.
func (p *telemetryInvocation) onInvocationEnd(ctx context.Context, info durable.InvocationEndHookInfo) {
	p.mu.Lock()
	p.ended = true
	p.mu.Unlock()

	continuing := info.Status == durable.PluginInvocationPending || info.Status == durable.PluginInvocationRetrying
	if continuing && !p.env.shouldEmit(StatusRunning) {
		return
	}

	// Finish this invocation's on-change dispatches before the last record
	// is built. ended is set, so no new dispatch starts. The last record is
	// therefore emitted after every other record of the invocation.
	p.onChangeWG.Wait()

	p.mu.Lock()
	p.setOperationsLocked(info.Operations)
	if continuing {
		// The execution continues after this invocation: keep the
		// running status and leave the end timestamp unset.
		record := p.snapshotRecordLocked()
		record.EmittedAt = p.nextEmittedAtLocked()
		p.mu.Unlock()

		p.env.dispatch(ctx, record)
		return
	}

	now := time.Now()
	p.record.EndTimestamp = &now
	if p.record.StartTimestamp != nil {
		d := now.Sub(*p.record.StartTimestamp).Milliseconds()
		p.record.DurationMs = &d
	}
	// Refresh EmittedAt to the actual emission time.
	p.record.EmittedAt = p.nextEmittedAtLocked()

	if info.Status == durable.PluginInvocationSucceeded {
		p.record.Status = StatusSucceeded
		p.record.Output = p.env.content.outputValue(info.ExecutionResult)
	} else {
		p.record.Status = StatusFailed
		if info.ExecutionError != nil {
			p.record.Error = errorRecordFromErr(info.ExecutionError)
		}
	}

	// Take a snapshot for emission.
	record := p.snapshotRecordLocked()
	p.mu.Unlock()

	if !p.env.shouldEmit(record.Status) {
		return
	}
	p.env.dispatch(ctx, record)
}

// enrichLogContext adds execution metadata to the log context.
func (p *telemetryInvocation) enrichLogContext(_ context.Context) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := map[string]any{
		"execution_arn": p.record.ExecutionArn,
	}
	if p.record.FunctionName != "" {
		m["function_name"] = p.record.FunctionName
	}
	return m
}

// snapshotRecordLocked returns an emission-ready copy of the current
// record: operations are filtered per the configured detail level (or
// copied so in-place content filtering cannot corrupt the live record)
// and content redaction is applied. Called with p.mu held.
func (p *telemetryInvocation) snapshotRecordLocked() Record {
	record := *p.record
	if p.env.operationDetail != OperationDetailFullTree {
		record.Operations = filterTopLevel(record.Operations)
	} else {
		ops := make([]OperationRecord, len(record.Operations))
		copy(ops, record.Operations)
		record.Operations = ops
	}
	p.env.content.applyOperations(&record)
	return record
}

// toOperationRecord converts an entry of a hook info's Operations map into
// an OperationRecord, computing DurationMs when both timestamps are
// present. It leaves Result unset; setOperationsLocked sets it.
//
// The entry's Error is a *durable.OperationError built from the checkpoint,
// or nil when the checkpoint holds no error type. The record's error
// carries the checkpointed error type and message. So a failed operation
// whose checkpoint holds no error type has no error in the record, even
// when the checkpoint holds an error message.
func toOperationRecord(id string, info durable.OperationHookInfo) OperationRecord {
	rec := OperationRecord{
		ID:      id,
		Name:    info.Name,
		Type:    info.Type,
		SubType: info.SubType,
		Status:  string(info.Status),
		Attempt: info.Attempt,
	}
	if info.ParentID != "" {
		rec.ParentID = info.ParentID
	}
	if !info.StartTimestamp.IsZero() {
		t := info.StartTimestamp
		rec.StartTimestamp = &t
	}
	if !info.EndTimestamp.IsZero() {
		t := info.EndTimestamp
		rec.EndTimestamp = &t
	}
	if rec.StartTimestamp != nil && rec.EndTimestamp != nil {
		d := rec.EndTimestamp.Sub(*rec.StartTimestamp).Milliseconds()
		rec.DurationMs = &d
	}
	var opErr *durable.OperationError
	if errors.As(info.Error, &opErr) {
		rec.Error = &ErrorRecord{Type: opErr.ErrorType, Message: opErr.Message}
	}
	return rec
}

// errorRecordFromErr builds the ErrorRecord of the execution from the Go
// error the handler returned. Unwraps one level via Cause() (for SDK
// operation errors) to get the clean message.
func errorRecordFromErr(err error) *ErrorRecord {
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
	return &ErrorRecord{Type: name, Message: msg}
}

// typeName returns a short, human-readable type name for err (e.g.
// "StepError" from "*durable.StepError").
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

// filterTopLevel returns a new slice containing only operations with an
// empty ParentID. Never mutates the input slice.
func filterTopLevel(ops []OperationRecord) []OperationRecord {
	filtered := make([]OperationRecord, 0, len(ops))
	for _, op := range ops {
		if op.ParentID == "" {
			filtered = append(filtered, op)
		}
	}
	return filtered
}

// shouldEmit reports whether the emit mode emits a record with the given
// status at invocation end. A pending or retrying invocation ends with
// StatusRunning.
func (p *TelemetryPlugin) shouldEmit(status Status) bool {
	switch p.emitMode {
	case EmitOnComplete:
		return status == StatusSucceeded || status == StatusFailed
	case EmitAlways:
		return true
	case EmitOnChange:
		// EmitOnChange also emits running records between invocation
		// start and end, through scheduleOnChangeDispatch.
		return true
	default:
		return status == StatusSucceeded || status == StatusFailed
	}
}

// scheduleOnChangeDispatch implements EmitOnChange's coalescing: at most
// one dispatch goroutine runs at a time. Its two callers, onInvocationStart
// and onOperationChange, call it only in EmitOnChange.
func (p *telemetryInvocation) scheduleOnChangeDispatch(ctx context.Context) {
	p.mu.Lock()
	if p.ended {
		p.mu.Unlock()
		return
	}
	if p.onChangeDispatching {
		p.onChangeDirty = true
		p.mu.Unlock()
		return
	}
	p.onChangeDispatching = true
	// Add while holding mu, before onInvocationEnd can set ended and
	// begin waiting. Once ended is set, no new dispatch can be added.
	p.onChangeWG.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.onChangeWG.Done()
		for {
			p.mu.Lock()
			p.onChangeDirty = false
			record := p.snapshotRecordLocked()
			record.EmittedAt = p.nextEmittedAtLocked()
			p.mu.Unlock()

			p.env.dispatch(ctx, record)

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
// swallowing each exporter's error independently, then flushes every
// Exporter that implements Flusher.
func (p *TelemetryPlugin) dispatch(ctx context.Context, record Record) {
	var wg sync.WaitGroup
	wg.Add(len(p.exporters))
	for _, exp := range p.exporters {
		exp := exp
		go func() {
			defer wg.Done()
			defer func() { _ = recover() }()
			toSend := record
			if rl, ok := exp.(RenderingSizeLimiter); ok {
				toSend = Truncate(toSend, rl.MaxRecordSizeBytes(), rl.Render)
			} else if limiter, ok := exp.(SizeLimiter); ok {
				toSend = Truncate(toSend, limiter.MaxRecordSizeBytes(), nil)
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

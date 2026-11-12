package telemetry

import (
	"cmp"
	"context"
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
// Operation hooks and log enrichment can overlap within that invocation.
// So mu protects the record, operation index, and dispatch bookkeeping.
type telemetryInvocation struct {
	env *TelemetryPlugin

	mu     sync.Mutex
	record *Record
	byOpID map[string]int // operation ID -> index in record.Operations

	// sampledIn is computed in CreatePlugin from the execution ARN.
	// The same execution has the same decision across invocations.
	sampledIn bool

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
		content:         defaultContent(cfg.Content),
	}
}

// defaultContent applies defaults for a zero-value ContentConfig: input,
// output, operation errors, and the execution error are included;
// operation results are not. This matches DefaultContentConfig.
//
// Note: the zero-value detection is all-or-nothing. If ANY Include* field
// is set to true, the others are NOT defaulted. Users who partially
// configure ContentConfig should set all Include* fields explicitly.
func defaultContent(c ContentConfig) ContentConfig {
	// If all Include* fields are false (the zero value), it means the
	// caller did not configure content — default to everything included.
	if !c.IncludeInput && !c.IncludeOutput && !c.IncludeOperationResults &&
		!c.IncludeOperationErrors && !c.IncludeExecutionError {
		return DefaultContentConfig()
	}
	return c
}

// TelemetryPlugin implements durable.PluginFactory, so pass it to
// durable.WithPlugins directly.
var _ durable.PluginFactory = (*TelemetryPlugin)(nil)

// CreatePlugin returns hooks bound to fresh invocation-local state.
// Configuration and exporters remain shared on the factory. The record,
// sampling decision, mutex, and on-change dispatch state belong to the
// returned plugin alone.
func (p *TelemetryPlugin) CreatePlugin(_ context.Context, info durable.InvocationHookInfo) (*durable.Plugin, error) {
	region, accountID, functionName, qualifier := parseFunctionARN(info.ExecutionArn)
	inv := &telemetryInvocation{
		env:       p,
		record:    NewRecord(info.ExecutionArn),
		byOpID:    make(map[string]int),
		sampledIn: sampledIn(info.ExecutionArn, p.samplingRate),
	}
	inv.record.FunctionName = functionName
	inv.record.FunctionQualifier = qualifier
	inv.record.Region = region
	inv.record.AccountID = accountID
	inv.record.Status = StatusRunning
	start := resolveExecutionStart(info)
	inv.record.StartTimestamp = &start
	// Build content only when it is included, so the Redactor never sees
	// a value that ApplyToRecord would drop at snapshot time.
	if p.content.IncludeInput {
		inv.record.Input = p.content.marshalContent(info.ExecutionInput)
	}

	return &durable.Plugin{
		OnInvocationStart: inv.onInvocationStart,
		OnInvocationEnd:   inv.onInvocationEnd,
		OnOperationStart:  inv.onOperationStart,
		OnOperationEnd:    inv.onOperationEnd,
		OnOperationChange: inv.onOperationChange,
		EnrichLogContext:  inv.enrichLogContext,
	}, nil
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

// onInvocationStart restores the full checkpointed operation snapshot.
// Completed child contexts can return without executing their bodies again.
// So their unchanged nested operations must come from this snapshot.
func (p *telemetryInvocation) onInvocationStart(_ context.Context, info durable.InvocationHookInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range orderedOperationIDs(info.Operations) {
		p.upsertOperationLocked(id, info.Operations[id])
	}
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

// onInvocationEnd finalizes the record with the terminal outcome and
// emits to exporters if the emit mode and sampling gates allow. A pending
// or retrying outcome means the execution continues in a later invocation,
// so it leaves the record running; EmitAlways still emits a snapshot for
// it, since that mode reports every invocation regardless of outcome.
func (p *telemetryInvocation) onInvocationEnd(ctx context.Context, info durable.InvocationEndHookInfo) {
	if info.Status == durable.PluginInvocationPending || info.Status == durable.PluginInvocationRetrying {
		if p.env.emitMode == EmitOnChange {
			// Finish this invocation's on-change dispatches before its
			// final running snapshot and before the end hook returns.
			p.onChangeWG.Wait()
		}
		if p.env.emitMode != EmitAlways && p.env.emitMode != EmitOnChange {
			return
		}
		p.mu.Lock()
		// The execution continues after this invocation: keep the
		// running status and leave the end timestamp unset.
		record := p.snapshotRecordLocked()
		record.EmittedAt = p.nextEmittedAtLocked()
		sampledIn := p.sampledIn
		p.mu.Unlock()

		if !sampledIn {
			return
		}
		p.env.dispatch(ctx, record)
		return
	}

	p.mu.Lock()

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
		if p.env.content.IncludeOutput {
			p.record.Output = p.env.content.marshalContent(info.ExecutionResult)
		}
	} else {
		p.record.Status = StatusFailed
		if info.ExecutionError != nil {
			p.record.Error = errorRecordFromErr(info.ExecutionError)
		}
	}

	// Take a snapshot for emission.
	record := p.snapshotRecordLocked()
	sampledIn := p.sampledIn
	p.mu.Unlock()

	if !sampledIn || !p.env.shouldEmit(record.Status) {
		return
	}

	// Wait for any in-flight on-change dispatch to finish before
	// sending the terminal record (ordering guarantee).
	p.onChangeWG.Wait()
	p.env.dispatch(ctx, record)
}

// onOperationStart records the operation's start into the cumulative record.
func (p *telemetryInvocation) onOperationStart(ctx context.Context, info durable.OperationHookInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// onOperationEnd records the operation's completion.
func (p *telemetryInvocation) onOperationEnd(ctx context.Context, info durable.OperationHookInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// onOperationChange folds externally-changed operations into the record.
func (p *telemetryInvocation) onOperationChange(ctx context.Context, info durable.OperationChangeHookInfo) {
	p.mu.Lock()
	for id, opInfo := range info.UpdatedOperations {
		p.upsertOperationLocked(id, opInfo)
	}
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
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
	p.env.content.ApplyToRecord(&record)
	return record
}

// upsertOperationLocked inserts or updates in place the OperationRecord
// for the given operation ID. Skips the root EXECUTION operation (already
// covered by the record's top-level fields). Called with p.mu held.
func (p *telemetryInvocation) upsertOperationLocked(id string, info durable.OperationHookInfo) {
	if info.Type == "EXECUTION" || info.Type == "Execution" {
		return
	}
	rec := toOperationRecord(id, info, p.env.content)
	if idx, ok := p.byOpID[id]; ok {
		p.record.Operations[idx] = rec
		return
	}
	p.byOpID[id] = len(p.record.Operations)
	p.record.Operations = append(p.record.Operations, rec)
}

// toOperationRecord converts a durable.OperationHookInfo into an
// OperationRecord, computing DurationMs when both timestamps are present.
// The result is built only when content includes operation results, so
// the Redactor never runs on a value that ApplyToRecord would drop.
func toOperationRecord(id string, info durable.OperationHookInfo, content ContentConfig) OperationRecord {
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
	if content.IncludeOperationResults {
		rec.Result = content.contentValue(info.Result)
	}
	if info.Error != nil {
		rec.Error = errorRecordFromErr(info.Error)
	}
	return rec
}

// errorRecordFromErr builds an ErrorRecord from a Go error. Unwraps one
// level via Cause() (for SDK operation errors) to get the clean message.
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

// shouldEmit applies EmitMode's gating for the terminal record.
func (p *TelemetryPlugin) shouldEmit(status Status) bool {
	switch p.emitMode {
	case EmitOnComplete:
		return status == StatusSucceeded || status == StatusFailed
	case EmitAlways:
		return true
	case EmitOnChange:
		// EmitOnChange emits mid-flight RUNNING snapshots via
		// scheduleOnChangeDispatch AND the terminal record.
		return true
	default:
		return status == StatusSucceeded || status == StatusFailed
	}
}

// scheduleOnChangeDispatch implements EmitOnChange's coalescing: at most
// one dispatch goroutine runs at a time. No-op when EmitMode is not
// EmitOnChange or the invocation is sampled out.
func (p *telemetryInvocation) scheduleOnChangeDispatch(ctx context.Context) {
	if p.env.emitMode != EmitOnChange {
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
	// Add before Unlock. onInvocationEnd calls Wait once it observes a
	// dispatch in flight. An Add after the Unlock would leave a window
	// where Wait sees a zero counter and returns early, so the final
	// record could precede this dispatch.
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

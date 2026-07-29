package insight

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
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

// Config configures the Workflow Insight plugin.
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

// InsightPlugin is the Workflow Insight plugin implementation. Construct
// via New; do not construct the zero value directly.
type InsightPlugin struct {
	emitMode        EmitMode
	operationDetail OperationDetail
	samplingRate    float64
	exporters       []Exporter
	content         ContentConfig

	mu     sync.Mutex
	record *Record
	byOpID map[string]int // operation ID -> index in record.Operations

	// sampledIn is this invocation's sampling decision, computed once
	// per OnInvocationStart. Deterministic per execution via
	// ExecutionArn hash.
	sampledIn bool

	// countArn and invocationCount track how many invocations of the
	// current execution this plugin instance has observed. countArn is
	// the execution identity (see executionIdentity); the counter
	// resets when a different execution's invocation arrives.
	countArn        string
	invocationCount int

	// onChangeDispatching/onChangeDirty implement EmitOnChange's
	// coalescing behavior: at most one dispatch goroutine runs at a
	// time per Plugin instance.
	onChangeDispatching bool
	onChangeDirty       bool

	// onChangeWG lets OnInvocationEnd wait for any in-flight on-change
	// dispatch to finish before sending the final terminal record.
	onChangeWG sync.WaitGroup
}

// New constructs a Workflow Insight plugin from cfg. A zero-value Config
// produces documented defaults: EmitOnComplete, SamplingRate 1.0, a
// single LambdaLogExporter.
func New(cfg Config) *InsightPlugin {
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

	return &InsightPlugin{
		emitMode:        emitMode,
		operationDetail: operationDetail,
		samplingRate:    samplingRate,
		exporters:       exporters,
		content:         defaultContent(cfg.Content),
	}
}

// defaultContent applies defaults for a zero-value ContentConfig: all
// content is included. This matches the documented default behavior.
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

// Plugin returns a durable.Plugin struct wired to this InsightPlugin's
// lifecycle hooks. Pass the result to durable.WithPlugins().
func (p *InsightPlugin) Plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: p.onInvocationStart,
		OnInvocationEnd:   p.onInvocationEnd,
		OnOperationStart:  p.onOperationStart,
		OnOperationEnd:    p.onOperationEnd,
		OnOperationChange: p.onOperationChange,
		EnrichLogContext:  p.enrichLogContext,
	}
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

// executionIdentity returns the portion of a durable execution ARN that
// identifies the execution, excluding any trailing per-invocation segment.
// For an ARN without the durable-execution marker, the whole ARN is the
// identity.
func executionIdentity(arn string) string {
	const marker = "/durable-execution/"
	idx := strings.Index(arn, marker)
	if idx < 0 {
		return arn
	}
	rest := arn[idx+len(marker):]
	if slash := strings.Index(rest, "/"); slash >= 0 {
		return arn[:idx+len(marker)+slash]
	}
	return arn
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

// onInvocationStart initializes this invocation's cumulative record.
func (p *InsightPlugin) onInvocationStart(_ context.Context, info durable.InvocationHookInfo) {
	_, _, functionName, _ := parseFunctionARN(info.ExecutionArn)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.sampledIn = sampledIn(info.ExecutionArn, p.samplingRate)

	// NewRecord sets EmittedAt and extracts ExecutionName.
	p.record = NewRecord(info.ExecutionArn)
	p.record.FunctionName = functionName
	p.record.Status = StatusRunning

	// Count the invocations of this execution observed by this plugin
	// instance. The comparison uses the execution identity, since the
	// ARN can carry a per-invocation trailing segment. A
	// first-invocation signal restarts the count even when the same
	// execution identity is seen again.
	identity := executionIdentity(info.ExecutionArn)
	if identity != p.countArn || info.IsFirstInvocation {
		p.countArn = identity
		p.invocationCount = 0
	}
	p.invocationCount++
	p.record.InvocationCount = p.invocationCount
	if !info.ExecutionStartTimestamp.IsZero() {
		t := info.ExecutionStartTimestamp
		p.record.StartTimestamp = &t
	}

	// Store execution input as a content field.
	if info.ExecutionInput != nil {
		if raw, err := marshalInput(info.ExecutionInput); err == nil && raw != "" {
			p.record.Input = p.content.BuildContentField(raw)
		}
	}

	p.byOpID = make(map[string]int)

	// Backfill operations that changed externally between invocations.
	for id, opInfo := range info.UpdatedOperations {
		p.upsertOperationLocked(id, opInfo)
	}
}

// onInvocationEnd finalizes the record with the terminal outcome and
// emits to exporters if the emit mode and sampling gates allow. A pending
// outcome leaves the record running; EmitAlways still emits a snapshot
// for it, since that mode reports every invocation regardless of outcome.
func (p *InsightPlugin) onInvocationEnd(ctx context.Context, info durable.InvocationEndHookInfo) {
	if info.Status == durable.PluginInvocationPending {
		if p.emitMode == EmitOnChange {
			// Drain any in-flight on-change dispatch so records are not
			// orphaned or contaminate the next invocation.
			p.onChangeWG.Wait()
		}
		if p.emitMode != EmitAlways && p.emitMode != EmitOnChange {
			return
		}
		p.mu.Lock()
		if p.record == nil {
			p.mu.Unlock()
			return
		}
		// The execution continues after this invocation: keep the
		// running status and leave the end timestamp unset.
		record := p.snapshotRecordLocked()
		record.EmittedAt = time.Now()
		sampledIn := p.sampledIn
		p.mu.Unlock()

		if !sampledIn {
			return
		}
		p.dispatch(ctx, record)
		return
	}

	p.mu.Lock()
	if p.record == nil {
		p.mu.Unlock()
		return
	}

	now := time.Now()
	p.record.EndTimestamp = &now
	if p.record.StartTimestamp != nil {
		d := now.Sub(*p.record.StartTimestamp).Milliseconds()
		p.record.DurationMs = &d
	}
	// Refresh EmittedAt to the actual emission time.
	p.record.EmittedAt = now

	if info.Status == durable.PluginInvocationSucceeded {
		p.record.Status = StatusSucceeded
		if info.ExecutionResult != nil {
			if raw, err := marshalInput(info.ExecutionResult); err == nil && raw != "" {
				p.record.Output = p.content.BuildContentField(raw)
			}
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

	if !sampledIn || !p.shouldEmit(record.Status) {
		return
	}

	// Wait for any in-flight on-change dispatch to finish before
	// sending the terminal record (ordering guarantee).
	p.onChangeWG.Wait()
	p.dispatch(ctx, record)
}

// onOperationStart records the operation's start into the cumulative record.
func (p *InsightPlugin) onOperationStart(ctx context.Context, info durable.OperationHookInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// onOperationEnd records the operation's completion.
func (p *InsightPlugin) onOperationEnd(ctx context.Context, info durable.OperationHookInfo) {
	p.mu.Lock()
	p.upsertOperationLocked(info.ID, info)
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// onOperationChange folds externally-changed operations into the record.
func (p *InsightPlugin) onOperationChange(ctx context.Context, info durable.OperationChangeHookInfo) {
	p.mu.Lock()
	for id, opInfo := range info.UpdatedOperations {
		p.upsertOperationLocked(id, opInfo)
	}
	p.mu.Unlock()
	p.scheduleOnChangeDispatch(ctx)
}

// enrichLogContext adds execution metadata to the log context.
func (p *InsightPlugin) enrichLogContext(_ context.Context) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.record == nil {
		return nil
	}
	m := map[string]any{
		"execution_arn": p.record.ExecutionArn,
	}
	if p.record.FunctionName != "" {
		m["function_name"] = p.record.FunctionName
	}
	if p.record.InvocationCount > 0 {
		m["invocation_count"] = p.record.InvocationCount
	}
	return m
}

// snapshotRecordLocked returns an emission-ready copy of the current
// record: operations are filtered per the configured detail level (or
// copied so in-place content filtering cannot corrupt the live record)
// and content redaction is applied. Called with p.mu held; p.record must
// be non-nil.
func (p *InsightPlugin) snapshotRecordLocked() Record {
	record := *p.record
	if p.operationDetail != OperationDetailFullTree {
		record.Operations = filterTopLevel(record.Operations)
	} else {
		ops := make([]OperationRecord, len(record.Operations))
		copy(ops, record.Operations)
		record.Operations = ops
	}
	p.content.ApplyToRecord(&record)
	return record
}

// upsertOperationLocked inserts or updates in place the OperationRecord
// for the given operation ID. Skips the root EXECUTION operation (already
// covered by the record's top-level fields). Called with p.mu held.
func (p *InsightPlugin) upsertOperationLocked(id string, info durable.OperationHookInfo) {
	if info.Type == "EXECUTION" || info.Type == "Execution" {
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

// toOperationRecord converts a durable.OperationHookInfo into an
// OperationRecord, computing DurationMs when both timestamps are present.
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
	if info.Result != "" {
		rec.Result = &ContentField{Value: info.Result}
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
func (p *InsightPlugin) shouldEmit(status Status) bool {
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
func (p *InsightPlugin) scheduleOnChangeDispatch(ctx context.Context) {
	if p.emitMode != EmitOnChange {
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
			var record Record
			hasRecord := p.record != nil
			if hasRecord {
				record = p.snapshotRecordLocked()
				// Refresh EmittedAt for this snapshot.
				record.EmittedAt = time.Now()
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
// swallowing each exporter's error independently, then flushes every
// Exporter that implements Flusher.
func (p *InsightPlugin) dispatch(ctx context.Context, record Record) {
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

// marshalInput serializes an arbitrary value to JSON string for storage
// in a ContentField. Returns empty string on marshal failure.
func marshalInput(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

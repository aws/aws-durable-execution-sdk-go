package durable

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Field names of the records the default log handler emits. They match the
// names the other durable execution SDKs use, so one CloudWatch query works
// across languages.
const (
	logKeyTimestamp     = "timestamp"
	logKeyMessage       = "message"
	logKeyRequestID     = "requestId"
	logKeyExecutionArn  = "executionArn"
	logKeyTenantID      = "tenantId"
	logKeyOperationID   = "operationId"
	logKeyOperationName = "operationName"
	logKeyAttempt       = "attempt"
	logKeyReplay        = "replay"
	logKeyErrorType     = "errorType"
	logKeyErrorMessage  = "errorMessage"
	logKeyStackTrace    = "stackTrace"
)

// logLevelEnvVar is the environment variable that selects the default
// handler's minimum level. Lambda sets it from the function's logging
// configuration.
const logLevelEnvVar = "AWS_LAMBDA_LOG_LEVEL"

// logTimestampLayout renders a record time as ISO 8601 UTC with
// millisecond precision and a Z suffix, e.g. 2026-09-17T04:56:45.657Z.
const logTimestampLayout = "2006-01-02T15:04:05.000Z"

// newDefaultLogHandler returns the handler [Wrap] installs when no
// [WithLogHandler] option is given: a JSON handler writing to w whose
// records carry timestamp, level, and message under those names. An
// attribute whose value is an error is expanded into errorType and
// errorMessage; stackTrace is added when the error carries recorded
// frames. level is the minimum level; records below it are dropped before
// any formatting.
func newDefaultLogHandler(w io.Writer, level slog.Leveler) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceDefaultAttr,
	})
}

// replaceDefaultAttr renames the built-in time and message keys, formats
// the time, and expands error values. It is called for every attribute at
// every group depth; the renames apply only at the top level, where the
// built-in keys live.
func replaceDefaultAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey:
			if a.Value.Kind() == slog.KindTime {
				return slog.String(logKeyTimestamp, a.Value.Time().UTC().Format(logTimestampLayout))
			}
			return a
		case slog.MessageKey:
			a.Key = logKeyMessage
			return a
		}
	}
	if a.Value.Kind() == slog.KindAny {
		if err, ok := a.Value.Any().(error); ok {
			return errorAttrs(err)
		}
	}
	return a
}

// errorAttrs expands err into errorType and errorMessage, plus stackTrace
// when the error carries recorded frames. The result is a group with an
// empty key, which the JSON handler inlines into the enclosing object.
func errorAttrs(err error) slog.Attr {
	attrs := []any{
		slog.String(logKeyErrorType, errorTypeName(err)),
		slog.String(logKeyErrorMessage, err.Error()),
	}
	if frames := errorStackTrace(err); len(frames) > 0 {
		attrs = append(attrs, slog.Any(logKeyStackTrace, frames))
	}
	return slog.Group("", attrs...)
}

// errorStackTrace returns the frames recorded on err, if the SDK recorded
// any. Only the SDK's own failure types carry frames. Every such type
// exposes them through operationError, so the frames are read there rather
// than from a per-type list that new failure types could fall out of. The
// value itself is inspected, not a wrapped cause: the frames belong to the
// failure that was logged.
func errorStackTrace(err error) []string {
	if oe, ok := unwrapErrorData(err).(interface{ operationError() *OperationError }); ok {
		return oe.operationError().StackTrace
	}
	return nil
}

// logLevelFromEnv maps the AWS_LAMBDA_LOG_LEVEL value to the default
// handler's minimum level. The comparison ignores case. An unset or
// unrecognised value selects INFO.
func logLevelFromEnv(value string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TRACE":
		return slog.LevelDebug - 4
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "FATAL":
		return slog.LevelError + 4
	default:
		return slog.LevelInfo
	}
}

// defaultLogHandler builds the handler used when no [WithLogHandler] option
// is given: JSON to stderr, Lambda's log channel, at the level
// AWS_LAMBDA_LOG_LEVEL selects.
func defaultLogHandler() slog.Handler {
	return newDefaultLogHandler(os.Stderr, logLevelFromEnv(os.Getenv(logLevelEnvVar)))
}

// replayHandler wraps an [slog.Handler] with per-context replay
// suppression. While replaying reports true and emitReplayed reports
// false, it reports every level disabled and drops every record. Because
// Enabled is consulted before a record is built, a suppressed call incurs
// no formatting cost. While replaying reports true and emitReplayed
// reports true, the record is passed to replayInner, which is inner with
// the replay attribute attached, so a replayed record is distinguishable
// from a live one; see [ReplayLogModeEmit].
//
// Suppression is decided per emitting context, not per invocation. Each
// context (root, child, and branch) owns one replayHandler whose replaying
// func reads that context's own mode. A branch that is still replaying
// therefore stays suppressed while a sibling branch that has reached live
// execution logs normally. The wrapper applies to whichever handler the
// user supplied, not only the default. emitReplayed reads the owning
// context's current [ReplayLogMode] on every call, so a change made with
// [ConfigureLogging] reaches a logger obtained before the change.
//
// replayInner is derived alongside inner by WithAttrs and WithGroup, so
// the replay attribute always sits at the level the handler was created
// at, beside the execution and operation attributes, and never under a
// group the user opened later. Enabled asks whichever of the two would
// receive the record, because a handler may answer differently once the
// replay attribute is attached.
//
// The top-level replay key belongs to the SDK. A user attribute under that
// key, attached through [slog.Logger.With] or passed with a record, is
// dropped while no group is open, so the key is single-valued in the
// output and a consumer never reads a user value as the SDK's marker. An
// empty-key group is inlined by conforming handlers, so the rule descends
// into one. Under a group the user opened the same name is a different
// attribute path and is kept.
type replayHandler struct {
	inner        slog.Handler
	replayInner  slog.Handler
	replaying    func() bool
	emitReplayed func() bool
	// grouped reports that WithGroup opened a group on this handler or an
	// ancestor. Attributes then land under the user's group, where the
	// replay key does not collide with the SDK's.
	grouped bool
}

var _ slog.Handler = (*replayHandler)(nil)

// newReplayLogger returns a logger over inner whose records are dropped
// while replaying reports true, unless emitReplayed reports true, in
// which case they are emitted with the replay attribute set.
func newReplayLogger(inner slog.Handler, replaying, emitReplayed func() bool) *slog.Logger {
	return slog.New(&replayHandler{
		inner:        inner,
		replayInner:  inner.WithAttrs([]slog.Attr{slog.Bool(logKeyReplay, true)}),
		replaying:    replaying,
		emitReplayed: emitReplayed,
	})
}

// target returns the handler a record emitted now would reach, or nil
// when the record is suppressed: replayInner while replaying with
// emission on, inner while live.
func (h *replayHandler) target() slog.Handler {
	if h.replaying() {
		if !h.emitReplayed() {
			return nil
		}
		return h.replayInner
	}
	return h.inner
}

func (h *replayHandler) Enabled(ctx context.Context, level slog.Level) bool {
	t := h.target()
	if t == nil {
		return false
	}
	return t.Enabled(ctx, level)
}

func (h *replayHandler) Handle(ctx context.Context, r slog.Record) error {
	t := h.target()
	if t == nil {
		return nil
	}
	if !h.grouped {
		r = withoutReplayAttr(r)
	}
	return t.Handle(ctx, r)
}

func (h *replayHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if !h.grouped {
		attrs, _ = withoutReplayAttrs(attrs)
	}
	if len(attrs) == 0 {
		return h
	}
	return &replayHandler{
		inner:        h.inner.WithAttrs(attrs),
		replayInner:  h.replayInner.WithAttrs(attrs),
		replaying:    h.replaying,
		emitReplayed: h.emitReplayed,
		grouped:      h.grouped,
	}
}

func (h *replayHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &replayHandler{
		inner:        h.inner.WithGroup(name),
		replayInner:  h.replayInner.WithGroup(name),
		replaying:    h.replaying,
		emitReplayed: h.emitReplayed,
		grouped:      true,
	}
}

// withoutReplayAttr returns r without any top-level attribute under the
// replay key, or r itself when it has none. A record's attributes cannot
// be removed in place, so a filtered record is rebuilt with the same
// time, level, message, and source.
func withoutReplayAttr(r slog.Record) slog.Record {
	if r.NumAttrs() == 0 {
		return r
	}
	attrs := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	kept, changed := withoutReplayAttrs(attrs)
	if !changed {
		return r
	}
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	out.AddAttrs(kept...)
	return out
}

// withoutReplayAttrs returns attrs without any attribute under the replay
// key, and whether anything was removed. It descends into empty-key
// groups, which conforming handlers inline at the enclosing level, and
// leaves every other attribute as it is.
func withoutReplayAttrs(attrs []slog.Attr) ([]slog.Attr, bool) {
	// out stays nil until the first removal, so an unchanged slice is
	// returned as is without a copy.
	var out []slog.Attr
	for i, a := range attrs {
		keep, changed := a, false
		if a.Key == logKeyReplay {
			changed = true
		} else if v := a.Value.Resolve(); a.Key == "" && v.Kind() == slog.KindGroup {
			if children, c := withoutReplayAttrs(v.Group()); c {
				keep = slog.Attr{Key: "", Value: slog.GroupValue(children...)}
				changed = true
			}
		}
		if changed && out == nil {
			out = make([]slog.Attr, 0, len(attrs)-1)
			out = append(out, attrs[:i]...)
		}
		if out != nil && keep.Key != logKeyReplay {
			out = append(out, keep)
		}
	}
	if out == nil {
		return attrs, false
	}
	return out, true
}

// reservedLogKeys are the top-level keys a plugin's EnrichLogContext field
// may not set: the record's own built-in keys, the SDK's execution and
// operation identifiers, and the replay marker. A plugin field under one
// of these keys is dropped, so a plugin cannot overwrite the identifiers
// other tooling correlates on. The SDK emits these keys only at the top
// level, so the rule applies only while no group is open; under a group
// the same name is a different attribute path and does not collide.
var reservedLogKeys = map[string]struct{}{
	slog.TimeKey:        {},
	slog.LevelKey:       {},
	slog.MessageKey:     {},
	logKeyTimestamp:     {},
	logKeyMessage:       {},
	logKeyRequestID:     {},
	logKeyExecutionArn:  {},
	logKeyTenantID:      {},
	logKeyOperationID:   {},
	logKeyOperationName: {},
	logKeyAttempt:       {},
	logKeyReplay:        {},
}

// enrichLogHandler wraps an [slog.Handler] with plugin log-context
// enrichment. On every record it calls enrich, which returns the merged
// fields of every plugin's EnrichLogContext hook, and appends them to the
// record as attributes before handing it to inner.
//
// Attributes are compared by qualified path, not by bare key. A record's
// attributes, and so the plugin fields appended to it, land under the
// groups opened through WithGroup; an attribute attached through WithAttrs
// lands under the groups open at that time. Key "k" at the top level and
// key "k" under group "g" are different attributes and do not collide.
// An empty-key group is inlined by conforming handlers, so its children
// count at the enclosing path.
//
// Precedence, highest first: the SDK's identifiers and the record's
// built-in keys (see reservedLogKeys, top level only), then the attributes
// the call site supplied, either with the record or earlier through
// [slog.Logger.With], then plugin fields. A plugin field is added only
// when its path is under neither of the first two. Among plugins, enrich
// has already merged them in registration order with later plugins
// overwriting earlier ones. Fields are appended in key order so the
// output is stable across runs.
//
// The path check covers ancestors as well as the field's own path. When
// an attribute was attached at path g and the logger then opens group g
// through WithGroup, every plugin field would land inside a second object
// under key g. Conforming handlers emit that object beside the attached
// one, so the JSON would carry key g twice and a consumer would keep one
// object at random. The handler therefore appends no plugin field while
// any group on its open path is an attached attribute. The attached
// attribute is emitted unchanged; the plugin fields are dropped for
// records logged through that logger.
//
// The handler is installed only when at least one plugin implements the
// hook, so the hook costs nothing otherwise. It sits inside the replay
// wrapper: a record suppressed during replay never reaches it, and the
// hook is not called for that record. The hook is also not called for a
// record whose fields would all be dropped because an open group is
// taken.
type enrichLogHandler struct {
	inner  slog.Handler
	enrich func(ctx context.Context) map[string]any
	// groups is the path of groups opened through WithGroup on this
	// handler or an ancestor. Record attributes, and the plugin fields
	// appended to a record, land under this path.
	groups []string
	// taken holds the qualified paths, as qualifiedLogKey encodes them, of
	// the attributes attached through WithAttrs on this handler or an
	// ancestor: the SDK's scope attributes and any the user added with
	// [slog.Logger.With]. A plugin field whose path is taken is dropped.
	// The map is shared by derived handlers and never written after
	// construction.
	taken map[string]struct{}
	// groupTaken reports that some prefix of groups is itself a taken
	// path: an attribute was attached at that path before the group was
	// opened. Every plugin field would then land under a taken ancestor,
	// so Handle appends none. It is decided when the group is opened.
	// Attributes attached later land below the open path, never on it, so
	// the decision does not go stale.
	groupTaken bool
}

var _ slog.Handler = (*enrichLogHandler)(nil)

// newEnrichLogHandler returns inner wrapped with plugin enrichment when d
// has a plugin implementing EnrichLogContext, else inner unchanged.
func newEnrichLogHandler(inner slog.Handler, d *pluginDispatcher) slog.Handler {
	if !d.hasLogEnricher() {
		return inner
	}
	return &enrichLogHandler{
		inner:  inner,
		enrich: func(ctx context.Context) map[string]any { return enrichLogContext(ctx, d) },
	}
}

func (h *enrichLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *enrichLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.groupTaken {
		return h.inner.Handle(ctx, r)
	}
	fields := h.enrich(ctx)
	if len(fields) == 0 {
		return h.inner.Handle(ctx, r)
	}
	// The record's own attributes win over plugin fields. Collect their
	// paths before adding anything.
	present := make(map[string]struct{}, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		addLogKeyPaths(present, h.groups, a)
		return true
	})
	keys := make([]string, 0, len(fields))
	for k := range fields {
		if _, reserved := reservedLogKeys[k]; reserved && len(h.groups) == 0 {
			continue
		}
		path := qualifiedLogKey(h.groups, k)
		if _, dup := present[path]; dup {
			continue
		}
		if _, dup := h.taken[path]; dup {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return h.inner.Handle(ctx, r)
	}
	slices.Sort(keys)
	// Handle receives a copy that shares storage with the caller's
	// record, so clone before appending.
	r = r.Clone()
	for _, k := range keys {
		r.AddAttrs(slog.Any(k, fields[k]))
	}
	return h.inner.Handle(ctx, r)
}

func (h *enrichLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	taken := make(map[string]struct{}, len(h.taken)+len(attrs))
	maps.Copy(taken, h.taken)
	for _, a := range attrs {
		addLogKeyPaths(taken, h.groups, a)
	}
	return &enrichLogHandler{
		inner:      h.inner.WithAttrs(attrs),
		enrich:     h.enrich,
		groups:     h.groups,
		taken:      taken,
		groupTaken: h.groupTaken,
	}
}

func (h *enrichLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	_, taken := h.taken[qualifiedLogKey(h.groups, name)]
	// Slice to capacity so appending never writes into a sibling's path.
	groups := append(h.groups[:len(h.groups):len(h.groups)], name)
	return &enrichLogHandler{
		inner:      h.inner.WithGroup(name),
		enrich:     h.enrich,
		groups:     groups,
		taken:      h.taken,
		groupTaken: h.groupTaken || taken,
	}
}

// addLogKeyPaths records in set the qualified path of a, nested under
// groups, and of every attribute inside it, following the handler rules
// of package slog: a value is resolved first, an empty-key group is
// inlined so its children count at the enclosing path, a non-empty group
// takes its own path and nests its children under it, and an empty group
// or a zero attribute is ignored.
func addLogKeyPaths(set map[string]struct{}, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() != slog.KindGroup {
		if a.Equal(slog.Attr{}) {
			return
		}
		set[qualifiedLogKey(groups, a.Key)] = struct{}{}
		return
	}
	children := a.Value.Group()
	if len(children) == 0 {
		return
	}
	if a.Key != "" {
		set[qualifiedLogKey(groups, a.Key)] = struct{}{}
		groups = append(groups[:len(groups):len(groups)], a.Key)
	}
	for _, c := range children {
		addLogKeyPaths(set, groups, c)
	}
}

// qualifiedLogKey encodes the path of key nested under groups as one
// string. Each segment is length-prefixed, so two paths are equal only
// when their segments are: key "a.b" at the top level and key "b" under
// group "a" encode differently.
func qualifiedLogKey(groups []string, key string) string {
	var b strings.Builder
	for _, g := range groups {
		b.WriteString(strconv.Itoa(len(g)))
		b.WriteByte(':')
		b.WriteString(g)
	}
	b.WriteString(strconv.Itoa(len(key)))
	b.WriteByte(':')
	b.WriteString(key)
	return b.String()
}

// executionLogAttrs are the attributes every record of one invocation
// carries: the request ID and execution ARN always, and the tenant ID when
// the invocation has one.
func executionLogAttrs(executionArn string, inv invocationInfo) []slog.Attr {
	attrs := []slog.Attr{
		slog.String(logKeyRequestID, inv.requestID),
		slog.String(logKeyExecutionArn, executionArn),
	}
	if inv.tenantID != "" {
		attrs = append(attrs, slog.String(logKeyTenantID, inv.tenantID))
	}
	return attrs
}

// contextLogAttrs are the attributes a child context adds to its records:
// the child operation's wire ID and its name when it has one. A child
// context has no attempt number; only a step body, condition check, or
// callback submitter carries one.
func contextLogAttrs(id, name string) []slog.Attr {
	attrs := []slog.Attr{slog.String(logKeyOperationID, hashID(id))}
	if name != "" {
		attrs = append(attrs, slog.String(logKeyOperationName, name))
	}
	return attrs
}

// operationLogAttrs are the attributes a step body, condition check, or
// callback submitter carries on its records: the operation's wire ID, its
// name when it has one, and the attempt number.
func operationLogAttrs(id, name string, attempt int) []slog.Attr {
	return append(contextLogAttrs(id, name), slog.Int(logKeyAttempt, attempt))
}

// errorTypeName returns the type name recorded for err in log records. The
// rule, applied to the value itself rather than to a wrapped cause:
//
//  1. An SDK failure that recorded an ErrorType reports that recorded
//     type, read through operationError. A [StepError] whose body returned
//     a ValidationError therefore logs "ValidationError".
//  2. Any other SDK error type reports its wire name from
//     [sdkWireErrorType], so the log agrees with the checkpoint record: a
//     [CombinatorError] logs "PromiseCombinatorError".
//  3. A replayed failure reports the type its record holds.
//  4. Everything else reports its Go type name as [userErrorTypeName]
//     derives it.
func errorTypeName(err error) string {
	err = unwrapErrorData(err)
	if oe, ok := err.(interface{ operationError() *OperationError }); ok {
		if recorded := oe.operationError().ErrorType; recorded != "" {
			return recorded
		}
	}
	if name, ok := sdkWireErrorType(err); ok {
		return name
	}
	if re, ok := err.(*replayedError); ok { //nolint:errorlint // the recorded type belongs to this value, not to a wrapped cause
		return re.errType
	}
	return userErrorTypeName(err)
}

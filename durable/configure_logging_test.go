package durable

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// replayedStepPayload is a checkpoint log with one SUCCEEDED step, so the
// handler's first step replays and the root context is in replay until a
// later operation with no checkpoint is claimed.
func replayedStepPayload() []byte {
	return childPayload(`"x"`,
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"done"`}),
	)
}

// replayScenarioPayload is the checkpoint log runReplayThenLive replays: a
// STARTED child context with one SUCCEEDED step inside it, and a STARTED
// step at the root. A STARTED child re-enters its body in replay mode
// until it claims an operation with no checkpoint; a STARTED step with no
// outcome re-executes its body while the root is still replaying.
func replayScenarioPayload() []byte {
	return childPayload(`"x"`,
		checkpointedChild("1", "STARTED", nil),
		checkpointedStep("1-1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"done"`}),
		checkpointedStep("2", "STARTED", nil),
	)
}

// runReplayThenLive runs a handler over replayScenarioPayload that logs
// from every scope both while replaying and live, in this order:
// "root-replaying" from the handler body, "child-replaying" from the
// re-entered child context, "child-live" after the child's first live
// step, "step-replaying" from the re-executed STARTED step body, and
// "root-live" after the root's first live step. configure, when non-nil,
// runs first with the root context. The handler is built with opts plus
// the fake Lambda API and a recording handler, which is returned.
func runReplayThenLive(t *testing.T, configure func(ctx Context) error, opts ...HandlerOption) *recordingHandler {
	t.Helper()
	fake := &fakeLambda{}
	rec := newRecordingHandler()
	opts = append([]HandlerOption{withLambdaAPI(fake), WithLogHandler(rec)}, opts...)
	h := Wrap(func(ctx Context, _ string) (string, error) {
		if configure != nil {
			if err := configure(ctx); err != nil {
				return "", err
			}
		}
		ctx.Logger().Info("root-replaying")
		if _, err := RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			c.Logger().Info("child-replaying")
			if _, err := Step(c, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
				return "", err
			}
			if _, err := Step(c, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("child-live")
			return "c", nil
		}); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "u", func(sc StepContext) (string, error) {
			sc.Logger().Info("step-replaying")
			return "u", nil
		}); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "v", func(StepContext) (string, error) { return "live", nil }); err != nil {
			return "", err
		}
		ctx.Logger().Info("root-live")
		return "ok", nil
	}, opts...)
	resp, err := h(lambdaCtx(t, "req-r", "tenant-r"), replayScenarioPayload())
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !strings.Contains(string(resp), `"SUCCEEDED"`) {
		t.Fatalf("response = %s, want SUCCEEDED", resp)
	}
	return rec
}

func TestReplayLogsSuppressedByDefault(t *testing.T) {
	// Without any option, records emitted while replaying are dropped in
	// every scope and only the live records reach the handler. No record
	// carries the replay attribute.
	rec := runReplayThenLive(t, nil)
	if got := rec.messages(); len(got) != 2 || got[0] != "child-live" || got[1] != "root-live" {
		t.Fatalf("messages = %v, want only [child-live root-live]", got)
	}
	for _, r := range rec.all() {
		if _, ok := r.attrs[logKeyReplay]; ok {
			t.Errorf("live record %q carries %s, want no replay attribute", r.message, logKeyReplay)
		}
	}
}

func TestWithReplayLogModeEmitEmitsReplayedRecords(t *testing.T) {
	// With ReplayLogModeEmit at construction, the replayed records are
	// emitted and carry replay=true; the live records carry no replay
	// attribute. Every record keeps its durable context fields.
	rec := runReplayThenLive(t, nil, WithReplayLogMode(ReplayLogModeEmit))
	assertReplayThenLiveEmitted(t, rec)
}

func TestConfigureLoggingReplayLogModeEmitEmitsReplayedRecords(t *testing.T) {
	// The same result through ConfigureLogging inside the handler body,
	// with the handler left unchanged. The mode reaches the child context
	// and step loggers derived after the call and the root logger alike.
	rec := runReplayThenLive(t, func(ctx Context) error {
		return ConfigureLogging(ctx, LogConfig{ReplayLogMode: ReplayLogModeEmit})
	})
	assertReplayThenLiveEmitted(t, rec)
}

// assertReplayThenLiveEmitted checks the records of runReplayThenLive under
// ReplayLogModeEmit: all five messages in order, replay=true on the three
// replayed ones only, the durable context fields on every record, and each
// scope's own operation attributes.
func assertReplayThenLiveEmitted(t *testing.T, rec *recordingHandler) {
	t.Helper()
	want := []string{"root-replaying", "child-replaying", "child-live", "step-replaying", "root-live"}
	replayed := map[string]bool{"root-replaying": true, "child-replaying": true, "step-replaying": true}
	if got := rec.messages(); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("messages = %v, want %v", got, want)
	}
	for _, r := range rec.all() {
		v, ok := r.attrs[logKeyReplay]
		if replayed[r.message] && v != true {
			t.Errorf("replayed record %q attrs = %v, want %s=true", r.message, r.attrs, logKeyReplay)
		}
		if !replayed[r.message] && ok {
			t.Errorf("live record %q attrs = %v, want no %s attribute", r.message, r.attrs, logKeyReplay)
		}
		if r.attrs[logKeyRequestID] != "req-r" || r.attrs[logKeyExecutionArn] != "arn:test" || r.attrs[logKeyTenantID] != "tenant-r" {
			t.Errorf("record %q lost the durable context fields: %v", r.message, r.attrs)
		}
		switch r.message {
		case "child-replaying", "child-live":
			if r.attrs[logKeyOperationID] != hashID("1") || r.attrs[logKeyOperationName] != "outer" {
				t.Errorf("child record %q attrs = %v, want the child's own scope", r.message, r.attrs)
			}
		case "step-replaying":
			if r.attrs[logKeyOperationID] != hashID("2") || r.attrs[logKeyOperationName] != "u" {
				t.Errorf("step record attrs = %v, want the step's own scope", r.attrs)
			}
		default:
			if _, ok := r.attrs[logKeyOperationID]; ok {
				t.Errorf("root record %q attrs = %v, want no operation scope", r.message, r.attrs)
			}
		}
	}
}

func TestConfigureLoggingReplayLogModeSuppressRestoresDefault(t *testing.T) {
	// ReplayLogModeSuppress from ConfigureLogging overrides an Emit set at
	// construction, and reaches a logger obtained before the call, because
	// the mode is read on every record.
	fake := &fakeLambda{}
	rec := newRecordingHandler()
	h := Wrap(func(ctx Context, _ string) (string, error) {
		early := ctx.Logger()
		early.Info("emitted-before")
		if err := ConfigureLogging(ctx, LogConfig{ReplayLogMode: ReplayLogModeSuppress}); err != nil {
			return "", err
		}
		early.Info("hidden-after")
		ctx.Logger().Info("hidden-after-too")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec), WithReplayLogMode(ReplayLogModeEmit))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if got := rec.messages(); len(got) != 1 || got[0] != "emitted-before" {
		t.Fatalf("messages = %v, want only [emitted-before]", got)
	}
}

func TestReplayLogModeEmitReplayAttributeStaysTopLevelUnderGroup(t *testing.T) {
	// Through the default JSON handler, the replay attribute is a top-level
	// key beside the SDK's identifiers, even when the logger has opened a
	// group: it belongs to the record's scope, not to the user's group.
	var out lockedBuffer
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().WithGroup("g").Info("replaying", "k", "v")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(newDefaultLogHandler(&out, slog.LevelInfo)), WithReplayLogMode(ReplayLogModeEmit))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	records := parseLogLines(t, out.String())
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1:\n%s", len(records), out.String())
	}
	rec := records[0]
	if rec[logKeyReplay] != true {
		t.Errorf("top-level %s = %v, want true: %s", logKeyReplay, rec[logKeyReplay], out.String())
	}
	g, _ := rec["g"].(map[string]any)
	if g["k"] != "v" {
		t.Errorf("g.k = %v, want v: %s", g["k"], out.String())
	}
	if _, ok := g[logKeyReplay]; ok {
		t.Errorf("%s must not land under the user's group: %s", logKeyReplay, out.String())
	}
	if n := countKey(out.String(), logKeyReplay); n != 1 {
		t.Errorf("key %s appears %d times, want once: %s", logKeyReplay, n, out.String())
	}
}

func TestPluginLogContextCannotOverwriteReplayAttribute(t *testing.T) {
	// A plugin field named replay is dropped at the top level on both a
	// replayed record, where the SDK sets it, and a live record, where it
	// is absent, so a consumer never mistakes a live record for a replayed
	// one.
	rec := newRecordingHandler()
	plugin := Plugin{EnrichLogContext: func(context.Context) map[string]any {
		return map[string]any{logKeyReplay: "plugin", "extra": "kept"}
	}}
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().Info("replaying")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
			return "", err
		}
		ctx.Logger().Info("live")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec), WithPlugins(plugin), WithReplayLogMode(ReplayLogModeEmit))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	records := rec.all()
	if len(records) != 2 {
		t.Fatalf("got %d records %v, want 2", len(records), rec.messages())
	}
	if records[0].attrs[logKeyReplay] != true || records[0].attrs["extra"] != "kept" {
		t.Errorf("replayed record attrs = %v, want replay=true and the plugin's other field", records[0].attrs)
	}
	if _, ok := records[1].attrs[logKeyReplay]; ok || records[1].attrs["extra"] != "kept" {
		t.Errorf("live record attrs = %v, want no replay attribute and the plugin's other field", records[1].attrs)
	}
}

// attrDependentHandler is a user-style [slog.Handler] whose Enabled answer
// depends on the attributes attached through WithAttrs: it reports every
// level enabled per the enabled field, and a WithAttrs call that attaches
// replay=true replaces that answer with enabledOnReplay. It hands the
// records it receives to rec.
type attrDependentHandler struct {
	enabled         bool
	enabledOnReplay bool
	rec             slog.Handler
}

func (h *attrDependentHandler) Enabled(context.Context, slog.Level) bool { return h.enabled }

func (h *attrDependentHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.rec.Handle(ctx, r)
}

func (h *attrDependentHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := &attrDependentHandler{enabled: h.enabled, enabledOnReplay: h.enabledOnReplay, rec: h.rec.WithAttrs(attrs)}
	for _, a := range attrs {
		if a.Key == logKeyReplay && a.Value.Kind() == slog.KindBool && a.Value.Bool() {
			out.enabled = h.enabledOnReplay
		}
	}
	return out
}

func (h *attrDependentHandler) WithGroup(string) slog.Handler { return h }

func TestReplayHandlerEnabledFollowsHandlerThatReceivesRecord(t *testing.T) {
	// A handler may answer Enabled differently once the replay attribute
	// is attached. The wrapper asks the handler the record will reach: the
	// one with replay=true during replay in emit mode, the plain one while
	// live. Enabled and Handle therefore agree in both states, so a record
	// the effective handler enables is never dropped and one it disables
	// is never built.
	cases := []struct {
		name                     string
		enabled, enabledOnReplay bool
		wantMessages             []string
	}{
		{"enabled only with replay attached", false, true, []string{"replayed"}},
		{"disabled once replay attached", true, false, []string{"live"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecordingHandler()
			replaying := true
			h := &attrDependentHandler{enabled: tc.enabled, enabledOnReplay: tc.enabledOnReplay, rec: rec}
			logger := newReplayLogger(h, func() bool { return replaying }, func() bool { return true })
			if got := logger.Enabled(t.Context(), slog.LevelInfo); got != tc.enabledOnReplay {
				t.Errorf("Enabled during replay = %v, want the replay handler's %v", got, tc.enabledOnReplay)
			}
			logger.Info("replayed")
			replaying = false
			if got := logger.Enabled(t.Context(), slog.LevelInfo); got != tc.enabled {
				t.Errorf("Enabled while live = %v, want the live handler's %v", got, tc.enabled)
			}
			logger.Info("live")
			if got := rec.messages(); strings.Join(got, " ") != strings.Join(tc.wantMessages, " ") {
				t.Fatalf("messages = %v, want %v", got, tc.wantMessages)
			}
		})
	}
}

func TestUserReplayAttributeIsDroppedAtTopLevel(t *testing.T) {
	// The top-level replay key belongs to the SDK. A user value under it,
	// attached with Logger.With or passed with the record, is dropped on
	// replayed and live records alike, so the default JSON handler emits
	// the key at most once and a live record never carries replay=false as
	// a fake marker. The same name under a group the user opened is kept.
	// Other user attributes beside the dropped one are unaffected.
	var out lockedBuffer
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().With(logKeyReplay, false).Info("replaying-with", "k", "v")
		ctx.Logger().Info("replaying-record", logKeyReplay, false, "k", "v")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
			return "", err
		}
		ctx.Logger().With(logKeyReplay, true).Info("live-with", "k", "v")
		ctx.Logger().Info("live-record", logKeyReplay, true, "k", "v")
		ctx.Logger().Info("live-inlined", slog.Group("", slog.Bool(logKeyReplay, true), slog.String("k", "v")))
		ctx.Logger().WithGroup("g").Info("live-grouped", logKeyReplay, "user", "k", "v")
		ctx.Logger().WithGroup("g").With(logKeyReplay, "user").Info("live-grouped-with", "k", "v")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(newDefaultLogHandler(&out, slog.LevelInfo)), WithReplayLogMode(ReplayLogModeEmit))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	records := parseLogLines(t, out.String())
	if len(records) != 7 {
		t.Fatalf("got %d records, want 7:\n%s", len(records), out.String())
	}
	for i, rec := range records {
		msg, _ := rec[logKeyMessage].(string)
		grouped := strings.HasPrefix(msg, "live-grouped")
		replayed := strings.HasPrefix(msg, "replaying")
		if rec["k"] != "v" && !grouped {
			t.Errorf("%q: k = %v, want v: %s", msg, rec["k"], lines[i])
		}
		switch {
		case replayed:
			if rec[logKeyReplay] != true || countKey(lines[i], logKeyReplay) != 1 {
				t.Errorf("%q: want %s=true exactly once: %s", msg, logKeyReplay, lines[i])
			}
		case grouped:
			if _, ok := rec[logKeyReplay]; ok {
				t.Errorf("%q: top-level %s present, want none: %s", msg, logKeyReplay, lines[i])
			}
			g, _ := rec["g"].(map[string]any)
			if g[logKeyReplay] != "user" || g["k"] != "v" {
				t.Errorf("%q: g = %v, want the user's replay and k under the group: %s", msg, g, lines[i])
			}
		default:
			if _, ok := rec[logKeyReplay]; ok || countKey(lines[i], logKeyReplay) != 0 {
				t.Errorf("%q: want no %s key on a live record: %s", msg, logKeyReplay, lines[i])
			}
		}
	}
}

func TestUserReplayAttributeIsDroppedInSuppressMode(t *testing.T) {
	// The key is reserved in every mode, not only under ReplayLogModeEmit:
	// a live record under the default mode also loses a user replay value,
	// so a consumer cannot be shown a marker the SDK never set.
	rec := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().With(logKeyReplay, true).Info("with", "k", "v")
		ctx.Logger().Info("record", logKeyReplay, true, "k", "v")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec))
	if _, err := h(t.Context(), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	records := rec.all()
	if len(records) != 2 {
		t.Fatalf("got %d records %v, want 2", len(records), rec.messages())
	}
	for _, r := range records {
		if _, ok := r.attrs[logKeyReplay]; ok || r.attrs["k"] != "v" {
			t.Errorf("record %q attrs = %v, want k=v and no %s", r.message, r.attrs, logKeyReplay)
		}
	}
}

func TestWithoutReplayAttrsLeavesUnchangedSliceAlone(t *testing.T) {
	attrs := []slog.Attr{slog.String("a", "1"), slog.Group("", slog.String("b", "2")), slog.Group("g", slog.Bool(logKeyReplay, true))}
	got, changed := withoutReplayAttrs(attrs)
	if changed || len(got) != 3 {
		t.Fatalf("withoutReplayAttrs(no top-level replay) = %v, %v; want the input unchanged", got, changed)
	}
	got, changed = withoutReplayAttrs([]slog.Attr{slog.Bool(logKeyReplay, false), slog.String("a", "1"), slog.Group("", slog.Bool(logKeyReplay, true), slog.String("b", "2"))})
	if !changed || len(got) != 2 || got[0].Key != "a" || got[1].Key != "" {
		t.Fatalf("withoutReplayAttrs = %v, %v; want [a, inlined group] and changed", got, changed)
	}
	if inner := got[1].Value.Group(); len(inner) != 1 || inner[0].Key != "b" {
		t.Fatalf("inlined group = %v, want only b", inner)
	}
}

func TestConfigureLoggingHandlerReceivesLaterRecordsWithContextFields(t *testing.T) {
	// Records emitted after ConfigureLogging replaces the handler reach the
	// new handler, not the construction-time one, and carry the durable
	// context fields: the execution attributes on every record, and the
	// operation attributes inside a step and a child context.
	old := newRecordingHandler()
	replacement := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().Info("before")
		if err := ConfigureLogging(ctx, LogConfig{Handler: replacement}); err != nil {
			return "", err
		}
		ctx.Logger().Info("after")
		if _, err := Step(ctx, "greet", func(sc StepContext) (string, error) {
			sc.Logger().Info("step", "custom", "value")
			return "s", nil
		}); err != nil {
			return "", err
		}
		return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			c.Logger().Info("child")
			return "c", nil
		})
	}, withLambdaAPI(fake), WithLogHandler(old))
	if _, err := h(lambdaCtx(t, "req-c", "tenant-c"), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	if got := old.messages(); len(got) != 1 || got[0] != "before" {
		t.Errorf("construction-time handler messages = %v, want only [before]", got)
	}
	if got := replacement.messages(); len(got) != 3 || got[0] != "after" || got[1] != "step" || got[2] != "child" {
		t.Fatalf("new handler messages = %v, want [after step child]", got)
	}
	for _, r := range replacement.all() {
		if r.attrs[logKeyRequestID] != "req-c" || r.attrs[logKeyExecutionArn] != "arn:test" || r.attrs[logKeyTenantID] != "tenant-c" {
			t.Errorf("record %q lost the execution fields: %v", r.message, r.attrs)
		}
	}
	records := replacement.all()
	step := records[1].attrs
	if step[logKeyOperationID] != hashID("1") || step[logKeyOperationName] != "greet" || step[logKeyAttempt] != int64(1) || step["custom"] != "value" {
		t.Errorf("step record attrs = %v, want the step's scope and the call site's attribute", step)
	}
	child := records[2].attrs
	if child[logKeyOperationID] != hashID("2") || child[logKeyOperationName] != "outer" {
		t.Errorf("child record attrs = %v, want the child's scope", child)
	}
	if _, ok := child[logKeyAttempt]; ok {
		t.Errorf("child record attrs = %v, want no attempt", child)
	}
}

func TestConfigureLoggingHandlerKeepsPluginFieldsAndReplaySuppression(t *testing.T) {
	// The replacement handler is wrapped like the original: plugin fields
	// still reach its records, and replay suppression still applies to it
	// while the mode is left unchanged.
	replacement := newRecordingHandler()
	plugin := Plugin{EnrichLogContext: func(context.Context) map[string]any {
		return map[string]any{"service": "orders"}
	}}
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		if err := ConfigureLogging(ctx, LogConfig{Handler: replacement}); err != nil {
			return "", err
		}
		ctx.Logger().Info("replaying")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
			return "", err
		}
		ctx.Logger().Info("live")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(slog.DiscardHandler), WithPlugins(plugin))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	records := replacement.all()
	if len(records) != 1 || records[0].message != "live" {
		t.Fatalf("messages = %v, want only [live]: suppression must wrap the new handler", replacement.messages())
	}
	if records[0].attrs["service"] != "orders" {
		t.Errorf("attrs = %v, want the plugin field service=orders on the new handler's record", records[0].attrs)
	}
}

func TestConfigureLoggingZeroConfigKeepsCurrent(t *testing.T) {
	// A zero LogConfig changes nothing: the handler and the mode stay.
	rec := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		if err := ConfigureLogging(ctx, LogConfig{}); err != nil {
			return "", err
		}
		ctx.Logger().Info("replaying")
		if _, err := Step(ctx, "s", func(StepContext) (string, error) { return "done", nil }); err != nil {
			return "", err
		}
		if _, err := Step(ctx, "t", func(StepContext) (string, error) { return "live", nil }); err != nil {
			return "", err
		}
		ctx.Logger().Info("live")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec), WithReplayLogMode(ReplayLogModeEmit))
	if _, err := h(t.Context(), replayedStepPayload()); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if got := rec.messages(); len(got) != 2 || got[0] != "replaying" || got[1] != "live" {
		t.Errorf("messages = %v, want [replaying live]: the handler and Emit mode must be kept", got)
	}
}

func TestConfigureLoggingPropagatesToContextsDerivedAfter(t *testing.T) {
	// A child or branch derived after the call inherits the new handler and
	// mode; one derived before keeps the settings it was derived with.
	original := newRecordingHandler()
	replacement := newRecordingHandler()
	ec := newTestContext(t, []*operation{execOp()})
	ec.logCfg.Store(&logDefaults{handler: slog.Handler(original)})
	ec.attachLogger()
	before := ec.child("1", "before", ec.owner, modeExecution)

	if err := ConfigureLogging(ec, LogConfig{Handler: replacement, ReplayLogMode: ReplayLogModeEmit}); err != nil {
		t.Fatal(err)
	}
	after := ec.child("2", "after", ec.owner, modeExecution)
	branch := ec.branch(ec.owner)

	before.Logger().Info("before")
	after.Logger().Info("after")
	branch.Logger().Info("branch")
	if got := original.messages(); len(got) != 1 || got[0] != "before" {
		t.Errorf("original handler messages = %v, want only [before]", got)
	}
	if got := replacement.messages(); len(got) != 2 || got[0] != "after" || got[1] != "branch" {
		t.Errorf("replacement handler messages = %v, want [after branch]", got)
	}
	if before.logDefaults().emitReplayed {
		t.Error("child derived before the call inherited the new replay log mode")
	}
	for name, c := range map[string]*execContext{"child": after, "branch": branch} {
		if !c.logDefaults().emitReplayed {
			t.Errorf("%s derived after the call: emitReplayed = false, want the configured Emit mode", name)
		}
	}
}

func TestConfigureLoggingAfterGoKeepsBranchSettings(t *testing.T) {
	// A Go branch started before ConfigureLogging keeps the original
	// handler even when the owner reconfigures while the branch goroutine
	// is still starting: the branch is derived from a snapshot taken before
	// the go statement.
	original := newRecordingHandler()
	replacement := newRecordingHandler()
	fake := &fakeLambda{}
	release := make(chan struct{})
	h := Wrap(func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "b", func(c Context) (string, error) {
			<-release
			c.Logger().Info("branch")
			return "b", nil
		})
		if err := ConfigureLogging(ctx, LogConfig{Handler: replacement}); err != nil {
			return "", err
		}
		close(release)
		ctx.Logger().Info("root")
		return fut.Result()
	}, withLambdaAPI(fake), WithLogHandler(original))
	if _, err := h(t.Context(), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if got := original.messages(); len(got) != 1 || got[0] != "branch" {
		t.Errorf("original handler messages = %v, want only [branch]", got)
	}
	if got := replacement.messages(); len(got) != 1 || got[0] != "root" {
		t.Errorf("replacement handler messages = %v, want only [root]", got)
	}
}

func TestConfigureLoggingDoesNotChangeCheckpoints(t *testing.T) {
	// The same handler run with and without a ConfigureLogging call, and
	// with the call on one invocation only, checkpoints the same operation
	// IDs and payloads: reconfiguration claims no operation and writes no
	// checkpoint.
	run := func(t *testing.T, configure bool) map[string]string {
		t.Helper()
		fake := &fakeLambda{}
		h := Wrap(func(ctx Context, _ string) (string, error) {
			if configure {
				if err := ConfigureLogging(ctx, LogConfig{Handler: slog.DiscardHandler, ReplayLogMode: ReplayLogModeEmit}); err != nil {
					return "", err
				}
			}
			ctx.Logger().Info("root")
			if _, err := Step(ctx, "a", func(StepContext) (string, error) { return "A", nil }); err != nil {
				return "", err
			}
			return Step(ctx, "b", func(StepContext) (string, error) { return "B", nil })
		}, withLambdaAPI(fake), WithLogHandler(slog.DiscardHandler))
		if _, err := h(t.Context(), childPayload(`"x"`)); err != nil {
			t.Fatalf("Invoke() error: %v", err)
		}
		return stepPayloadsByID(t, fake)
	}
	plain := run(t, false)
	configured := run(t, true)
	if len(plain) != 2 || len(configured) != 2 {
		t.Fatalf("step payloads: plain = %v, configured = %v; want two steps each", plain, configured)
	}
	for id, payload := range plain {
		if configured[id] != payload {
			t.Errorf("step %s payload = %q with ConfigureLogging, %q without", id, configured[id], payload)
		}
	}
}

func TestConfigureLoggingWrongGoroutine(t *testing.T) {
	// ConfigureLogging changes the settings only from the owning goroutine;
	// a call from another goroutine is rejected and leaves them unchanged.
	ec := newTestContext(t, []*operation{execOp()})
	want := ec.logDefaults()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ConfigureLogging(ec, LogConfig{Handler: newRecordingHandler(), ReplayLogMode: ReplayLogModeEmit})
	}()
	err := <-errCh
	if !errors.Is(err, ErrWrongGoroutine) {
		t.Fatalf("ConfigureLogging from another goroutine error = %v, want ErrWrongGoroutine", err)
	}
	if !strings.Contains(err.Error(), "ConfigureLogging") {
		t.Errorf("error = %q, want it to name ConfigureLogging", err)
	}
	if got := ec.logDefaults(); got != want {
		t.Errorf("log defaults changed after a rejected call: %+v, want %+v", got, want)
	}
}

func TestConfigureLoggingForeignContext(t *testing.T) {
	err := ConfigureLogging(nil, LogConfig{Handler: slog.DiscardHandler})
	if err == nil || !strings.Contains(err.Error(), "Context was not created by the SDK") {
		t.Errorf("ConfigureLogging(nil) error = %v, want a foreign context error", err)
	}
}

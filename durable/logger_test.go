package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
)

// lockedBuffer is a goroutine-safe bytes.Buffer for capturing log output
// emitted from concurrent branches.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// recordedLog is one record a recordingHandler received, with the
// attributes attached through WithAttrs and the record's own attributes
// merged into one map.
type recordedLog struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

// recordingHandler is a user-style [slog.Handler] that keeps every record
// it receives. It is what a test uses to see exactly which structured
// attributes reach a supplied handler.
type recordingHandler struct {
	mu      *sync.Mutex
	records *[]recordedLog
	attrs   []slog.Attr
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{mu: &sync.Mutex{}, records: &[]recordedLog{}}
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, recordedLog{level: r.Level, message: r.Message, attrs: attrs})
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &recordingHandler{mu: h.mu, records: h.records, attrs: merged}
}

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingHandler) all() []recordedLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]recordedLog(nil), (*h.records)...)
}

func (h *recordingHandler) messages() []string {
	var out []string
	for _, r := range h.all() {
		out = append(out, r.message)
	}
	return out
}

// parseLogLines decodes each JSON line the default handler wrote.
func parseLogLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		records = append(records, rec)
	}
	return records
}

// lambdaCtx returns a context carrying Lambda invocation metadata, as the
// runtime supplies it.
func lambdaCtx(t *testing.T, requestID, tenantID string) context.Context {
	t.Helper()
	return lambdacontext.NewContext(t.Context(), &lambdacontext.LambdaContext{
		AwsRequestID: requestID,
		TenantID:     tenantID,
	})
}

// isoMillisUTC matches an ISO 8601 UTC timestamp with millisecond
// precision and a Z suffix, e.g. 2026-09-17T04:56:45.657Z.
var isoMillisUTC = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func TestReplayHandlerSuppressesWhileReplaying(t *testing.T) {
	rec := newRecordingHandler()
	replaying := false
	logger := newReplayLogger(rec, func() bool { return replaying })

	// Not replaying: emit.
	logger.Info("visible")
	if got := rec.messages(); len(got) != 1 || got[0] != "visible" {
		t.Fatalf("messages = %v, want [visible]", got)
	}

	// Replaying: suppress every level, and report every level disabled so
	// no record is even built.
	replaying = true
	logger.Info("hidden")
	logger.Debug("hidden")
	logger.Warn("hidden")
	logger.Error("hidden")
	if logger.Enabled(t.Context(), slog.LevelError) {
		t.Error("Enabled(ERROR) = true during replay, want false")
	}
	if got := rec.messages(); len(got) != 1 {
		t.Fatalf("messages during replay = %v, want no new records", got)
	}

	// Replay ended: emit again, including through With and WithGroup.
	replaying = false
	logger.With("k", "v").WithGroup("g").Warn("back")
	if got := rec.messages(); len(got) != 2 || got[1] != "back" {
		t.Fatalf("messages = %v, want [visible back]", got)
	}
}

func TestExecContextLoggerReadsOwnReplayState(t *testing.T) {
	// The logger a context returns consults that context's own mode. No
	// shared flag is toggled: the context's replay state is the source of
	// truth on every call.
	rec := newRecordingHandler()
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})
	ec := newExecContext(t.Context(), "arn:test", invocationInfo{}, rec, state)
	if !ec.IsReplaying() {
		t.Fatal("expected replay mode")
	}

	ec.Logger().Info("should-be-hidden")
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("expected no output during replay, got: %v", got)
	}

	// Claim the replayed operation, then refresh for the next (absent)
	// one: the context flips to live execution.
	ec.owner = currentGoroutineOwner()
	_, _ = ec.claimOperation()
	ec.refreshReplayMode()
	if ec.IsReplaying() {
		t.Fatal("expected execution mode after flip")
	}

	ec.Logger().Info("should-be-visible")
	if got := rec.messages(); len(got) != 1 || got[0] != "should-be-visible" {
		t.Fatalf("messages = %v, want [should-be-visible]", got)
	}
}

func TestStepContextLoggerFollowsBranchReplayState(t *testing.T) {
	// A step context derives its logger from the context that ran the
	// step, so a step body logging while its branch replays is suppressed
	// and one logging while live is emitted.
	rec := newRecordingHandler()
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})
	ec := newExecContext(t.Context(), "arn:test", invocationInfo{}, rec, state)

	sc := &stepContext{Context: ec.Context, logger: ec.operationLogger("1", "s", 1), attempt: 1}
	sc.Logger().Info("hidden")
	if got := rec.messages(); len(got) != 0 {
		t.Fatalf("expected no output during replay, got: %v", got)
	}

	ec.mode.Store(int32(modeExecution))
	sc.Logger().Info("visible")
	if got := rec.messages(); len(got) != 1 || got[0] != "visible" {
		t.Fatalf("messages = %v, want [visible]", got)
	}
}

func TestChildLoggerIsolatedFromSiblingReplayTransition(t *testing.T) {
	// Two child contexts of one replaying root. The first reaches live
	// execution; the second is still replaying. Only the first may log.
	rec := newRecordingHandler()
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusStarted},
		{id: hashID("1-1"), status: statusSucceeded},
		{id: hashID("2"), status: statusStarted},
		{id: hashID("2-1"), status: statusSucceeded},
	})
	root := newExecContext(t.Context(), "arn:test", invocationInfo{}, rec, state)
	owner := currentGoroutineOwner()
	a := root.child("1", "", owner, modeReplay)
	b := root.child("2", "", owner, modeReplay)

	// a flips to live execution; b and the root stay in replay.
	a.mode.Store(int32(modeExecution))

	a.Logger().Info("a-live")
	b.Logger().Info("b-replaying")
	root.Logger().Info("root-replaying")

	if got := rec.messages(); len(got) != 1 || got[0] != "a-live" {
		t.Errorf("messages = %v, want only the live branch line [a-live]", got)
	}
}

func TestGoBranchReplaySuppressionIsPerBranch(t *testing.T) {
	// Two Go branches replay from a checkpoint log. Branch "a" has one
	// checkpointed step and reaches live execution on its second step.
	// Branch "b" has two checkpointed steps and waits until "a" is live
	// before logging: at that point "b" is still replaying, so its line
	// must be suppressed. Once "b" runs its own live step, its line is
	// emitted. The handler is user-supplied, so this also shows that
	// suppression wraps a supplied handler.
	fake := &fakeLambda{}
	rec := newRecordingHandler()
	payload := childPayload(`"x"`,
		checkpointedChild("1", "STARTED", nil),
		checkpointedStep("1-1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"a1"`}),
		checkpointedChild("2", "STARTED", nil),
		checkpointedStep("2-1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"b1"`}),
		checkpointedStep("2-2", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"b2"`}),
	)
	aLive := make(chan struct{})

	h := Wrap(func(ctx Context, _ string) (string, error) {
		a := Go(ctx, "a", func(c Context) (string, error) {
			if _, err := Step(c, "a1", func(StepContext) (string, error) { return "a1", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("a-replaying")
			if _, err := Step(c, "a2", func(StepContext) (string, error) { return "a2", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("a-live")
			close(aLive)
			return "a", nil
		})
		b := Go(ctx, "b", func(c Context) (string, error) {
			if _, err := Step(c, "b1", func(StepContext) (string, error) { return "b1", nil }); err != nil {
				return "", err
			}
			if _, err := Step(c, "b2", func(StepContext) (string, error) { return "b2", nil }); err != nil {
				return "", err
			}
			<-aLive
			if !c.IsReplaying() {
				t.Error("branch b must still be replaying after its checkpointed steps")
			}
			c.Logger().Info("b-still-replaying")
			if _, err := Step(c, "b3", func(StepContext) (string, error) { return "b3", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("b-live")
			return "b", nil
		})
		if _, err := a.Result(); err != nil {
			return "", err
		}
		if _, err := b.Result(); err != nil {
			return "", err
		}
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec))

	resp, err := h(t.Context(), payload)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !strings.Contains(string(resp), `"SUCCEEDED"`) {
		t.Fatalf("response = %s, want SUCCEEDED", resp)
	}

	got := strings.Join(rec.messages(), "\n")
	for _, hidden := range []string{"a-replaying", "b-still-replaying"} {
		if strings.Contains(got, hidden) {
			t.Errorf("replaying branch line %q must be suppressed, got:\n%s", hidden, got)
		}
	}
	for _, shown := range []string{"a-live", "b-live"} {
		if !strings.Contains(got, shown) {
			t.Errorf("live branch line %q must be emitted, got:\n%s", shown, got)
		}
	}
}

// runStepLoggingHandler runs a fresh execution whose single step logs one
// INFO record through the step context, with the given handler installed.
// The step is named "greet" and the invocation carries a request ID and a
// tenant ID, as a Lambda invocation would.
func runStepLoggingHandler(t *testing.T, handler slog.Handler, logAttrs ...any) {
	t.Helper()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, name string) (string, error) {
		return Step(ctx, "greet", func(sc StepContext) (string, error) {
			sc.Logger().Info("Greeting step started for: "+name, logAttrs...)
			return "Hello, " + name + "!", nil
		})
	}, withLambdaAPI(fake), WithLogHandler(handler))

	ctx := lambdaCtx(t, "req-1", "tenant-a")
	resp, err := h(ctx, childPayload(`"World"`))
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !strings.Contains(string(resp), `"SUCCEEDED"`) {
		t.Fatalf("response = %s, want SUCCEEDED", resp)
	}
}

func TestDefaultHandlerStepRecordMatchesReferenceShape(t *testing.T) {
	// One record emitted from inside a step is compared with the fixture
	// shaped after the reference implementation's default logger: the same
	// field names, level casing, and timestamp format. Key order is not
	// compared, and values that vary per run are checked by shape.
	fixtureBytes, err := os.ReadFile(filepath.Join("testdata", "log_record_step.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatalf("fixture is not JSON: %v", err)
	}

	var out lockedBuffer
	runStepLoggingHandler(t, newDefaultLogHandler(&out, slog.LevelInfo))
	records := parseLogLines(t, out.String())
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1:\n%s", len(records), out.String())
	}
	rec := records[0]

	for key := range fixture {
		if _, ok := rec[key]; !ok {
			t.Errorf("record is missing field %q present in the fixture", key)
		}
	}
	for key := range rec {
		if _, ok := fixture[key]; !ok {
			t.Errorf("record has field %q absent from the fixture", key)
		}
	}
	for _, absent := range []string{slog.MessageKey, slog.TimeKey} {
		if _, ok := rec[absent]; ok {
			t.Errorf("record must not carry slog's built-in %q key", absent)
		}
	}

	if got := rec["level"]; got != fixture["level"] {
		t.Errorf("level = %v, want %v", got, fixture["level"])
	}
	if got := rec["message"]; got != fixture["message"] {
		t.Errorf("message = %v, want %v", got, fixture["message"])
	}
	if got, _ := rec["timestamp"].(string); !isoMillisUTC.MatchString(got) {
		t.Errorf("timestamp = %q, want ISO 8601 UTC with milliseconds and a Z suffix", got)
	}
	if got := rec["requestId"]; got != "req-1" {
		t.Errorf("requestId = %v, want req-1", got)
	}
	if got := rec["executionArn"]; got != "arn:test" {
		t.Errorf("executionArn = %v, want arn:test", got)
	}
	if got := rec["tenantId"]; got != "tenant-a" {
		t.Errorf("tenantId = %v, want tenant-a", got)
	}
	if got := rec["operationId"]; got != hashID("1") {
		t.Errorf("operationId = %v, want the step's wire ID %q", got, hashID("1"))
	}
	if got := rec["operationName"]; got != "greet" {
		t.Errorf("operationName = %v, want greet", got)
	}
	if got := rec["attempt"]; got != float64(1) {
		t.Errorf("attempt = %v, want 1", got)
	}
}

func TestDefaultHandlerTimestampIsISO8601MillisUTC(t *testing.T) {
	// The record time is rendered in UTC regardless of its zone, with
	// exactly three fractional digits and a Z suffix.
	var buf bytes.Buffer
	h := newDefaultLogHandler(&buf, slog.LevelInfo)
	loc := time.FixedZone("plus2", 2*60*60)
	r := slog.NewRecord(time.Date(2026, 9, 17, 6, 56, 45, 657_123_456, loc), slog.LevelInfo, "m", 0)
	if err := h.Handle(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	rec := parseLogLines(t, buf.String())[0]
	if got := rec["timestamp"]; got != "2026-09-17T04:56:45.657Z" {
		t.Errorf("timestamp = %v, want 2026-09-17T04:56:45.657Z", got)
	}
	if _, ok := rec[slog.TimeKey]; ok {
		t.Error("record must not carry slog's built-in time key")
	}
	if _, ok := rec[slog.MessageKey]; ok {
		t.Error("record must not carry slog's built-in msg key")
	}
}

func TestDefaultHandlerLevelNames(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(newDefaultLogHandler(&buf, slog.LevelDebug))
	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Error("e")
	records := parseLogLines(t, buf.String())
	want := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, rec := range records {
		if rec["level"] != want[i] {
			t.Errorf("record %d level = %v, want %s", i, rec["level"], want[i])
		}
	}
}

// namedLogError is a user error type used to check errorType derivation.
type namedLogError struct{ msg string }

func (e *namedLogError) Error() string { return e.msg }

func TestDefaultHandlerExpandsErrorAttributes(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(newDefaultLogHandler(&buf, slog.LevelInfo))

	logger.Error("failed", "err", &namedLogError{msg: "boom"})
	logger.Error("plain", "cause", errors.New("plain failure"))
	logger.Error("sdk", "err", &StepError{
		Name: "s", Attempts: 1, ErrorType: "ValidationError", Message: "bad input",
		StackTrace: []string{"frame-1", "frame-2"},
	})

	records := parseLogLines(t, buf.String())
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}

	rec := records[0]
	if rec["errorType"] != "namedLogError" || rec["errorMessage"] != "boom" {
		t.Errorf("errorType/errorMessage = %v/%v, want namedLogError/boom", rec["errorType"], rec["errorMessage"])
	}
	if _, ok := rec["err"]; ok {
		t.Error("the error attribute must be replaced by errorType and errorMessage, not kept under its own key")
	}
	if _, ok := rec["stackTrace"]; ok {
		t.Error("stackTrace must be omitted for an error without recorded frames")
	}

	rec = records[1]
	if rec["errorType"] != "Error" || rec["errorMessage"] != "plain failure" {
		t.Errorf("errorType/errorMessage = %v/%v, want Error/plain failure", rec["errorType"], rec["errorMessage"])
	}

	rec = records[2]
	if rec["errorType"] != "ValidationError" {
		t.Errorf("errorType = %v, want the recorded ValidationError", rec["errorType"])
	}
	frames, _ := rec["stackTrace"].([]any)
	if len(frames) != 2 || frames[0] != "frame-1" {
		t.Errorf("stackTrace = %v, want the recorded frames", rec["stackTrace"])
	}
}

func TestLogLevelFromEnv(t *testing.T) {
	tests := []struct {
		value string
		want  slog.Level
	}{
		{"", slog.LevelInfo},
		{"TRACE", slog.LevelDebug - 4},
		{"DEBUG", slog.LevelDebug},
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"WARNING", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"FATAL", slog.LevelError + 4},
		{"nonsense", slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run("value="+tt.value, func(t *testing.T) {
			if got := logLevelFromEnv(tt.value); got != tt.want {
				t.Errorf("logLevelFromEnv(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestDefaultHandlerLevelFromEnvironment(t *testing.T) {
	// Each AWS_LAMBDA_LOG_LEVEL value selects the minimum level of the
	// handler Wrap installs; unset means INFO. Records below the minimum
	// are dropped, and Enabled reports them disabled so they cost no
	// formatting.
	tests := []struct {
		value    string
		disabled slog.Level
		enabled  slog.Level
	}{
		{"", slog.LevelDebug, slog.LevelInfo},
		{"DEBUG", slog.LevelDebug - 1, slog.LevelDebug},
		{"INFO", slog.LevelDebug, slog.LevelInfo},
		{"WARN", slog.LevelInfo, slog.LevelWarn},
		{"ERROR", slog.LevelWarn, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run("value="+tt.value, func(t *testing.T) {
			t.Setenv(logLevelEnvVar, tt.value)
			h := defaultLogHandler()
			if h.Enabled(t.Context(), tt.disabled) {
				t.Errorf("Enabled(%v) = true, want false", tt.disabled)
			}
			if !h.Enabled(t.Context(), tt.enabled) {
				t.Errorf("Enabled(%v) = false, want true", tt.enabled)
			}
		})
	}
}

func TestDefaultHandlerDropsRecordsBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(newDefaultLogHandler(&buf, slog.LevelWarn))
	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Log(t.Context(), slog.LevelError, "runtime-level")
	records := parseLogLines(t, buf.String())
	if len(records) != 2 || records[0]["level"] != "WARN" || records[1]["message"] != "runtime-level" {
		t.Fatalf("records = %v, want only the WARN and ERROR records", records)
	}
}

func TestWrapInstallsDefaultHandlerWhenNoneSupplied(t *testing.T) {
	// Without WithLogHandler, and with WithLogHandler(nil), Wrap installs
	// the default handler rather than leaving the logger without one.
	h := func(_ Context, _ string) (string, error) { return "", nil }
	for _, opts := range [][]HandlerOption{nil, {WithLogHandler(nil)}} {
		options := handlerOptions{}
		for _, o := range opts {
			o.applyHandler(&options)
		}
		if options.logHandler != nil {
			t.Fatal("options must not carry a handler before Wrap")
		}
		if Wrap(h, opts...) == nil {
			t.Fatal("Wrap returned nil")
		}
	}
}

func TestUserHandlerReceivesScopedAttributes(t *testing.T) {
	// A supplied handler receives the execution and operation attributes
	// as structured attributes (through WithAttrs), not baked into the
	// message, together with the call site's own attributes.
	rec := newRecordingHandler()
	runStepLoggingHandler(t, rec, "custom", "value")

	records := rec.all()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	r := records[0]
	if r.level != slog.LevelInfo {
		t.Errorf("level = %v, want INFO", r.level)
	}
	if r.message != "Greeting step started for: World" {
		t.Errorf("message = %q, want the bare message with no attributes baked in", r.message)
	}
	want := map[string]any{
		logKeyRequestID:     "req-1",
		logKeyExecutionArn:  "arn:test",
		logKeyTenantID:      "tenant-a",
		logKeyOperationID:   hashID("1"),
		logKeyOperationName: "greet",
		logKeyAttempt:       int64(1),
		"custom":            "value",
	}
	for k, v := range want {
		if r.attrs[k] != v {
			t.Errorf("attr %s = %v (%T), want %v (%T)", k, r.attrs[k], r.attrs[k], v, v)
		}
	}
	if len(r.attrs) != len(want) {
		t.Errorf("attrs = %v, want exactly %v", r.attrs, want)
	}
}

func TestExecutionAttributesOmitTenantWhenAbsent(t *testing.T) {
	// requestId and executionArn are always present; tenantId only when
	// the invocation has one. A handler-body record carries no operation
	// attributes.
	rec := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().Info("from handler")
		return "ok", nil
	}, withLambdaAPI(fake), WithLogHandler(rec))
	if _, err := h(lambdaCtx(t, "req-2", ""), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	records := rec.all()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	got := records[0].attrs
	if got[logKeyRequestID] != "req-2" || got[logKeyExecutionArn] != "arn:test" {
		t.Errorf("attrs = %v, want requestId req-2 and executionArn arn:test", got)
	}
	for _, absent := range []string{logKeyTenantID, logKeyOperationID, logKeyOperationName, logKeyAttempt} {
		if _, ok := got[absent]; ok {
			t.Errorf("attr %s must be omitted outside its scope, got %v", absent, got)
		}
	}
}

func TestChildContextLoggerCarriesChildOperationScope(t *testing.T) {
	// Each scope carries exactly its own operation attributes. The handler
	// body has none. A child context (RunInChildContext, Go) carries the
	// child operation's ID and name, with no attempt. A step nested inside
	// a child carries the step's own ID, name, and attempt, not the
	// child's. A grandchild carries its own ID and name.
	rec := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		ctx.Logger().Info("handler")
		_, err := RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			c.Logger().Info("child")
			if _, err := Step(c, "inner", func(sc StepContext) (string, error) {
				sc.Logger().Info("step")
				return "s", nil
			}); err != nil {
				return "", err
			}
			return RunInChildContext(c, "nested", func(cc Context) (string, error) {
				cc.Logger().Info("grandchild")
				return "g", nil
			})
		})
		if err != nil {
			return "", err
		}
		fut := Go(ctx, "branch", func(c Context) (string, error) {
			c.Logger().Info("go")
			return "b", nil
		})
		return fut.Result()
	}, withLambdaAPI(fake), WithLogHandler(rec))
	if _, err := h(lambdaCtx(t, "req-3", ""), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	base := map[string]any{logKeyRequestID: "req-3", logKeyExecutionArn: "arn:test"}
	withScope := func(extra map[string]any) map[string]any {
		want := map[string]any{}
		for k, v := range base {
			want[k] = v
		}
		for k, v := range extra {
			want[k] = v
		}
		return want
	}
	want := map[string]map[string]any{
		"handler":    withScope(nil),
		"child":      withScope(map[string]any{logKeyOperationID: hashID("1"), logKeyOperationName: "outer"}),
		"step":       withScope(map[string]any{logKeyOperationID: hashID("1-1"), logKeyOperationName: "inner", logKeyAttempt: int64(1)}),
		"grandchild": withScope(map[string]any{logKeyOperationID: hashID("1-2"), logKeyOperationName: "nested"}),
		"go":         withScope(map[string]any{logKeyOperationID: hashID("2"), logKeyOperationName: "branch"}),
	}
	records := rec.all()
	if len(records) != len(want) {
		t.Fatalf("got %d records %v, want %d", len(records), rec.messages(), len(want))
	}
	for _, r := range records {
		expect, ok := want[r.message]
		if !ok {
			t.Errorf("unexpected record %q", r.message)
			continue
		}
		if !reflect.DeepEqual(r.attrs, expect) {
			t.Errorf("record %q attrs = %v, want exactly %v", r.message, r.attrs, expect)
		}
	}
}

func TestDefaultHandlerNestedScopesEmitEachKeyOnce(t *testing.T) {
	// The default JSON handler does not merge repeated keys. A step inside
	// a child context must therefore replace the child's scope, not add to
	// it, so operationId and operationName appear once per record.
	var buf lockedBuffer
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "outer", func(c Context) (string, error) {
			c.Logger().Info("child")
			return Step(c, "inner", func(sc StepContext) (string, error) {
				sc.Logger().Info("step")
				return "s", nil
			})
		})
	}, withLambdaAPI(fake), WithLogHandler(newDefaultLogHandler(&buf, slog.LevelInfo)))
	if _, err := h(lambdaCtx(t, "req-4", ""), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	for _, line := range lines {
		for _, key := range []string{logKeyOperationID, logKeyOperationName, logKeyAttempt} {
			if n := strings.Count(line, `"`+key+`":`); n > 1 {
				t.Errorf("key %s appears %d times in %s", key, n, line)
			}
		}
	}
	stepLine := lines[1]
	if !strings.Contains(stepLine, `"`+logKeyOperationName+`":"inner"`) || !strings.Contains(stepLine, `"`+logKeyOperationID+`":"`+hashID("1-1")+`"`) {
		t.Errorf("step record carries the step's own scope, got %s", stepLine)
	}
}

func TestConditionCheckLoggerCarriesOperationAttributes(t *testing.T) {
	// A condition check's StepContext logger carries the operation ID and
	// name of the WaitForCondition operation and the poll attempt.
	rec := newRecordingHandler()
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (int, error) {
		return WaitForCondition(ctx, "poll", func(sc StepContext, n int) (int, error) {
			sc.Logger().Info("checking")
			return n + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: func(int, int) WaitDecision { return WaitDecision{Continue: false} },
		})
	}, withLambdaAPI(fake), WithLogHandler(rec))
	if _, err := h(t.Context(), childPayload(`"x"`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	records := rec.all()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	got := records[0].attrs
	if got[logKeyOperationID] != hashID("1") || got[logKeyOperationName] != "poll" || got[logKeyAttempt] != int64(1) {
		t.Errorf("attrs = %v, want operationId %q, operationName poll, attempt 1", got, hashID("1"))
	}
}

func TestOperationLogAttrsOmitEmptyName(t *testing.T) {
	attrs := operationLogAttrs("1-2", "", 3)
	keys := make([]string, 0, len(attrs))
	for _, a := range attrs {
		keys = append(keys, a.Key)
	}
	if strings.Join(keys, ",") != logKeyOperationID+","+logKeyAttempt {
		t.Errorf("keys = %v, want operationId and attempt only", keys)
	}
}

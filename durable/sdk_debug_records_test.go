package durable_test

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// sdkDebugMessages are the messages of the records the SDK writes at Debug
// about its own work.
var sdkDebugMessages = []string{
	"operation claimed",
	"replay complete; executing live",
	"checkpoint enqueued",
	"checkpoint flushed",
	"invocation suspending",
	"operation completed",
}

// attrRecord is one captured record with every attribute it carries, the
// ones attached through WithAttrs included, keyed by name.
type attrRecord struct {
	level slog.Level
	msg   string
	attrs map[string]slog.Value
}

// attrSink collects the records of every handler derived from one
// attrHandler.
type attrSink struct {
	mu   sync.Mutex
	recs []attrRecord
}

func (s *attrSink) records() []attrRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.recs)
}

// attrHandler records each record at or above level with the attributes
// attached through WithAttrs merged in. Groups are ignored.
type attrHandler struct {
	sink  *attrSink
	level slog.Level
	attrs []slog.Attr
}

func (h attrHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h attrHandler) Handle(_ context.Context, r slog.Record) error {
	m := make(map[string]slog.Value, len(h.attrs)+r.NumAttrs())
	for _, a := range h.attrs {
		m[a.Key] = a.Value.Resolve()
	}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Resolve()
		return true
	})
	h.sink.mu.Lock()
	h.sink.recs = append(h.sink.recs, attrRecord{level: r.Level, msg: r.Message, attrs: m})
	h.sink.mu.Unlock()
	return nil
}

func (h attrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.attrs = append(slices.Clip(h.attrs), attrs...)
	return h
}

func (h attrHandler) WithGroup(string) slog.Handler { return h }

// isSDKDebug reports whether r is one of the SDK's Debug records.
func isSDKDebug(r attrRecord) bool {
	return r.level == slog.LevelDebug && slices.Contains(sdkDebugMessages, r.msg)
}

// lifecycleHandler claims a step, a child context with a nested wait and
// step, and a root wait, so it writes checkpoints, suspends twice, and
// resumes twice. mode is the replay log mode it selects.
func lifecycleHandler(sink *attrSink, level slog.Level, mode durable.ReplayLogMode) func(durable.Context, any) (string, error) {
	return func(ctx durable.Context, _ any) (string, error) {
		if err := durable.ConfigureLogging(ctx, durable.LogConfig{
			Handler:       attrHandler{sink: sink, level: level},
			ReplayLogMode: mode,
		}); err != nil {
			return "", err
		}
		if _, err := durable.Step(ctx, "s1", func(_ durable.StepContext) (string, error) {
			return "a", nil
		}); err != nil {
			return "", err
		}
		if _, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
			if err := durable.Wait(c, "inner-pause", time.Minute); err != nil {
				return "", err
			}
			return durable.Step(c, "s2", func(_ durable.StepContext) (string, error) {
				return "b", nil
			})
		}); err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "pause", time.Minute); err != nil {
			return "", err
		}
		return "done", nil
	}
}

func runLifecycle(t *testing.T, level slog.Level, mode durable.ReplayLogMode) []attrRecord {
	t.Helper()
	sink := &attrSink{}
	result, err := durabletest.NewLocalRunner(lifecycleHandler(sink, level, mode)).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	return sink.records()
}

// requireString fails unless r carries key as a string and returns it.
func requireString(t *testing.T, r attrRecord, key string) string {
	t.Helper()
	v, ok := r.attrs[key]
	if !ok {
		t.Fatalf("%q record: missing %s; attrs %v", r.msg, key, r.attrs)
	}
	if v.Kind() != slog.KindString {
		t.Fatalf("%q record: %s kind = %s, want String", r.msg, key, v.Kind())
	}
	return v.String()
}

// requireInt fails unless r carries key as an int.
func requireInt(t *testing.T, r attrRecord, key string) {
	t.Helper()
	v, ok := r.attrs[key]
	if !ok {
		t.Fatalf("%q record: missing %s; attrs %v", r.msg, key, r.attrs)
	}
	if v.Kind() != slog.KindInt64 {
		t.Fatalf("%q record: %s kind = %s, want Int64", r.msg, key, v.Kind())
	}
}

// requireAbsent fails if r carries key.
func requireAbsent(t *testing.T, r attrRecord, key string) {
	t.Helper()
	if _, ok := r.attrs[key]; ok {
		t.Fatalf("%q record: unexpected %s; attrs %v", r.msg, key, r.attrs)
	}
}

func findRecord(recs []attrRecord, match func(attrRecord) bool) (attrRecord, bool) {
	for _, r := range recs {
		if match(r) {
			return r, true
		}
	}
	return attrRecord{}, false
}

func withName(msg, name string) func(attrRecord) bool {
	return func(r attrRecord) bool {
		v, ok := r.attrs["operationName"]
		return r.msg == msg && ok && v.String() == name
	}
}

func TestSDKDebugRecordsLifecycle(t *testing.T) {
	recs := runLifecycle(t, slog.LevelDebug, durable.ReplayLogModeSuppress)

	for _, msg := range sdkDebugMessages {
		if _, ok := findRecord(recs, func(r attrRecord) bool { return r.msg == msg }); !ok {
			t.Errorf("no %q record", msg)
		}
	}

	// Every SDK record carries the invocation's identity attributes. The
	// local runner passes no Lambda request ID, so requestId is checked
	// for its presence and type only.
	for _, r := range recs {
		if !isSDKDebug(r) {
			continue
		}
		requireString(t, r, "requestId")
		if requireString(t, r, "executionArn") == "" {
			t.Errorf("%q record: empty executionArn", r.msg)
		}
		requireAbsent(t, r, "replay")
	}

	claimed, ok := findRecord(recs, withName("operation claimed", "s1"))
	if !ok {
		t.Fatal("no operation claimed record for s1")
	}
	requireString(t, claimed, "operationId")
	if got := requireString(t, claimed, "operationSubtype"); got != "Step" {
		t.Errorf("s1 operationSubtype = %q, want Step", got)
	}

	// An operation claimed inside a child context names that operation.
	nested, ok := findRecord(recs, withName("operation claimed", "s2"))
	if !ok {
		t.Fatal("no operation claimed record for s2")
	}
	requireString(t, nested, "operationId")
	if nested.attrs["operationId"].String() == claimed.attrs["operationId"].String() {
		t.Error("s1 and s2 records carry the same operationId")
	}
	childClaim, ok := findRecord(recs, withName("operation claimed", "child"))
	if !ok {
		t.Fatal("no operation claimed record for child")
	}
	if got := requireString(t, childClaim, "operationSubtype"); got != "RunInChildContext" {
		t.Errorf("child operationSubtype = %q, want RunInChildContext", got)
	}
	unnamed, ok := findRecord(recs, func(r attrRecord) bool {
		return r.msg == "operation claimed" && r.attrs["operationSubtype"].String() == "Wait" &&
			r.attrs["operationName"].String() == "pause"
	})
	if !ok {
		t.Fatal("no operation claimed record for pause")
	}
	requireString(t, unnamed, "operationId")

	completed, ok := findRecord(recs, withName("operation completed", "s1"))
	if !ok {
		t.Fatal("no operation completed record for s1")
	}
	requireString(t, completed, "operationId")
	if got := requireString(t, completed, "operationSubtype"); got != "Step" {
		t.Errorf("completed operationSubtype = %q, want Step", got)
	}
	if got := requireString(t, completed, "status"); got != "SUCCEEDED" {
		t.Errorf("completed status = %q, want SUCCEEDED", got)
	}

	for _, msg := range []string{"checkpoint enqueued", "checkpoint flushed"} {
		r, _ := findRecord(recs, func(r attrRecord) bool { return r.msg == msg })
		requireInt(t, r, "updateCount")
	}
	enqueued, _ := findRecord(recs, func(r attrRecord) bool {
		return r.msg == "checkpoint enqueued" && r.attrs["updateCount"].Int64() > 0
	})
	if enqueued.msg == "" {
		t.Error("no checkpoint enqueued record with updates")
	}

	suspending, _ := findRecord(recs, func(r attrRecord) bool { return r.msg == "invocation suspending" })
	if got := requireString(t, suspending, "reason"); got != "wait" {
		t.Errorf("suspending reason = %q, want wait", got)
	}

	// The child context leaves replay when its inner wait reports a new
	// outcome; that record names the child. The root leaves replay when
	// the child returns, and its record names no operation.
	childLive, ok := findRecord(recs, withName("replay complete; executing live", "child"))
	if !ok {
		t.Fatal("no replay complete record for the child context")
	}
	requireString(t, childLive, "operationId")
	rootLive, ok := findRecord(recs, func(r attrRecord) bool {
		_, named := r.attrs["operationId"]
		return r.msg == "replay complete; executing live" && !named
	})
	if !ok {
		t.Fatal("no replay complete record for the root context")
	}
	requireAbsent(t, rootLive, "operationName")
}

func TestSDKDebugRecordsAbsentAtInfo(t *testing.T) {
	recs := runLifecycle(t, slog.LevelInfo, durable.ReplayLogModeSuppress)
	n := 0
	for _, r := range recs {
		if r.level == slog.LevelDebug {
			n++
		}
	}
	if n != 0 {
		t.Fatalf("Debug records at Info level = %d, want 0", n)
	}
}

// TestSDKDebugRecordsReplayMode checks the s1 operation claimed record,
// which the second and third invocations write while the root replays.
func TestSDKDebugRecordsReplayMode(t *testing.T) {
	count := func(recs []attrRecord) (live, replayed int) {
		for _, r := range recs {
			if !withName("operation claimed", "s1")(r) {
				continue
			}
			if v, ok := r.attrs["replay"]; ok {
				if v.Kind() != slog.KindBool || !v.Bool() {
					t.Fatalf("replay attribute = %v, want true", v)
				}
				replayed++
				continue
			}
			live++
		}
		return live, replayed
	}

	live, replayed := count(runLifecycle(t, slog.LevelDebug, durable.ReplayLogModeSuppress))
	if live != 1 || replayed != 0 {
		t.Errorf("suppress: live = %d, replayed = %d, want 1 and 0", live, replayed)
	}
	live, replayed = count(runLifecycle(t, slog.LevelDebug, durable.ReplayLogModeEmit))
	if live != 1 || replayed != 2 {
		t.Errorf("emit: live = %d, replayed = %d, want 1 and 2", live, replayed)
	}
}

// TestSDKDebugSuspendReasons checks the reason of the invocation suspending
// record for each kind of pending operation.
func TestSDKDebugSuspendReasons(t *testing.T) {
	cases := []struct {
		name string
		body func(ctx durable.Context) error
		want string
	}{
		{"callback", func(ctx durable.Context) error {
			cb, err := durable.CreateCallback[string](ctx, "cb")
			if err != nil {
				return err
			}
			_, err = cb.Result(ctx)
			return err
		}, "callback"},
		{"retry", func(ctx durable.Context) error {
			strategy, err := durable.NewRetryStrategy(durable.RetryConfig{MaxAttempts: 2, InitialDelay: time.Minute, BackoffRate: 1})
			if err != nil {
				return err
			}
			_, err = durable.Step(ctx, "flaky", func(sc durable.StepContext) (int, error) {
				if sc.Attempt() == 1 {
					return 0, errDebugFlaky
				}
				return 1, nil
			}, durable.WithRetry(strategy))
			return err
		}, "retry"},
		{"condition", func(ctx durable.Context) error {
			_, err := durable.WaitForCondition(ctx, "poll", func(_ durable.StepContext, s int) (int, error) {
				return s + 1, nil
			}, durable.ConditionConfig[int]{WaitStrategy: func(s, _ int) durable.WaitDecision {
				return durable.WaitDecision{Continue: s < 2, Delay: time.Minute}
			}})
			return err
		}, "condition"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &attrSink{}
			handler := func(ctx durable.Context, _ any) (string, error) {
				if err := durable.ConfigureLogging(ctx, durable.LogConfig{
					Handler: attrHandler{sink: sink, level: slog.LevelDebug},
				}); err != nil {
					return "", err
				}
				return "", tc.body(ctx)
			}
			if _, err := durabletest.NewLocalRunner(handler).Run(nil); err != nil {
				t.Fatal(err)
			}
			r, ok := findRecord(sink.records(), func(r attrRecord) bool { return r.msg == "invocation suspending" })
			if !ok {
				t.Fatal("no invocation suspending record")
			}
			if got := requireString(t, r, "reason"); got != tc.want {
				t.Errorf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

type debugFlakyError struct{}

func (debugFlakyError) Error() string { return "flaky" }

var errDebugFlaky error = debugFlakyError{}

// TestSDKDebugCallbackCompletedLive checks the operation completed record
// of a callback resolved between invocations. The second invocation is the
// first to observe the outcome, so the record is live under both replay
// log modes, although the root context is still replaying when
// CreateCallback reports the outcome.
func TestSDKDebugCallbackCompletedLive(t *testing.T) {
	for _, mode := range []durable.ReplayLogMode{durable.ReplayLogModeSuppress, durable.ReplayLogModeEmit} {
		sink := &attrSink{}
		handler := func(ctx durable.Context, _ any) (string, error) {
			if err := durable.ConfigureLogging(ctx, durable.LogConfig{
				Handler:       attrHandler{sink: sink, level: slog.LevelDebug},
				ReplayLogMode: mode,
			}); err != nil {
				return "", err
			}
			cb, err := durable.CreateCallback[string](ctx, "cb")
			if err != nil {
				return "", err
			}
			return cb.Result(ctx)
		}
		runner := durabletest.NewLocalRunner(handler)
		r, err := runner.Run(nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Pending {
			t.Fatalf("mode %v: first invocation = %s, want PENDING", mode, r.Status)
		}
		open := runner.OpenCallbacks()
		if len(open) != 1 {
			t.Fatalf("mode %v: open callbacks = %d, want 1", mode, len(open))
		}
		if err := runner.SendCallbackSuccess(open[0].CallbackID, "ok"); err != nil {
			t.Fatal(err)
		}
		r, err = runner.Run(nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Succeeded {
			t.Fatalf("mode %v: status = %s, want SUCCEEDED", mode, r.Status)
		}

		var live, replayed int
		for _, rec := range sink.records() {
			if !withName("operation completed", "cb")(rec) {
				continue
			}
			if _, ok := rec.attrs["replay"]; ok {
				replayed++
				continue
			}
			live++
			if got := requireString(t, rec, "status"); got != "SUCCEEDED" {
				t.Errorf("mode %v: status = %q, want SUCCEEDED", mode, got)
			}
		}
		if live != 1 || replayed != 0 {
			t.Errorf("mode %v: live = %d, replayed = %d, want 1 and 0", mode, live, replayed)
		}
	}
}

// countCompleted returns how many operation completed records name carries
// with no replay attribute, and how many carry replay=true.
func countCompleted(t *testing.T, recs []attrRecord, name string) (live, replayed int) {
	t.Helper()
	for _, rec := range recs {
		if !withName("operation completed", name)(rec) {
			continue
		}
		if v, ok := rec.attrs["replay"]; ok {
			if v.Kind() != slog.KindBool || !v.Bool() {
				t.Fatalf("%q record: replay = %v, want true", name, v)
			}
			replayed++
			continue
		}
		live++
	}
	return live, replayed
}

// countSuspends returns how many invocation suspending records carry reason.
func countSuspends(recs []attrRecord, reason string) int {
	n := 0
	for _, rec := range recs {
		if v, ok := rec.attrs["reason"]; ok && rec.msg == "invocation suspending" && v.String() == reason {
			n++
		}
	}
	return n
}

// TestSDKDebugResumedStepCompletedLive checks the operation completed record
// of a step whose first attempt fails and whose retry runs in the next
// invocation. That invocation re-enters the step from its checkpointed
// retry state and records the outcome, so the record is live under both
// replay log modes. A later invocation replays the outcome, which only
// ReplayLogModeEmit writes, with replay=true.
func TestSDKDebugResumedStepCompletedLive(t *testing.T) {
	cases := []struct {
		mode         durable.ReplayLogMode
		wantReplayed int
	}{
		{durable.ReplayLogModeSuppress, 0},
		{durable.ReplayLogModeEmit, 1},
	}
	for _, tc := range cases {
		sink := &attrSink{}
		handler := func(ctx durable.Context, _ any) (string, error) {
			if err := durable.ConfigureLogging(ctx, durable.LogConfig{
				Handler:       attrHandler{sink: sink, level: slog.LevelDebug},
				ReplayLogMode: tc.mode,
			}); err != nil {
				return "", err
			}
			strategy, err := durable.NewRetryStrategy(durable.RetryConfig{
				MaxAttempts:  2,
				InitialDelay: time.Second,
				BackoffRate:  1,
			})
			if err != nil {
				return "", err
			}
			out, err := durable.Step(ctx, "flaky", func(sc durable.StepContext) (string, error) {
				if sc.Attempt() == 1 {
					return "", errDebugFlaky
				}
				return "ok", nil
			}, durable.WithRetry(strategy))
			if err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "after", time.Minute); err != nil {
				return "", err
			}
			return out, nil
		}
		r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Succeeded {
			t.Fatalf("mode %v: status = %s, want SUCCEEDED", tc.mode, r.Status)
		}
		recs := sink.records()
		if n := countSuspends(recs, "retry"); n != 1 {
			t.Fatalf("mode %v: retry suspensions = %d, want 1", tc.mode, n)
		}
		live, replayed := countCompleted(t, recs, "flaky")
		if live != 1 || replayed != tc.wantReplayed {
			t.Errorf("mode %v: live = %d, replayed = %d, want 1 and %d", tc.mode, live, replayed, tc.wantReplayed)
		}
	}
}

// TestSDKDebugAsyncVirtualChildCompletedLive checks the operation completed
// record of an asynchronous virtual child context whose inner wait spans
// invocations. In the invocation that observes the wait's outcome, the
// child leaves replay and completes while its parent is still replaying,
// before the parent reads the future. The record follows the child, so it
// is live under both replay log modes. A later invocation replays the
// child, which only ReplayLogModeEmit writes, with replay=true.
func TestSDKDebugAsyncVirtualChildCompletedLive(t *testing.T) {
	cases := []struct {
		mode         durable.ReplayLogMode
		wantReplayed int
	}{
		{durable.ReplayLogModeSuppress, 0},
		{durable.ReplayLogModeEmit, 1},
	}
	for _, tc := range cases {
		sink := &attrSink{}
		handler := func(ctx durable.Context, _ any) (string, error) {
			if err := durable.ConfigureLogging(ctx, durable.LogConfig{
				Handler:       attrHandler{sink: sink, level: slog.LevelDebug},
				ReplayLogMode: tc.mode,
			}); err != nil {
				return "", err
			}
			fut := durable.RunInChildContextAsync(ctx, "vchild", func(c durable.Context) (string, error) {
				if err := durable.Wait(c, "inner", time.Second); err != nil {
					return "", err
				}
				return durable.Step(c, "inner-step", func(_ durable.StepContext) (string, error) {
					return "v", nil
				})
			}, durable.WithChildVirtual())
			out, err := fut.Result(ctx)
			if err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "after", time.Minute); err != nil {
				return "", err
			}
			return out, nil
		}
		r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Succeeded {
			t.Fatalf("mode %v: status = %s, want SUCCEEDED", tc.mode, r.Status)
		}
		recs := sink.records()
		if n := countSuspends(recs, "wait"); n != 2 {
			t.Fatalf("mode %v: wait suspensions = %d, want 2", tc.mode, n)
		}
		live, replayed := countCompleted(t, recs, "vchild")
		if live != 1 || replayed != tc.wantReplayed {
			t.Errorf("mode %v: live = %d, replayed = %d, want 1 and %d", tc.mode, live, replayed, tc.wantReplayed)
		}
	}
}

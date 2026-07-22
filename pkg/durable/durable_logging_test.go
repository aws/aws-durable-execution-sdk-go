package durable_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// errTransientFailure is a shared sentinel used by this file's retry tests
// (TestLogging_RetryRealReExecutionNotSuppressed,
// TestLogging_RetryAttemptFieldIncrements) to simulate a step's first
// attempt failing before a real (in-process, zero-delay) retry succeeds.
var errTransientFailure = errors.New("transient failure")

// fixedZeroDelayRetry returns a retry strategy that always retries
// (up to maxAttempts) with a zero-second delay, matching the
// "retry immediately, no suspend/re-invoke round trip" idiom documented on
// utils.Presets.FixedDelay and used identically in durable_test.go's
// TestStep_RetryThenSucceed. A zero delay is required for this file's
// tests, which assert on log calls WITHIN a single invocation - a non-zero
// delay would suspend the invocation via execmgr.Manager.WaitForOperation
// (see step.go's retryOrFail) and require a second, separate invocation
// (as TestLogging_ReplaySkipSuppressed_RealExecutionNotSuppressed already
// covers for the replay-across-invocations case) rather than exercising
// the "real re-execution within the same invocation" distinction task 13
// calls out.
func fixedZeroDelayRetry(maxAttempts int) func(err error, attempt int) types.RetryDecision {
	return utils.Presets.FixedDelay(types.Duration{Seconds: 0}, maxAttempts)
}

// recordingLogger is a types.Logger test double that records every call
// made to it into an in-memory slice, for asserting on exactly which log
// calls happened (and with what fields) across an invocation -
// docs/remaining-work.md §5 tasks 13/14 explicitly ask for this rather
// than only asserting on side effects. No equivalent test double existed
// in this package before this task (grepped for "Logger"/"logger" across
// pkg/durable/*_test.go and pkg/durable/testing/*_test.go beforehand -
// no matches), so this is new.
type recordingLogger struct {
	mu    sync.Mutex
	calls []loggedCall
}

type loggedCall struct {
	level  string
	msg    string
	fields map[string]any
}

func newRecordingLogger() *recordingLogger { return &recordingLogger{} }

func (l *recordingLogger) record(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, loggedCall{level: level, msg: msg, fields: fields})
}

func (l *recordingLogger) Debug(msg string, fields map[string]any) { l.record("DEBUG", msg, fields) }
func (l *recordingLogger) Info(msg string, fields map[string]any)  { l.record("INFO", msg, fields) }
func (l *recordingLogger) Warn(msg string, fields map[string]any)  { l.record("WARN", msg, fields) }
func (l *recordingLogger) Error(msg string, fields map[string]any) { l.record("ERROR", msg, fields) }

// messages returns just the recorded messages, in call order, for
// assertions that don't care about fields.
func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.calls))
	for i, c := range l.calls {
		out[i] = c.msg
	}
	return out
}

// findCall returns the fields of the first recorded call with the given
// message, or nil if none matches.
func (l *recordingLogger) findCall(msg string) map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.calls {
		if c.msg == msg {
			return c.fields
		}
	}
	return nil
}

var _ types.Logger = (*recordingLogger)(nil)

// TestLogging_ReplaySkipSuppressed_SingleStepFullyReplaySkips is the core
// task-13 suppression test: when the handler's only durable operation is
// already fully checkpointed, EVERY log call made via
// DurableContext.Logger() on the replay invocation is suppressed - not
// just calls positioned textually before the step - because there is no
// "next incomplete operation" to reach at all on that invocation. This
// matches the confirmed official doc's own worked example verbatim: a
// single step, with logger calls immediately before and after it, replayed
// once the step is complete.
func TestLogging_ReplaySkipSuppressed_SingleStepFullyReplaySkips(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		dc.Logger().Info("before step", map[string]any{})
		result, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("inside step body", map[string]any{})
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		dc.Logger().Info("after step", map[string]any{})
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "abc"})

	// First (real) invocation: every log call should appear exactly once.
	firstInput := newInvocationInput("exec-log-1", "arn:test:log-1", "token-0", eventPayload)
	out1, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("first invocation error: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected first invocation Succeeded, got %s", out1.Status)
	}

	firstMsgs := logger.messages()
	wantFirst := []string{"before step", "inside step body", "after step"}
	if len(firstMsgs) != len(wantFirst) {
		t.Fatalf("first invocation: expected %v, got %v", wantFirst, firstMsgs)
	}
	for i, m := range wantFirst {
		if firstMsgs[i] != m {
			t.Fatalf("first invocation: expected %v, got %v", wantFirst, firstMsgs)
		}
	}

	// Second invocation ("replay"): seed the checkpointed step so it
	// replay-skips. Per the confirmed official doc ("it runs your handler
	// from the start until it reaches the next incomplete operation...
	// does not re-emit log entries encountered before that point"), since
	// this handler's ONLY operation (the step) is now fully complete,
	// there IS no "next incomplete operation" to reach on this
	// invocation at all - the replay-skip frontier is never crossed, and
	// EVERY log call in the handler (both "before step" and "after step")
	// is suppressed, exactly like the JS reference doc's own worked
	// example (a single step, logs immediately before and after it) would
	// suppress BOTH surrounding log lines on a replay where that one step
	// is already complete - not just the one before the step. "inside
	// step body" can never appear regardless, since the step replay-skips
	// and fn is never invoked at all.
	snapshot := client.snapshot()
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-log-1", "arn:test:log-1", "token-1", eventPayload, extraOps...)

	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation error: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second invocation Succeeded, got %s", out2.Status)
	}

	allMsgs := logger.messages()
	secondMsgs := allMsgs[len(firstMsgs):]
	if len(secondMsgs) != 0 {
		t.Fatalf("second invocation: expected no log calls at all (the handler's only operation fully replay-skips, so the suppression frontier is never crossed), got %v", secondMsgs)
	}
}

// TestLogging_ReplaySkipSuppressed_CrossesFrontierAtIncompleteStep is the
// complementary task-13 test to the single-step case above: with TWO
// steps, where only the FIRST is already checkpointed on replay, logging
// before/inside/after the first (replay-skipping) step is suppressed, but
// the SECOND step (genuinely incomplete - not yet checkpointed at all) is
// where the replay-skip frontier is crossed, per the confirmed official
// doc's "runs your handler from the start until it reaches the next
// incomplete operation." Every log call from that point onward - inside
// the second step's real execution, and any handler code after it - must
// NOT be suppressed.
func TestLogging_ReplaySkipSuppressed_CrossesFrontierAtIncompleteStep(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		dc.Logger().Info("before step one", map[string]any{})
		_, err := operations.Step(dc, "step-one", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("inside step one", map[string]any{})
			return "one", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		dc.Logger().Info("between steps", map[string]any{})
		result, err := operations.Step(dc, "step-two", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("inside step two", map[string]any{})
			return "two", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		dc.Logger().Info("after step two", map[string]any{})
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "abc"})

	firstInput := newInvocationInput("exec-log-1b", "arn:test:log-1b", "token-0", eventPayload)
	if _, err := entry(context.Background(), firstInput); err != nil {
		t.Fatalf("first invocation error: %v", err)
	}
	firstMsgs := logger.messages()

	// Second invocation: seed ONLY step-one's checkpointed operation (not
	// step-two's), simulating a replay that resumes partway through -
	// step-one replay-skips, step-two does not exist in the log yet and
	// is therefore the "next incomplete operation."
	snapshot := client.snapshot()
	stepOneOp, ok := snapshot[expectedStepID("exec-log-1b", 1)]
	if !ok {
		t.Fatal("expected step-one (hashed) to be checkpointed after the first invocation")
	}
	secondInput := newInvocationInput("exec-log-1b", "arn:test:log-1b", "token-1", eventPayload, stepOneOp)

	if _, err := entry(context.Background(), secondInput); err != nil {
		t.Fatalf("second invocation error: %v", err)
	}

	secondMsgs := logger.messages()[len(firstMsgs):]
	want := []string{"inside step two", "after step two"}
	if len(secondMsgs) != len(want) {
		t.Fatalf("second invocation: expected %v (everything up to and including step-one's replay-skip suppressed; step-two's real execution and everything after it NOT suppressed), got %v", want, secondMsgs)
	}
	for i, m := range want {
		if secondMsgs[i] != m {
			t.Fatalf("second invocation: expected %v, got %v", want, secondMsgs)
		}
	}
}

// TestLogging_RetryRealReExecutionNotSuppressed verifies the specific
// distinction docs/remaining-work.md §5 task 13 calls out: a retry's real
// re-execution of a step body must NOT be suppressed just because the
// overall invocation is, in some global sense, "replaying" - only an
// actual replay-SKIP (returning a checkpointed result without running fn)
// is suppressed. This drives a step through one failure and one retry
// entirely within a SINGLE invocation (SkipTime-equivalent: a zero-second
// retry delay re-executes immediately in-process per step.go's
// retryOrFail doc), so there is no separate "replay invocation" here at
// all - both the failing attempt 1 and the succeeding attempt 2 are real
// executions of fn, and both must log.
func TestLogging_RetryRealReExecutionNotSuppressed(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("attempt running", map[string]any{"attempt": sc.Attempt()})
			if sc.Attempt() < 2 {
				return "", errTransientFailure
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	input := newInvocationInput("exec-log-2", "arn:test:log-2", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "xyz"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded after retry, got status=%s error=%v", out.Status, out.Error)
	}

	msgs := logger.messages()
	if len(msgs) != 2 {
		t.Fatalf("expected both the failing attempt and the retry's real re-execution to log (2 calls), got %d: %v", len(msgs), msgs)
	}
	for _, m := range msgs {
		if m != "attempt running" {
			t.Fatalf("unexpected message %q in %v", m, msgs)
		}
	}
}

// TestLogging_ModeAwareFalseNeverSuppresses verifies that setting
// ModeAware: false (an explicit LoggerConfig with the field left at its
// zero value, or explicitly false) disables replay suppression entirely -
// every log call appears on every invocation, matching JS's documented
// "Pass modeAware: false to emit logs on every replay."
func TestLogging_ModeAwareFalseNeverSuppresses(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		dc.Logger().Info("before step", map[string]any{})
		result, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: false},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "abc"})

	firstInput := newInvocationInput("exec-log-3", "arn:test:log-3", "token-0", eventPayload)
	if _, err := entry(context.Background(), firstInput); err != nil {
		t.Fatalf("first invocation error: %v", err)
	}

	snapshot := client.snapshot()
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-log-3", "arn:test:log-3", "token-1", eventPayload, extraOps...)
	if _, err := entry(context.Background(), secondInput); err != nil {
		t.Fatalf("second invocation error: %v", err)
	}

	msgs := logger.messages()
	// "before step" should appear on BOTH invocations since ModeAware is
	// off - 2 total calls to "before step".
	count := 0
	for _, m := range msgs {
		if m == "before step" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected 'before step' to log on both invocations with ModeAware=false, got %d occurrences in %v", count, msgs)
	}
}

// TestLogging_StepContextFieldsInjected is the core task-14 test: a log
// call made via StepContext.Logger() automatically carries the step's
// operation ID, name, and attempt number as structured fields, without
// the caller passing them.
func TestLogging_StepContextFieldsInjected(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "validate-order", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("running", map[string]any{})
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	input := newInvocationInput("exec-log-4", "arn:test:log-4", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fields := logger.findCall("running")
	if fields == nil {
		t.Fatal("expected a 'running' log call to have been recorded")
	}
	wantOperationID := expectedStepID("exec-log-4", 1)
	if fields["operationId"] != wantOperationID {
		t.Errorf("expected operationId=%s (hashed), got %v", wantOperationID, fields["operationId"])
	}
	if fields["operationName"] != "validate-order" {
		t.Errorf("expected operationName=validate-order, got %v", fields["operationName"])
	}
	if fields["attempt"] != 1 {
		t.Errorf("expected attempt=1, got %v", fields["attempt"])
	}
	if fields["executionArn"] != "arn:test:log-4" {
		t.Errorf("expected executionArn=arn:test:log-4, got %v", fields["executionArn"])
	}
}

// TestLogging_RetryAttemptFieldIncrements verifies the attempt field
// specifically increments across a real retry (not just defaults to 1),
// since attempt is the one field of the three (operationId/operationName/
// attempt) that varies within a single step's lifetime.
func TestLogging_RetryAttemptFieldIncrements(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("attempt log", map[string]any{})
			if sc.Attempt() < 2 {
				return "", errTransientFailure
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](fixedZeroDelayRetry(5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	input := newInvocationInput("exec-log-5", "arn:test:log-5", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "xyz"}))

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var attempts []any
	for _, m := range logger.calls {
		if m.msg == "attempt log" {
			attempts = append(attempts, m.fields["attempt"])
		}
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2 'attempt log' calls, got %d: %v", len(attempts), attempts)
	}
	if attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("expected attempt fields [1 2], got %v", attempts)
	}
}

// TestLogging_ChildContextFieldsInjected verifies that a DurableContext
// obtained from RunInChildContext carries contextId/contextName in
// addition to executionArn, matching the confirmed cross-SDK "DurableContext
// (child)" execution metadata.
func TestLogging_ChildContextFieldsInjected(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.RunInChildContext(dc, "child-work", func(child types.DurableContext) (string, error) {
			child.Logger().Info("inside child context", map[string]any{})
			return "done", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)
	input := newInvocationInput("exec-log-6", "arn:test:log-6", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fields := logger.findCall("inside child context")
	if fields == nil {
		t.Fatal("expected an 'inside child context' log call to have been recorded")
	}
	wantContextID := expectedStepID("exec-log-6", 1)
	if fields["contextId"] != wantContextID {
		t.Errorf("expected contextId=%s (hashed), got %v", wantContextID, fields["contextId"])
	}
	if fields["contextName"] != "child-work" {
		t.Errorf("expected contextName=child-work, got %v", fields["contextName"])
	}
	if fields["operationId"] != wantContextID {
		t.Errorf("expected operationId=%s (hashed), got %v", wantContextID, fields["operationId"])
	}
	if fields["executionArn"] != "arn:test:log-6" {
		t.Errorf("expected executionArn=arn:test:log-6, got %v", fields["executionArn"])
	}
	if _, hasParent := fields["parentId"]; hasParent {
		t.Errorf("expected no parentId for a context directly under the root, got %v", fields["parentId"])
	}
}

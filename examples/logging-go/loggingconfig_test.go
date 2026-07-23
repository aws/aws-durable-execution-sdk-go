// loggingconfig_test.go constructs durable.WithDurableExecution directly
// (bypassing testing.LocalTestRunner, which builds its own internal
// durable.Config and has no hook today for injecting a custom
// types.LoggerConfig - confirmed by reading runner.go's Run/Continue in
// full: both always call durable.WithDurableExecution(r.handler,
// &durable.Config{Client: r.client}), a literal with no LoggerConfig
// field set at all), so this test can directly observe this example's
// actual replay-mode-aware log suppression behavior (docs/remaining-work.md
// §5 tasks 13/14) against a recordingLogger test double, using a small
// local fakeClient - mirroring pkg/durable/durable_fake_client_test.go's
// fakeClient and pkg/durable/durable_logging_test.go's own two-invocation
// test structure (both internal to the durable package's test suite and
// not importable from here), adapted to this example's ReportEvent/
// ReportResult handler.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// recordingLogger is a types.Logger test double recording every call made
// to it, in order, for asserting on exactly which log calls happened
// across an invocation. See this file's top-level doc for why this is a
// local copy of pkg/durable/durable_logging_test.go's identically-named
// type rather than an import (that file lives in an internal _test.go
// file in a different package).
type recordingLogger struct {
	mu    sync.Mutex
	calls []string
}

func newRecordingLogger() *recordingLogger { return &recordingLogger{} }

func (l *recordingLogger) record(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, msg)
}

func (l *recordingLogger) Debug(msg string, _ map[string]any) { l.record(msg) }
func (l *recordingLogger) Info(msg string, _ map[string]any)  { l.record(msg) }
func (l *recordingLogger) Warn(msg string, _ map[string]any)  { l.record(msg) }
func (l *recordingLogger) Error(msg string, _ map[string]any) { l.record(msg) }

// messages returns just the recorded messages, in call order.
func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.calls))
	copy(out, l.calls)
	return out
}

var _ types.Logger = (*recordingLogger)(nil)

// fakeClient is a minimal in-memory checkpoint.Client, adapted from
// pkg/durable/durable_fake_client_test.go's identically-named type (see
// that file's doc for the full field-by-field rationale, which applies
// unchanged here) down to exactly what this example's handler needs:
// STEP operations only (no WAIT/CALLBACK/CHAINED_INVOKE/CONTEXT
// handling, since handler.go never uses those), and no retry-specific
// Attempt/Payload bookkeeping (both of this handler's steps always
// succeed on their first attempt).
type fakeClient struct {
	mu    sync.Mutex
	ops   map[string]types.Operation
	token int
}

func newFakeClient() *fakeClient {
	return &fakeClient{ops: make(map[string]types.Operation)}
}

func (f *fakeClient) Checkpoint(_ context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var updated []types.Operation
	for _, u := range req.Updates {
		op := f.ops[u.ID]
		op.ID = u.ID
		op.ParentID = u.ParentID
		op.Name = u.Name
		op.Type = u.Type

		switch u.Action {
		case types.OperationActionStart:
			op.Status = types.OperationStatusStarted
		case types.OperationActionSucceed:
			op.Status = types.OperationStatusSucceeded
			if u.Type == types.OperationTypeStep {
				if op.StepDetails == nil {
					op.StepDetails = &types.StepDetails{}
				}
				op.StepDetails.Result = u.Payload
			}
		case types.OperationActionFail:
			op.Status = types.OperationStatusFailed
			if u.Type == types.OperationTypeStep {
				if op.StepDetails == nil {
					op.StepDetails = &types.StepDetails{}
				}
				op.StepDetails.Error = u.Error
			}
		}

		f.ops[u.ID] = op
		updated = append(updated, op)
	}

	f.token++
	nextToken := fmt.Sprintf("token-%d", f.token)
	return &types.CheckpointDurableExecutionResponse{
		NextCheckpointToken: &nextToken,
		UpdatedOperations:   updated,
	}, nil
}

// snapshot returns a copy of all recorded operations, for building the
// SECOND invocation's InitialExecutionState.Operations.
func (f *fakeClient) snapshot() map[string]types.Operation {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]types.Operation, len(f.ops))
	for k, v := range f.ops {
		out[k] = v
	}
	return out
}

// newLoggingInvocationInput builds a types.DurableExecutionInvocationInput
// matching the confirmed real wire shape (see
// docs/checkpoint-replay-design.md, and
// pkg/durable/durable_test.go's identically-purposed newInvocationInput,
// which this mirrors): the event is always delivered via
// InitialExecutionState.Operations containing a root EXECUTION operation
// whose ExecutionDetails.InputPayload holds the JSON-encoded event.
func newLoggingInvocationInput(t *testing.T, executionID, arn, checkpointToken string, event ReportEvent, extraOps ...types.Operation) types.DurableExecutionInvocationInput {
	t.Helper()
	b, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshaling event: %v", err)
	}
	payload := string(b)

	ops := append([]types.Operation{{
		ID:     executionID,
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusStarted,
		ExecutionDetails: &types.ExecutionDetails{
			InputPayload: &payload,
		},
	}}, extraOps...)

	return types.DurableExecutionInvocationInput{
		DurableExecutionArn:   arn,
		CheckpointToken:       checkpointToken,
		InitialExecutionState: types.InitialExecutionState{Operations: ops},
	}
}

// TestLogging_ReplaySkipSuppressesThroughCompletedSteps drives this
// example's ACTUAL handler through two real, separate invocations of
// durable.WithDurableExecution (constructed directly with a
// recordingLogger, exactly as main.go wires up a real LoggerConfig - see
// that file's Config literal) and confirms:
//
//  1. On the FIRST (real) invocation, every dc.Logger()/sc.Logger() call
//     the handler makes is recorded, in order.
//  2. On the SECOND invocation - a full replay, since BOTH steps are
//     already checkpointed Succeeded by the time this runs - EVERY log
//     call is suppressed, because there is no "next incomplete
//     operation" for this invocation to reach at all (matching the
//     confirmed official reference doc's own worked example - see
//     handler.go's doc, and
//     TestLogging_ReplaySkipSuppressed_SingleStepFullyReplaySkips in the
//     internal reference test for the identical single-step case this
//     generalizes to two sequential steps).
func TestLogging_ReplaySkipSuppressesThroughCompletedSteps(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: true},
	}
	entry := durable.WithDurableExecution(handler, cfg)

	firstInput := newLoggingInvocationInput(t, "exec-log-1", "arn:test:log-1", "token-0", ReportEvent{ReportID: "report-1"})
	out1, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("first invocation error: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		msg := "<nil>"
		if out1.Error != nil {
			msg = out1.Error.ErrorMessage
		}
		t.Fatalf("expected first invocation SUCCEEDED, got %s (%s)", out1.Status, msg)
	}

	firstMsgs := logger.messages()
	wantFirst := []string{
		"handler started",
		"about to gather data",
		"gathering data",
		"finished gathering data",
		"about to summarize data",
		"summarizing data",
		"finished summarizing data",
		"handler completed",
	}
	if len(firstMsgs) != len(wantFirst) {
		t.Fatalf("first invocation: expected %d messages %v, got %d: %v", len(wantFirst), wantFirst, len(firstMsgs), firstMsgs)
	}
	for i, m := range wantFirst {
		if firstMsgs[i] != m {
			t.Fatalf("first invocation: expected message %d to be %q, got %q (full: %v)", i, m, firstMsgs[i], firstMsgs)
		}
	}

	// Second invocation ("replay"): seed InitialExecutionState with every
	// operation checkpointed by the first invocation, exactly as a real
	// re-invocation would receive them. Both steps are already
	// Succeeded, so this handler's ENTIRE execution replay-skips - there
	// is no incomplete operation anywhere in it for this invocation to
	// reach, so every log call the handler makes is expected to be
	// suppressed.
	snapshot := client.snapshot()
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newLoggingInvocationInput(t, "exec-log-1", "arn:test:log-1", "token-1", ReportEvent{ReportID: "report-1"}, extraOps...)

	logger.calls = nil // reset before the second invocation
	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation error: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		msg := "<nil>"
		if out2.Error != nil {
			msg = out2.Error.ErrorMessage
		}
		t.Fatalf("expected second invocation SUCCEEDED (replay-skip, not a re-failure), got %s (%s)", out2.Status, msg)
	}

	secondMsgs := logger.messages()
	if len(secondMsgs) != 0 {
		t.Fatalf("expected EVERY log call to be suppressed on a full replay (no incomplete operation to reach), got %v", secondMsgs)
	}
}

// TestLogging_ModeAwareFalseNeverSuppresses is the direct counterpart to
// the suppression test above: with ModeAware explicitly false, a replay
// of the SAME fully-completed execution must NOT suppress any log
// calls - confirming suppression is genuinely OPT-IN behavior gated by
// this one config field, not an unconditional replay-detection side
// effect a caller has no way to turn off. See
// types.LoggerConfig.ModeAware's own doc for the Go-specific default
// nuance this deliberately sidesteps by setting the field explicitly
// either way, rather than relying on any default.
func TestLogging_ModeAwareFalseNeverSuppresses(t *testing.T) {
	client := newFakeClient()
	logger := newRecordingLogger()

	cfg := &durable.Config{
		Client:       client,
		LoggerConfig: &types.LoggerConfig{CustomLogger: logger, ModeAware: false},
	}
	entry := durable.WithDurableExecution(handler, cfg)

	firstInput := newLoggingInvocationInput(t, "exec-log-2", "arn:test:log-2", "token-0", ReportEvent{ReportID: "report-2"})
	if _, err := entry(context.Background(), firstInput); err != nil {
		t.Fatalf("first invocation error: %v", err)
	}

	snapshot := client.snapshot()
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newLoggingInvocationInput(t, "exec-log-2", "arn:test:log-2", "token-1", ReportEvent{ReportID: "report-2"}, extraOps...)

	logger.calls = nil
	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation error: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second invocation SUCCEEDED, got %s", out2.Status)
	}

	// With ModeAware: false, the DurableContext-level log calls
	// ("handler started", "about to gather data", "finished gathering
	// data", "about to summarize data", "finished summarizing data",
	// "handler completed" - 6 calls) are all still emitted on replay,
	// since suppression itself is disabled. The two StepContext-level
	// calls ("gathering data", "summarizing data") are NOT among them,
	// but for a completely different reason than suppression: both
	// steps replay-skip (their checkpointed result is already
	// Succeeded), so fn itself - and therefore sc.Logger() - never runs
	// again at all, exactly like it wouldn't on any replay regardless
	// of ModeAware (see types.StepContext.Logger's own doc: "there is
	// no such thing as a StepContext during a replay-skip"). This
	// test's point is narrower than "replay reruns everything": it's
	// that ModeAware: false stops SUPPRESSING the calls that DO run on
	// a replay invocation (the DurableContext-level ones sitting between
	// operations), not that it forces step bodies to re-execute, which
	// no LoggerConfig setting does or should.
	secondMsgs := logger.messages()
	if len(secondMsgs) != 6 {
		t.Fatalf("expected all 6 DurableContext-level log calls to be emitted (ModeAware: false disables suppression), got %d: %v", len(secondMsgs), secondMsgs)
	}
}

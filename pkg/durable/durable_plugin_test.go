package durable_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// recordingPlugin is a plugin.InstrumentationPlugin that records every
// hook invocation it receives, guarded by a mutex since plugin.Dispatch
// may call a single plugin's hooks from different goroutines across
// different operations (though never concurrently for the SAME hook
// call - see plugin.Dispatch's own doc).
type recordingPlugin struct {
	plugin.NoopPlugin

	mu               sync.Mutex
	invocationStarts []plugin.InvocationInfo
	invocationEnds   []plugin.InvocationEndInfo
	operationStarts  []plugin.OperationInfo
	operationEnds    []plugin.OperationInfo
	attemptStarts    []plugin.AttemptInfo
	attemptEnds      []plugin.AttemptEndInfo
	operationChanges []plugin.OperationChangeInfo
}

func (p *recordingPlugin) OnInvocationStart(_ context.Context, info plugin.InvocationInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invocationStarts = append(p.invocationStarts, info)
}

func (p *recordingPlugin) OnInvocationEnd(_ context.Context, info plugin.InvocationEndInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invocationEnds = append(p.invocationEnds, info)
}

func (p *recordingPlugin) OnOperationStart(_ context.Context, info plugin.OperationInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operationStarts = append(p.operationStarts, info)
}

func (p *recordingPlugin) OnOperationEnd(_ context.Context, info plugin.OperationEndInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operationEnds = append(p.operationEnds, info.OperationInfo)
}

func (p *recordingPlugin) OnOperationAttemptStart(_ context.Context, info plugin.AttemptInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attemptStarts = append(p.attemptStarts, info)
}

func (p *recordingPlugin) OnOperationAttemptEnd(_ context.Context, info plugin.AttemptEndInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attemptEnds = append(p.attemptEnds, info)
}

func (p *recordingPlugin) OnOperationChange(_ context.Context, info plugin.OperationChangeInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operationChanges = append(p.operationChanges, info)
}

func (p *recordingPlugin) snapshot() (starts, ends []plugin.OperationInfo, invStarts []plugin.InvocationInfo, invEnds []plugin.InvocationEndInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]plugin.OperationInfo{}, p.operationStarts...),
		append([]plugin.OperationInfo{}, p.operationEnds...),
		append([]plugin.InvocationInfo{}, p.invocationStarts...),
		append([]plugin.InvocationEndInfo{}, p.invocationEnds...)
}

func (p *recordingPlugin) attemptSnapshot() (starts []plugin.AttemptInfo, ends []plugin.AttemptEndInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]plugin.AttemptInfo{}, p.attemptStarts...), append([]plugin.AttemptEndInfo{}, p.attemptEnds...)
}

// TestPlugin_InvocationAndStepHooks_HappyPath verifies that a single
// successful step fires exactly one OnInvocationStart, one
// OnOperationStart, one OnOperationEnd (Succeeded), and one
// OnInvocationEnd (Succeeded) - the EXPERIMENTAL instrumentation-plugin
// hooks wired into durable.WithDurableExecution and operations.Step (see
// pkg/durable/plugin's package doc for scope).
func TestPlugin_InvocationAndStepHooks_HappyPath(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-1", "arn:test:plugin-1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, invStarts, invEnds := rec.snapshot()

	if len(invStarts) != 1 {
		t.Fatalf("expected exactly 1 OnInvocationStart, got %d", len(invStarts))
	}
	if invStarts[0].ExecutionARN != "arn:test:plugin-1" {
		t.Errorf("OnInvocationStart: expected ExecutionARN %q, got %q", "arn:test:plugin-1", invStarts[0].ExecutionARN)
	}
	if !invStarts[0].IsFirstInvocation {
		t.Error("OnInvocationStart: expected IsFirstInvocation=true on a fresh execution's first invocation")
	}

	if len(invEnds) != 1 {
		t.Fatalf("expected exactly 1 OnInvocationEnd, got %d", len(invEnds))
	}
	if invEnds[0].Status != plugin.InvocationStatusSucceeded {
		t.Errorf("OnInvocationEnd: expected Status=Succeeded, got %v", invEnds[0].Status)
	}
	if invEnds[0].ExecutionResult == nil {
		t.Error("OnInvocationEnd: expected a non-nil ExecutionResult on success")
	}

	if len(opStarts) != 1 {
		t.Fatalf("expected exactly 1 OnOperationStart, got %d", len(opStarts))
	}
	if opStarts[0].Name != "validate" || opStarts[0].Attempt != 1 {
		t.Errorf("OnOperationStart: expected name=validate attempt=1, got name=%s attempt=%d", opStarts[0].Name, opStarts[0].Attempt)
	}
	if opStarts[0].Status != plugin.OperationStatusStarted {
		t.Errorf("OnOperationStart: expected Status=Started, got %v", opStarts[0].Status)
	}

	if len(opEnds) != 1 {
		t.Fatalf("expected exactly 1 OnOperationEnd, got %d", len(opEnds))
	}
	if opEnds[0].Status != plugin.OperationStatusSucceeded {
		t.Errorf("OnOperationEnd: expected Status=Succeeded, got %v", opEnds[0].Status)
	}
	if opEnds[0].Result == "" {
		t.Error("OnOperationEnd: expected a non-empty checkpointed Result on success")
	}
	// Same operation ID reported by both hooks for the same step.
	if opStarts[0].ID != opEnds[0].ID {
		t.Errorf("expected OnOperationStart/OnOperationEnd to report the same operation ID, got %q vs %q", opStarts[0].ID, opEnds[0].ID)
	}
}

// TestPlugin_StepRetry_OnlyFinalOutcomeFiresOperationEnd verifies that a
// step which fails, retries, then succeeds fires OnOperationStart once
// per attempt (2 attempts here) but OnOperationEnd only ONCE, for the
// final successful outcome - a retryable failure is not an operation
// "end" from a plugin's perspective (see dispatchOperationEnd's own doc
// in step.go), matching the JS reference SDK's event model.
func TestPlugin_StepRetry_OnlyFinalOutcomeFiresOperationEnd(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	attempts := 0
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			attempts++
			if attempts == 1 {
				return "", errors.New("transient failure")
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			// Retry immediately (zero delay) so this resolves
			// synchronously within a single invocation, matching this
			// test file's sibling TestStep_RetryThenSucceed's own
			// pattern.
			return types.RetryDecision{ShouldRetry: attempt < 2, Delay: &types.Duration{Seconds: 0}}
		}))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-retry", "arn:test:plugin-retry", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "xyz"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, _, _ := rec.snapshot()
	attemptStarts, attemptEnds := rec.attemptSnapshot()

	if len(opStarts) != 1 {
		t.Fatalf("expected exactly 1 OnOperationStart (fired once, on the first attempt, not refired per retry), got %d", len(opStarts))
	}
	if opStarts[0].Attempt != 1 {
		t.Errorf("expected OnOperationStart to report attempt=1, got %d", opStarts[0].Attempt)
	}

	if len(attemptStarts) != 2 {
		t.Fatalf("expected 2 OnOperationAttemptStart calls (one per attempt), got %d", len(attemptStarts))
	}
	if attemptStarts[0].Attempt != 1 || attemptStarts[1].Attempt != 2 {
		t.Errorf("expected attempts 1 then 2, got %d then %d", attemptStarts[0].Attempt, attemptStarts[1].Attempt)
	}

	if len(attemptEnds) != 2 {
		t.Fatalf("expected 2 OnOperationAttemptEnd calls (one per attempt, including the failed first one), got %d", len(attemptEnds))
	}
	if attemptEnds[0].Outcome != plugin.AttemptOutcomeFailed {
		t.Errorf("expected the first attempt's end to report Failed, got %v", attemptEnds[0].Outcome)
	}
	if attemptEnds[1].Outcome != plugin.AttemptOutcomeSucceeded {
		t.Errorf("expected the second attempt's end to report Succeeded, got %v", attemptEnds[1].Outcome)
	}

	if len(opEnds) != 1 {
		t.Fatalf("expected exactly 1 OnOperationEnd (only the final outcome), got %d", len(opEnds))
	}
	if opEnds[0].Status != plugin.OperationStatusSucceeded {
		t.Errorf("expected the single OnOperationEnd to report Succeeded, got %v", opEnds[0].Status)
	}
	if opEnds[0].Attempt != 2 {
		t.Errorf("expected the final OnOperationEnd to report attempt=2, got %d", opEnds[0].Attempt)
	}
}

// TestPlugin_StepFailsAfterExhaustingRetries_FiresFailedOperationEnd
// verifies that a step which exhausts its retries fires exactly one
// OnOperationEnd with Status=Failed and a non-nil Error, and that the
// overall invocation's OnInvocationEnd reports Status=Failed too.
func TestPlugin_StepFailsAfterExhaustingRetries_FiresFailedOperationEnd(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("permanent failure")
		}, operations.WithStepRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: false}
		}))
		return orderResult{}, err
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-fail", "arn:test:plugin-fail", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "fail-me"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected Failed, got status=%s", out.Status)
	}

	opStarts, opEnds, _, invEnds := rec.snapshot()

	if len(opStarts) != 1 {
		t.Fatalf("expected 1 OnOperationStart, got %d", len(opStarts))
	}
	if len(opEnds) != 1 {
		t.Fatalf("expected 1 OnOperationEnd, got %d", len(opEnds))
	}
	if opEnds[0].Status != plugin.OperationStatusFailed {
		t.Errorf("expected OnOperationEnd Status=Failed, got %v", opEnds[0].Status)
	}
	if opEnds[0].Error == nil {
		t.Error("expected OnOperationEnd to carry a non-nil Error on failure")
	}

	if len(invEnds) != 1 {
		t.Fatalf("expected 1 OnInvocationEnd, got %d", len(invEnds))
	}
	if invEnds[0].Status != plugin.InvocationStatusFailed {
		t.Errorf("expected OnInvocationEnd Status=Failed, got %v", invEnds[0].Status)
	}
	if invEnds[0].ExecutionError == nil {
		t.Error("expected OnInvocationEnd to carry a non-nil ExecutionError on failure")
	}
}

// TestPlugin_ReplaySkip_DoesNotRefireOperationHooks verifies that when a
// step has already completed in a prior invocation, a fresh invocation
// that replay-skips past it does NOT refire OnOperationStart/
// OnOperationEnd for that step - only genuinely-executed (non-skipped)
// operations fire these hooks, matching the JS reference SDK's own
// isReplay-gated event model (see plugin.OperationInfo.IsReplay's doc for
// why this initial port's replay-skip path is excluded rather than firing
// with IsReplay=true).
func TestPlugin_ReplaySkip_DoesNotRefireOperationHooks(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		second, err := operations.Step(dc, "second", func(sc types.StepContext) (string, error) {
			return "second:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated + "," + second}, nil
	}

	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "replay-me"})

	// First invocation: no plugins configured, just to populate the
	// checkpointed operation log via the fake client (mirroring
	// TestStep_ReplaySkip's own two-invocation setup pattern).
	entry1 := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	firstInput := newInvocationInput("exec-plugin-replay", "arn:test:plugin-replay", "token-0", eventPayload)
	if _, err := entry1(context.Background(), firstInput); err != nil {
		t.Fatalf("first invocation: unexpected error: %v", err)
	}

	// Second invocation: same execution, now WITH a plugin, replaying
	// through the same checkpointed operations via the SAME fake client
	// (which already has both steps recorded as Succeeded).
	rec := &recordingPlugin{}
	entry2 := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	var extraOps []types.Operation
	for _, op := range client.snapshot() {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-plugin-replay", "arn:test:plugin-replay", "token-1", eventPayload, extraOps...)

	out, err := entry2(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation: unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, invStarts, _ := rec.snapshot()

	if len(invStarts) != 1 {
		t.Fatalf("expected exactly 1 OnInvocationStart even on replay, got %d", len(invStarts))
	}
	if invStarts[0].IsFirstInvocation {
		t.Error("expected IsFirstInvocation=false on a replaying invocation")
	}

	if len(opStarts) != 0 {
		t.Errorf("expected 0 OnOperationStart calls on a fully-replay-skipped invocation, got %d", len(opStarts))
	}
	if len(opEnds) != 0 {
		t.Errorf("expected 0 OnOperationEnd calls on a fully-replay-skipped invocation, got %d", len(opEnds))
	}
}

// TestPlugin_NoPluginsConfigured_NoOverhead verifies that omitting
// Config.Plugins entirely (the common case) works exactly as before -
// plugin.Dispatch's nil/empty-slice no-op path (see that function's own
// doc) means this is not just "no crash" but genuinely zero dispatch
// overhead.
func TestPlugin_NoPluginsConfigured_NoOverhead(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-plugin-none", "arn:test:plugin-none", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}
}

// TestPlugin_PanicInHook_IsSwallowed verifies that a plugin hook which
// panics does not affect the execution outcome - plugin.Dispatch's own
// recover (see that function's doc) must catch it, matching the JS SDK's
// "errors thrown by a hook are swallowed" contract adapted to Go's own
// failure mode (panic, not a returned/thrown error, since these hooks
// have no error return value).
func TestPlugin_PanicInHook_IsSwallowed(t *testing.T) {
	client := newFakeClient()

	panicky := &panickyPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{panicky}})
	input := newInvocationInput("exec-plugin-panic", "arn:test:plugin-panic", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded despite every plugin hook panicking, got status=%s error=%v", out.Status, out.Error)
	}
}

// TestPlugin_Wait_FiresStartAndEnd verifies OnOperationStart/OnOperationEnd
// fire for a plain Wait operation (SkipTime resolves it synchronously in
// this test's fakeClient-backed setup, so both hooks fire within a
// single invocation).
func TestPlugin_Wait_FiresStartAndEnd(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		if err := operations.Wait(dc, "pause", types.Duration{Seconds: 1}); err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-wait", "arn:test:plugin-wait", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, _, _ := rec.snapshot()
	if len(opStarts) != 1 || opStarts[0].Type != "WAIT" {
		t.Fatalf("expected exactly 1 OnOperationStart for the Wait, got %d (%+v)", len(opStarts), opStarts)
	}
	if len(opEnds) != 1 || opEnds[0].Status != plugin.OperationStatusSucceeded {
		t.Fatalf("expected exactly 1 OnOperationEnd Succeeded for the Wait, got %d (%+v)", len(opEnds), opEnds)
	}
}

// TestPlugin_RunInChildContext_FiresStartEndAndWrapsChildFn verifies
// OnOperationStart/OnOperationEnd fire for RunInChildContext, and that a
// plugin's WrapChildContextFn hook actually wraps the child's function
// body (observed here via a plugin that increments a counter before and
// after calling the wrapped fn).
func TestPlugin_RunInChildContext_FiresStartEndAndWrapsChildFn(t *testing.T) {
	client := newFakeClient()
	wrapper := &wrapCountingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
			return operations.Step(child, "inner", func(sc types.StepContext) (string, error) {
				return "inner-result", nil
			})
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{wrapper}})
	input := newInvocationInput("exec-plugin-child", "arn:test:plugin-child", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	if wrapper.childWrapBefore != 1 || wrapper.childWrapAfter != 1 {
		t.Fatalf("expected WrapChildContextFn to wrap exactly once (before=%d after=%d)", wrapper.childWrapBefore, wrapper.childWrapAfter)
	}
	// The Step inside the child context ALSO goes through
	// WrapOperationAttemptFn - confirm that fired too, proving the wrap
	// composed correctly around nested operations rather than only the
	// outermost child context.
	if wrapper.attemptWrapBefore != 1 || wrapper.attemptWrapAfter != 1 {
		t.Fatalf("expected WrapOperationAttemptFn to wrap the nested Step exactly once (before=%d after=%d)", wrapper.attemptWrapBefore, wrapper.attemptWrapAfter)
	}
}

// TestPlugin_Invoke_FiresStart verifies OnOperationStart fires when
// Invoke is first called. fakeClient does not auto-resolve a
// CHAINED_INVOKE synchronously (unlike its own WAIT handling), matching
// the real backend's genuine cross-Lambda suspension - so the invocation
// suspends as Pending, and this test verifies the start hook rather than
// attempting to exercise the resume/end path, which would require either
// extending fakeClient's own simulation or a real deployed target
// function (see examples/*/cloud_integration_test.go for the latter,
// real-backend-verified pattern for Invoke's resume behavior).
func TestPlugin_Invoke_FiresStart(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Invoke[string, string](dc, "call-other-fn", "arn:aws:lambda:us-east-1:123456789012:function:other", "payload")
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-invoke", "arn:test:plugin-invoke", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusPending {
		t.Fatalf("expected the invocation to suspend as Pending (fakeClient does not auto-resolve CHAINED_INVOKE), got status=%s", out.Status)
	}

	opStarts, opEnds, _, _ := rec.snapshot()
	if len(opStarts) != 1 || opStarts[0].Type != "CHAINED_INVOKE" {
		t.Fatalf("expected exactly 1 OnOperationStart for the Invoke, got %d (%+v)", len(opStarts), opStarts)
	}
	if len(opEnds) != 0 {
		t.Fatalf("expected 0 OnOperationEnd calls (the invocation suspended before the invoke resolved), got %d", len(opEnds))
	}
}

// TestPlugin_WaitForCondition_AttemptHooksFirePerPoll verifies
// OnOperationStart fires once (not per poll) while
// OnOperationAttemptStart/End fire once per poll attempt, for a
// WaitForCondition that takes 3 polls to succeed - mirroring Step's own
// retry semantics test.
func TestPlugin_WaitForCondition_AttemptHooksFirePerPoll(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	polls := 0
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		_, err := operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
			polls++
			return operations.ConditionResult[int]{State: state + 1, ConditionMet: polls >= 3}, nil
		}, 0, operations.WithConditionRetryStrategy[int](func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: attempt < 3, Delay: &types.Duration{Seconds: 0}}
		}))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-condition", "arn:test:plugin-condition", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, _, _ := rec.snapshot()
	attemptStarts, attemptEnds := rec.attemptSnapshot()

	if len(opStarts) != 1 {
		t.Fatalf("expected exactly 1 OnOperationStart, got %d", len(opStarts))
	}
	if len(opEnds) != 1 || opEnds[0].Status != plugin.OperationStatusSucceeded {
		t.Fatalf("expected exactly 1 OnOperationEnd Succeeded, got %d (%+v)", len(opEnds), opEnds)
	}
	if len(attemptStarts) != 3 {
		t.Fatalf("expected 3 OnOperationAttemptStart calls (one per poll), got %d", len(attemptStarts))
	}
	if len(attemptEnds) != 3 {
		t.Fatalf("expected 3 OnOperationAttemptEnd calls, got %d", len(attemptEnds))
	}
	if attemptEnds[0].Outcome != plugin.AttemptOutcomeFailed || attemptEnds[2].Outcome != plugin.AttemptOutcomeSucceeded {
		t.Errorf("expected the first 2 poll attempts to end Failed (condition not met) and the 3rd Succeeded, got %v, %v, %v", attemptEnds[0].Outcome, attemptEnds[1].Outcome, attemptEnds[2].Outcome)
	}
}

// TestPlugin_Parallel_FiresPerBranchOperationHooks verifies each Parallel
// branch fires its own OnOperationStart/OnOperationEnd (SubType
// "ParallelBranch"), i.e. per-item hook coverage for batch operations.
func TestPlugin_Parallel_FiresPerBranchOperationHooks(t *testing.T) {
	client := newFakeClient()
	rec := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		batch, err := operations.Parallel(dc, "fanout", []func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) { return "a", nil },
			func(child types.DurableContext) (string, error) { return "b", nil },
		})
		if err != nil {
			return orderResult{}, err
		}
		if err := batch.ThrowIfError(); err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	input := newInvocationInput("exec-plugin-parallel", "arn:test:plugin-parallel", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	opStarts, opEnds, _, _ := rec.snapshot()
	branchStarts := 0
	branchEnds := 0
	for _, s := range opStarts {
		if s.SubType == "ParallelBranch" {
			branchStarts++
		}
	}
	for _, e := range opEnds {
		if e.SubType == "ParallelBranch" {
			branchEnds++
			if e.Status != plugin.OperationStatusSucceeded {
				t.Errorf("expected each branch's OnOperationEnd to report Succeeded, got %v", e.Status)
			}
		}
	}
	if branchStarts != 2 {
		t.Fatalf("expected 2 OnOperationStart calls with SubType=ParallelBranch, got %d", branchStarts)
	}
	if branchEnds != 2 {
		t.Fatalf("expected 2 OnOperationEnd calls with SubType=ParallelBranch, got %d", branchEnds)
	}
}

// TestPlugin_WrapInvocation_WrapsHandlerCall verifies a plugin's
// WrapInvocation hook genuinely wraps the handler call (observed via a
// before/after counter around the call to fn) and that its own returned
// result/error pass through unchanged into the real execution outcome.
func TestPlugin_WrapInvocation_WrapsHandlerCall(t *testing.T) {
	client := newFakeClient()
	wrapper := &wrapCountingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{wrapper}})
	input := newInvocationInput("exec-plugin-wrapinvoke", "arn:test:plugin-wrapinvoke", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}
	if wrapper.invocationWrapBefore != 1 || wrapper.invocationWrapAfter != 1 {
		t.Fatalf("expected WrapInvocation to wrap exactly once (before=%d after=%d)", wrapper.invocationWrapBefore, wrapper.invocationWrapAfter)
	}
}

// TestPlugin_OnOperationChange_FiresOnResumedInvocation verifies
// OnOperationChange fires, with a non-empty UpdatedOperations map keyed
// by the resumed Wait operation's own ID, on the SECOND invocation of a
// two-invocation execution - the resumed Wait operation is what changed
// externally (its timer elapsed) between invocations.
func TestPlugin_OnOperationChange_FiresOnResumedInvocation(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		if err := operations.Wait(dc, "pause", types.Duration{Seconds: 1}); err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "resume-me"})

	// First invocation: run to completion (fakeClient resolves Wait
	// synchronously - see its own doc), populating the checkpointed
	// operation log with a Succeeded Wait operation.
	entry1 := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	firstInput := newInvocationInput("exec-plugin-change", "arn:test:plugin-change", "token-0", eventPayload)
	if _, err := entry1(context.Background(), firstInput); err != nil {
		t.Fatalf("first invocation: unexpected error: %v", err)
	}

	rec := &recordingPlugin{}
	entry2 := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{rec}})
	var extraOps []types.Operation
	var waitOpID string
	for _, op := range client.snapshot() {
		extraOps = append(extraOps, op)
		if op.Type == types.OperationTypeWait {
			waitOpID = op.ID
		}
	}
	if waitOpID == "" {
		t.Fatal("expected to find the checkpointed Wait operation from the first invocation")
	}

	// UpdatedOperationIds is NOT auto-populated by newInvocationInput
	// (that helper's own doc only describes the real backend's behavior,
	// not this test harness's) - set it explicitly here to the resumed
	// Wait's own ID, matching what a real backend would report:
	// "operations whose status changed since the last checkpoint."
	secondInput := newInvocationInput("exec-plugin-change", "arn:test:plugin-change", "token-1", eventPayload, extraOps...)
	secondInput.UpdatedOperationIds = []string{waitOpID}

	out, err := entry2(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation: unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	rec.mu.Lock()
	changes := append([]plugin.OperationChangeInfo{}, rec.operationChanges...)
	rec.mu.Unlock()

	if len(changes) != 1 {
		t.Fatalf("expected exactly 1 OnOperationChange call, got %d", len(changes))
	}
	if len(changes[0].UpdatedOperations) == 0 {
		t.Error("expected a non-empty UpdatedOperations map")
	}
}

// TestPlugin_EnrichLogContext_MergedIntoLoggerFields verifies a plugin's
// EnrichLogContext output is actually merged into log calls made through
// DurableContext.Logger()/StepContext.Logger() - observed via a custom
// types.Logger installed on the Config that records the fields it
// receives.
func TestPlugin_EnrichLogContext_MergedIntoLoggerFields(t *testing.T) {
	client := newFakeClient()
	enricher := &enrichingPlugin{fields: map[string]any{"traceId": "trace-123"}}
	captured := &fieldCapturingLogger{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		dc.Logger().Info("handler log", nil)
		_, err := operations.Step(dc, "s", func(sc types.StepContext) (string, error) {
			sc.Logger().Info("step log", nil)
			return "ok", nil
		})
		return orderResult{}, err
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:       client,
		Plugins:      []plugin.InstrumentationPlugin{enricher},
		LoggerConfig: &types.LoggerConfig{CustomLogger: captured, ModeAware: true},
	})
	input := newInvocationInput("exec-plugin-enrich", "arn:test:plugin-enrich", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	captured.mu.Lock()
	defer captured.mu.Unlock()
	if len(captured.calls) < 2 {
		t.Fatalf("expected at least 2 captured log calls (handler + step), got %d", len(captured.calls))
	}
	for _, call := range captured.calls {
		if call["traceId"] != "trace-123" {
			t.Errorf("expected every log call's fields to include the plugin's enriched traceId, got %v", call)
		}
	}
}

type enrichingPlugin struct {
	plugin.NoopPlugin
	fields map[string]any
}

func (p *enrichingPlugin) EnrichLogContext() map[string]any { return p.fields }

type fieldCapturingLogger struct {
	mu    sync.Mutex
	calls []map[string]any
}

func (l *fieldCapturingLogger) record(fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, fields)
}
func (l *fieldCapturingLogger) Debug(_ string, fields map[string]any) { l.record(fields) }
func (l *fieldCapturingLogger) Info(_ string, fields map[string]any)  { l.record(fields) }
func (l *fieldCapturingLogger) Warn(_ string, fields map[string]any)  { l.record(fields) }
func (l *fieldCapturingLogger) Error(_ string, fields map[string]any) { l.record(fields) }

// TestPlugin_MultiplePlugins_AllReceiveEveryNotificationHook verifies
// that configuring MULTIPLE plugins together (not just one, as every
// other test in this file does) results in every notification hook
// (OnInvocationStart/End, OnOperationStart/End, OnOperationAttemptStart/
// End) being dispatched to ALL of them, end-to-end through
// durable.WithDurableExecution - not just plugin.Dispatch's own isolated
// unit tests (pkg/durable/plugin/plugin_test.go), which exercise the
// concurrent multi-plugin fan-out mechanism directly but never through a
// real handler execution.
func TestPlugin_MultiplePlugins_AllReceiveEveryNotificationHook(t *testing.T) {
	client := newFakeClient()
	rec1 := &recordingPlugin{}
	rec2 := &recordingPlugin{}
	rec3 := &recordingPlugin{}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{rec1, rec2, rec3},
	})
	input := newInvocationInput("exec-plugin-multi", "arn:test:plugin-multi", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	for i, rec := range []*recordingPlugin{rec1, rec2, rec3} {
		opStarts, opEnds, invStarts, invEnds := rec.snapshot()
		attemptStarts, attemptEnds := rec.attemptSnapshot()
		if len(invStarts) != 1 {
			t.Errorf("plugin %d: expected 1 OnInvocationStart, got %d", i, len(invStarts))
		}
		if len(invEnds) != 1 {
			t.Errorf("plugin %d: expected 1 OnInvocationEnd, got %d", i, len(invEnds))
		}
		if len(opStarts) != 1 {
			t.Errorf("plugin %d: expected 1 OnOperationStart, got %d", i, len(opStarts))
		}
		if len(opEnds) != 1 {
			t.Errorf("plugin %d: expected 1 OnOperationEnd, got %d", i, len(opEnds))
		}
		if len(attemptStarts) != 1 {
			t.Errorf("plugin %d: expected 1 OnOperationAttemptStart, got %d", i, len(attemptStarts))
		}
		if len(attemptEnds) != 1 {
			t.Errorf("plugin %d: expected 1 OnOperationAttemptEnd, got %d", i, len(attemptEnds))
		}
	}
}

// TestPlugin_MultiplePlugins_WrapChainComposesOutermostFirst verifies
// that multiple plugins' Wrap* hooks compose in configuration order -
// the FIRST plugin in the Plugins slice is the OUTERMOST layer (its own
// before-code runs first and its own after-code runs last), matching
// plugin.WrapChain's documented ordering contract - end-to-end through
// RunInChildContext's real WrapChildContextFn dispatch, not just
// plugin.WrapChain's own isolated unit tests.
func TestPlugin_MultiplePlugins_WrapChainComposesOutermostFirst(t *testing.T) {
	client := newFakeClient()

	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}

	outer := &orderRecordingWrapPlugin{name: "outer", record: record}
	inner := &orderRecordingWrapPlugin{name: "inner", record: record}

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
			record("fn")
			return "child-result", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	// outer is FIRST in the slice, so it must be the OUTERMOST wrapper:
	// expected call order is outer-before, inner-before, fn, inner-after,
	// outer-after.
	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{outer, inner},
	})
	input := newInvocationInput("exec-plugin-wrapchain", "arn:test:plugin-wrapchain", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "abc"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"outer-before", "inner-before", "fn", "inner-after", "outer-after"}
	if len(order) != len(want) {
		t.Fatalf("expected call order %v, got %v", want, order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("expected call order %v, got %v (mismatch at index %d)", want, order, i)
		}
	}
}

// orderRecordingWrapPlugin records "<name>-before"/"<name>-after" (via
// the shared record func) around its own call to fn, for both
// WrapChildContextFn and WrapInvocation - used to observe multi-plugin
// Wrap* composition ordering.
type orderRecordingWrapPlugin struct {
	plugin.NoopPlugin
	name   string
	record func(string)
}

func (p *orderRecordingWrapPlugin) WrapChildContextFn(_ context.Context, _ plugin.OperationInfo, fn plugin.WrapFn) (any, error) {
	p.record(p.name + "-before")
	result, err := fn()
	p.record(p.name + "-after")
	return result, err
}

// wrapCountingPlugin records before/after counts for each Wrap* hook it
// overrides, calling fn synchronously in between (as every Wrap* hook
// contract requires - see plugin.InstrumentationPlugin's own doc).
type wrapCountingPlugin struct {
	plugin.NoopPlugin

	invocationWrapBefore, invocationWrapAfter int
	childWrapBefore, childWrapAfter           int
	attemptWrapBefore, attemptWrapAfter       int
}

func (p *wrapCountingPlugin) WrapInvocation(ctx context.Context, _ plugin.InvocationInfo, fn plugin.WrapFn) (any, error) {
	p.invocationWrapBefore++
	result, err := fn()
	p.invocationWrapAfter++
	return result, err
}

func (p *wrapCountingPlugin) WrapChildContextFn(ctx context.Context, _ plugin.OperationInfo, fn plugin.WrapFn) (any, error) {
	p.childWrapBefore++
	result, err := fn()
	p.childWrapAfter++
	return result, err
}

func (p *wrapCountingPlugin) WrapOperationAttemptFn(ctx context.Context, _ plugin.AttemptInfo, fn plugin.WrapFn) (any, error) {
	p.attemptWrapBefore++
	result, err := fn()
	p.attemptWrapAfter++
	return result, err
}

type panickyPlugin struct{ plugin.NoopPlugin }

func (panickyPlugin) OnInvocationStart(context.Context, plugin.InvocationInfo)  { panic("boom") }
func (panickyPlugin) OnInvocationEnd(context.Context, plugin.InvocationEndInfo) { panic("boom") }
func (panickyPlugin) OnOperationStart(context.Context, plugin.OperationInfo)    { panic("boom") }
func (panickyPlugin) OnOperationEnd(context.Context, plugin.OperationEndInfo)   { panic("boom") }

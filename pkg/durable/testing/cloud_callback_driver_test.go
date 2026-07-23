package testing

// cloud_callback_driver_test.go tests CloudTestRunner.RunUntilCallback/
// Continue and cloudCallbackDriver against hand-written fakes (no real
// AWS credentials or network access) - the same "subset interface,
// exposed for testability" pattern cloud_runner_test.go's own
// fakeLambdaInvoker/fakeStateClient already establish, extended here for
// the async-invoke/callback-driving capability cloud_callback_driver.go
// and cloud_runner.go's RunUntilCallback/Continue/AsyncLambdaInvoker
// added in this same session.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// fakeAsyncLambdaInvoker implements both LambdaInvoker (unused by these
// tests, but required to satisfy CloudTestRunner.Invoker's declared
// field type) and AsyncLambdaInvoker - modeling a durable function whose
// asynchronous Invoke call returns a fixed executionArn.
type fakeAsyncLambdaInvoker struct {
	executionArn   string
	invokeAsyncErr error
	invokeAsyncN   int
}

func (f *fakeAsyncLambdaInvoker) Invoke(_ context.Context, _ string, _ []byte) ([]byte, bool, string, error) {
	return nil, false, "", errors.New("fakeAsyncLambdaInvoker.Invoke: not used by these tests")
}

func (f *fakeAsyncLambdaInvoker) InvokeAsync(_ context.Context, _ string, _ []byte) (string, error) {
	f.invokeAsyncN++
	if f.invokeAsyncErr != nil {
		return "", f.invokeAsyncErr
	}
	return f.executionArn, nil
}

// syncOnlyLambdaInvoker implements ONLY LambdaInvoker, deliberately NOT
// AsyncLambdaInvoker - used to confirm RunUntilCallback's own type-assertion
// guard (cloud_runner.go) fails clearly rather than panicking when an
// Invoker lacks asynchronous-invoke support.
type syncOnlyLambdaInvoker struct{}

func (syncOnlyLambdaInvoker) Invoke(_ context.Context, _ string, _ []byte) ([]byte, bool, string, error) {
	return nil, false, "", errors.New("syncOnlyLambdaInvoker.Invoke: not used by these tests")
}

// fakeCallbackAPI implements CallbackAPI (cloud_callback_driver.go)
// without any real AWS credentials or network access, recording every
// call it receives for assertions.
type fakeCallbackAPI struct {
	successErr error
	failureErr error

	successCalls []*lambda.SendDurableExecutionCallbackSuccessInput
	failureCalls []*lambda.SendDurableExecutionCallbackFailureInput
}

func (f *fakeCallbackAPI) SendDurableExecutionCallbackSuccess(_ context.Context, params *lambda.SendDurableExecutionCallbackSuccessInput, _ ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackSuccessOutput, error) {
	f.successCalls = append(f.successCalls, params)
	if f.successErr != nil {
		return nil, f.successErr
	}
	return &lambda.SendDurableExecutionCallbackSuccessOutput{}, nil
}

func (f *fakeCallbackAPI) SendDurableExecutionCallbackFailure(_ context.Context, params *lambda.SendDurableExecutionCallbackFailureInput, _ ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackFailureOutput, error) {
	f.failureCalls = append(f.failureCalls, params)
	if f.failureErr != nil {
		return nil, f.failureErr
	}
	return &lambda.SendDurableExecutionCallbackFailureOutput{}, nil
}

// TestCloudCallbackDriver_SendCallbackResult_Success confirms
// cloudCallbackDriver.sendCallbackResult calls the real
// SendDurableExecutionCallbackSuccess API shape correctly when result is
// non-nil.
func TestCloudCallbackDriver_SendCallbackResult_Success(t *testing.T) {
	api := &fakeCallbackAPI{}
	driver := &cloudCallbackDriver{api: api}

	result := `"approved"`
	if err := driver.sendCallbackResult("callback-123", &result, nil); err != nil {
		t.Fatalf("sendCallbackResult: %v", err)
	}

	if len(api.successCalls) != 1 {
		t.Fatalf("expected 1 SendDurableExecutionCallbackSuccess call, got %d", len(api.successCalls))
	}
	call := api.successCalls[0]
	if got := *call.CallbackId; got != "callback-123" {
		t.Fatalf("expected CallbackId 'callback-123', got %q", got)
	}
	if got := string(call.Result); got != result {
		t.Fatalf("expected Result %q, got %q", result, got)
	}
	if len(api.failureCalls) != 0 {
		t.Fatalf("expected 0 SendDurableExecutionCallbackFailure calls, got %d", len(api.failureCalls))
	}
}

// TestCloudCallbackDriver_SendCallbackResult_Failure confirms
// cloudCallbackDriver.sendCallbackResult calls the real
// SendDurableExecutionCallbackFailure API shape correctly when errObj is
// non-nil.
func TestCloudCallbackDriver_SendCallbackResult_Failure(t *testing.T) {
	api := &fakeCallbackAPI{}
	driver := &cloudCallbackDriver{api: api}

	errObj := &types.ErrorObject{ErrorMessage: "rejected", ErrorType: "ApprovalRejected"}
	if err := driver.sendCallbackResult("callback-456", nil, errObj); err != nil {
		t.Fatalf("sendCallbackResult: %v", err)
	}

	if len(api.failureCalls) != 1 {
		t.Fatalf("expected 1 SendDurableExecutionCallbackFailure call, got %d", len(api.failureCalls))
	}
	call := api.failureCalls[0]
	if got := *call.CallbackId; got != "callback-456" {
		t.Fatalf("expected CallbackId 'callback-456', got %q", got)
	}
	if got := *call.Error.ErrorMessage; got != "rejected" {
		t.Fatalf("expected ErrorMessage 'rejected', got %q", got)
	}
	if got := *call.Error.ErrorType; got != "ApprovalRejected" {
		t.Fatalf("expected ErrorType 'ApprovalRejected', got %q", got)
	}
}

// TestCloudCallbackDriver_SendCallbackResult_NeitherProvided confirms the
// defensive error path when neither result nor errObj is set - should
// never happen via Operation.SendCallbackSuccess/SendCallbackFailure's
// own public API (operation.go), but sendCallbackResult itself should
// not silently succeed or panic if it somehow did.
func TestCloudCallbackDriver_SendCallbackResult_NeitherProvided(t *testing.T) {
	api := &fakeCallbackAPI{}
	driver := &cloudCallbackDriver{api: api}

	err := driver.sendCallbackResult("callback-789", nil, nil)
	if err == nil {
		t.Fatal("expected an error when neither result nor errObj is provided")
	}
	if len(api.successCalls) != 0 || len(api.failureCalls) != 0 {
		t.Fatal("expected no real API calls when neither result nor errObj is provided")
	}
}

// TestCloudTestRunner_RunUntilCallback_FindsCallbackAndAllowsCompletion is
// the core new-capability test: RunUntilCallback stops polling as soon as
// a CALLBACK operation with a populated CallbackID appears (BEFORE the
// execution reaches a terminal status), returns a PENDING TestResult
// whose Operation handle supports SendCallbackSuccess, and Continue then
// resumes polling to a genuine terminal SUCCEEDED status - mirroring
// examples/map-with-condition-and-callback-go/cloud_integration_test.go's
// own real-backend flow, but entirely against fakes here.
func TestCloudTestRunner_RunUntilCallback_FindsCallbackAndAllowsCompletion(t *testing.T) {
	ctx := context.Background()

	invoker := &fakeAsyncLambdaInvoker{executionArn: "arn:aws:lambda:us-east-1:123456789012:function:f:1/durable-execution/f/exec-1"}

	pendingCallbackOp := types.Operation{
		ID:     "callback-op-1",
		Type:   types.OperationTypeCallback,
		Name:   "batch-approval",
		Status: types.OperationStatusStarted,
		CallbackDetails: &types.CallbackDetails{
			CallbackID: "real-callback-id-1",
		},
	}
	terminalOp := types.Operation{
		ID:     "exec-root",
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusSucceeded,
	}

	state := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{pendingCallbackOp}}, // first poll: callback open, execution not terminal
			{Operations: []types.Operation{terminalOp}},        // after Continue: terminal
		},
	}

	runner := NewCloudTestRunner[map[string]any, map[string]any]("arn:aws:lambda:us-east-1:123456789012:function:f:1", invoker, state)
	runner.PollInterval = 1 * time.Millisecond
	runner.Timeout = 5 * time.Second

	res, resArn, err := runner.RunUntilCallback(ctx, "", map[string]any{"orderIds": []string{"order-1"}}, "batch-approval")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if resArn != invoker.executionArn {
		t.Fatalf("expected returned arn %q, got %q", invoker.executionArn, resArn)
	}
	if res.GetStatus() != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING, got %s", res.GetStatus())
	}
	if invoker.invokeAsyncN != 1 {
		t.Fatalf("expected exactly 1 asynchronous Invoke call, got %d", invoker.invokeAsyncN)
	}

	op, ok := res.GetOperation("batch-approval")
	if !ok {
		t.Fatal("expected to find the 'batch-approval' CALLBACK operation")
	}
	if op.GetType() != types.OperationTypeCallback {
		t.Fatalf("expected CALLBACK type, got %s", op.GetType())
	}

	// SendCallbackSuccess should work against this cloud-produced,
	// mid-flight TestResult exactly like it would against a
	// LocalTestRunner-produced one - this is the whole point of wiring a
	// cloudCallbackDriver into newTestResult's runner parameter. This
	// test deliberately does NOT actually call op.SendCallbackSuccess
	// here (doing so would make a REAL AWS SDK call via
	// NewCallbackDriver's real credential chain, which this fake-based
	// unit test must not depend on) - cloudCallbackDriver's own
	// sendCallbackResult method is independently, fully tested against a
	// fake CallbackAPI above
	// (TestCloudCallbackDriver_SendCallbackResult_Success/Failure).
	// Instead, this test (being in the same `testing` package, not an
	// external `testing_test` package) reaches into Operation's own
	// package-private runner field directly, confirming RunUntilCallback
	// genuinely wired a non-nil, callback-capable driver in - the
	// structural precondition operation.go's own SendCallbackSuccess/
	// SendCallbackFailure nil-runner guard checks before ever reaching
	// cloudCallbackDriver.sendCallbackResult at all.
	if op.runner == nil {
		t.Fatal("expected the 'batch-approval' Operation handle to have a non-nil callback driver wired in by RunUntilCallback")
	}
	if _, ok := op.runner.(*cloudCallbackDriver); !ok {
		t.Fatalf("expected the wired-in callback driver to be a *cloudCallbackDriver, got %T", op.runner)
	}

	final, err := runner.Continue(ctx, "arn:aws:lambda:us-east-1:123456789012:function:f:1/durable-execution/f/exec-1")
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED after Continue, got %s", final.GetStatus())
	}
}

// TestCloudTestRunner_RunUntilCallback_TerminatesWithoutCallback confirms
// RunUntilCallback returns the terminal TestResult as-is (not an error)
// when the execution reaches a terminal status before the named
// CALLBACK operation ever appears - see that method's own doc for why
// this is deliberate, honest behavior rather than a bug.
func TestCloudTestRunner_RunUntilCallback_TerminatesWithoutCallback(t *testing.T) {
	ctx := context.Background()

	invoker := &fakeAsyncLambdaInvoker{executionArn: "arn:aws:lambda:us-east-1:123456789012:function:f:1/durable-execution/f/exec-2"}
	terminalOp := types.Operation{
		ID:     "exec-root",
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusSucceeded,
	}
	state := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{terminalOp}},
		},
	}

	runner := NewCloudTestRunner[map[string]any, map[string]any]("arn:aws:lambda:us-east-1:123456789012:function:f:1", invoker, state)
	runner.PollInterval = 1 * time.Millisecond
	runner.Timeout = 5 * time.Second

	res, _, err := runner.RunUntilCallback(ctx, "", map[string]any{}, "some-callback-that-never-appears")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED (the execution finished without needing the named callback), got %s", res.GetStatus())
	}
}

// TestCloudTestRunner_RunUntilCallback_RequiresAsyncLambdaInvoker confirms
// the type-assertion guard fails clearly (not a panic) when Invoker does
// not implement AsyncLambdaInvoker.
func TestCloudTestRunner_RunUntilCallback_RequiresAsyncLambdaInvoker(t *testing.T) {
	ctx := context.Background()
	state := &fakeStateClient{responses: []types.GetDurableExecutionStateResponse{{}}}

	runner := NewCloudTestRunner[map[string]any, map[string]any]("arn:aws:lambda:us-east-1:123456789012:function:f:1", syncOnlyLambdaInvoker{}, state)

	_, _, err := runner.RunUntilCallback(ctx, "", map[string]any{}, "batch-approval")
	if err == nil {
		t.Fatal("expected an error when Invoker does not implement AsyncLambdaInvoker")
	}
}

// TestCloudTestRunner_Continue_RequiresNonEmptyArn confirms Continue
// rejects an empty arn clearly rather than polling a meaningless target.
func TestCloudTestRunner_Continue_RequiresNonEmptyArn(t *testing.T) {
	ctx := context.Background()
	state := &fakeStateClient{responses: []types.GetDurableExecutionStateResponse{{}}}
	runner := NewCloudTestRunner[map[string]any, map[string]any]("arn:aws:lambda:us-east-1:123456789012:function:f:1", &fakeAsyncLambdaInvoker{}, state)

	_, err := runner.Continue(ctx, "")
	if err == nil {
		t.Fatal("expected an error when arn is empty")
	}
}

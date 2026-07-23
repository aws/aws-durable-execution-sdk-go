package testing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/sigv4lambda"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// Compile-time assertion that sigv4lambda.Client - the package this
// task's briefing explicitly named as having "a working GetExecutionState
// implementation" against the confirmed real API shape - actually
// satisfies checkpoint.GetExecutionStateClient, the interface
// CloudTestRunner depends on. This is a real, load-bearing check (not
// just documentation): if sigv4lambda.Client's method signature ever
// drifts from this interface, this line fails to compile, at build time,
// rather than surfacing as a confusing "does not implement" error only
// where a caller happens to try to use the two together.
var _ checkpoint.GetExecutionStateClient = (*sigv4lambda.Client)(nil)

// fakeLambdaInvoker is a test double for LambdaInvoker (see
// cloud_runner.go), letting CloudTestRunner's tests run without real AWS
// credentials or network access - the same "subset interface, exposed
// for testability" pattern pkg/durable/awssdk/client_test.go's
// fakeLambdaAPI and pkg/durable/durable_fake_client_test.go's fakeClient
// already establish in this repo for the analogous
// CheckpointDurableExecution/full-checkpoint-loop cases.
type fakeLambdaInvoker struct {
	// responsePayload/functionError/executionArn/err are returned
	// verbatim from every Invoke call - sufficient for
	// CloudTestRunner's tests, since Run's own poll loop (not Invoke) is
	// what drives multi-step behavior; a single Invoke call is all
	// CloudTestRunner.Run ever makes per Run call (see cloud_runner.go's
	// doc on why: a synchronous Invoke against a durable function
	// already blocks until the execution is terminal, confirmed via the
	// official invoking-durable-functions guide).
	//
	// executionArn models InvokeOutput.DurableExecutionArn (see
	// lambda_invoker.go's updated doc) - left empty in most existing
	// tests below, which instead exercise Run's caller-supplied-arn path
	// (still fully supported - see cloud_runner.go Run's doc on why a
	// non-empty caller arn takes precedence over whatever Invoke itself
	// returns); TestCloudTestRunner_Run_DiscoversArnFromInvokeResponse
	// is the one test that sets this field, specifically to exercise the
	// OTHER path (arn == "" at the Run call site, falling back to
	// Invoke's own returned ARN).
	responsePayload []byte
	functionError   bool
	executionArn    string
	err             error

	lastFunctionName string
	lastPayload      []byte
	invokeCount      int
}

func (f *fakeLambdaInvoker) Invoke(_ context.Context, functionNameOrARN string, payload []byte) ([]byte, bool, string, error) {
	f.invokeCount++
	f.lastFunctionName = functionNameOrARN
	f.lastPayload = payload
	if f.err != nil {
		return nil, false, "", f.err
	}
	return f.responsePayload, f.functionError, f.executionArn, nil
}

// fakeStateClient is a test double for checkpoint.GetExecutionStateClient,
// letting CloudTestRunner's poll loop be exercised deterministically -
// including the "not terminal yet, keep polling" path - without a real
// backend or real wall-clock waiting.
type fakeStateClient struct {
	// responses is consumed in order, one response per
	// GetExecutionState call; the last entry repeats once exhausted
	// (rather than erroring), so tests can specify only as many distinct
	// polls as they care to distinguish.
	responses []types.GetDurableExecutionStateResponse
	err       error

	calls           int
	lastReq         types.GetDurableExecutionStateRequest
	requestedArns   []string
	requestedTokens []string
}

func (f *fakeStateClient) GetExecutionState(_ context.Context, req types.GetDurableExecutionStateRequest) (*types.GetDurableExecutionStateResponse, error) {
	f.lastReq = req
	f.requestedArns = append(f.requestedArns, req.DurableExecutionArn)
	f.requestedTokens = append(f.requestedTokens, req.CheckpointToken)
	if f.err != nil {
		return nil, f.err
	}
	idx := f.calls
	if idx >= len(f.responses) {
		idx = len(f.responses) - 1
	}
	f.calls++
	resp := f.responses[idx]
	return &resp, nil
}

func succeededExecutionOp() types.Operation {
	return types.Operation{
		ID:     "exec-root",
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusSucceeded,
	}
}

func TestCloudTestRunner_Run_ImmediatelyTerminal_Succeeded(t *testing.T) {
	type event struct {
		OrderID string `json:"orderId"`
	}
	type result struct {
		Greeting string `json:"greeting"`
	}

	respPayload, err := json.Marshal(result{Greeting: "Hello, cloud!"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	invoker := &fakeLambdaInvoker{responsePayload: respPayload}
	stepOp := types.Operation{
		ID:     "1",
		Type:   types.OperationTypeStep,
		Name:   "create-greeting",
		Status: types.OperationStatusSucceeded,
	}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{succeededExecutionOp(), stepOp}},
		},
	}

	runner := NewCloudTestRunner[event, result]("arn:aws:lambda:us-east-1:123456789012:function:my-func:1", invoker, stateClient)
	runner.PollInterval = time.Millisecond

	res, err := runner.Run(context.Background(), "arn:aws:lambda:us-east-1:123456789012:function:my-func:1/durable-execution/exec-1", event{OrderID: "abc"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", res.GetStatus())
	}

	out, err := GetResult[result](res)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Greeting != "Hello, cloud!" {
		t.Fatalf("expected greeting from the Invoke response payload, got %q", out.Greeting)
	}

	op, ok := res.GetOperation("create-greeting")
	if !ok {
		t.Fatal("expected to find the 'create-greeting' step operation in the cloud-polled history")
	}
	if op.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", op.GetType())
	}
	if op.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected step SUCCEEDED, got %s", op.GetStatus())
	}

	if invoker.invokeCount != 1 {
		t.Fatalf("expected exactly 1 Invoke call, got %d", invoker.invokeCount)
	}
	if invoker.lastFunctionName != "arn:aws:lambda:us-east-1:123456789012:function:my-func:1" {
		t.Fatalf("unexpected FunctionName passed to Invoke: %s", invoker.lastFunctionName)
	}
	var gotEvent event
	if err := json.Unmarshal(invoker.lastPayload, &gotEvent); err != nil {
		t.Fatalf("unmarshaling invoked payload: %v", err)
	}
	if gotEvent.OrderID != "abc" {
		t.Fatalf("expected the original event to be marshaled into the Invoke payload, got %+v", gotEvent)
	}
}

// TestCloudTestRunner_Run_DiscoversArnFromInvokeResponse exercises the
// OTHER of the two ARN-sourcing paths Run now supports (see its doc
// comment): a caller with NO pre-known execution ARN (the genuinely
// fresh-invocation case, arn == "" at the call site) still gets a
// correctly-polled TestResult, because Run falls back to the real
// Invoke API's own returned DurableExecutionArn (see
// LambdaInvoker.Invoke's updated doc, and lambda_invoker.go's
// sdkLambdaInvoker.Invoke for the production plumbing this fake
// stands in for) rather than requiring the caller to have already
// discovered the ARN some other way. Every other test in this file
// exercises the ORIGINAL, still-fully-supported caller-supplied-arn
// path (a non-empty arn passed to Run) - this is deliberately the one
// new test added for the newly-added fallback, not a rewrite of the
// others.
func TestCloudTestRunner_Run_DiscoversArnFromInvokeResponse(t *testing.T) {
	type event struct{}
	type result struct{}

	const discoveredArn = "arn:aws:lambda:us-east-1:123456789012:function:my-func:1/durable-execution/fresh-exec/abc123"

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`), executionArn: discoveredArn}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{succeededExecutionOp()}},
		},
	}

	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond

	// arn == "" - the caller has no pre-known ARN, matching a genuinely
	// fresh invocation with nothing to resume observing.
	res, err := runner.Run(context.Background(), "", event{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", res.GetStatus())
	}
	if len(stateClient.requestedArns) == 0 || stateClient.requestedArns[0] != discoveredArn {
		t.Fatalf("expected GetExecutionState to be polled with the ARN discovered from the Invoke response (%q), got %+v", discoveredArn, stateClient.requestedArns)
	}
}

// TestCloudTestRunner_Run_NoArnAvailable confirms Run fails with a clear,
// actionable error - rather than polling GetExecutionState with an
// empty/meaningless ARN - when neither the caller supplied one nor the
// Invoke response returned one (e.g. a misconfigured FunctionName that
// isn't actually a durable function, or one invoked unqualified - see
// FunctionName's own doc on the Qualified ARNs requirement).
func TestCloudTestRunner_Run_NoArnAvailable(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)} // executionArn left empty
	runner := NewCloudTestRunner[event, result]("my-func", invoker, &fakeStateClient{})

	_, err := runner.Run(context.Background(), "", event{})
	if err == nil {
		t.Fatal("expected Run to fail when no execution ARN is available from either the caller or the Invoke response")
	}
}

func TestCloudTestRunner_Run_PollsUntilTerminal(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			// First two polls: execution still running (no terminal
			// status) - matching a real execution suspended on a
			// Wait/retry-delay/Callback that Lambda itself is still
			// resolving before the synchronous Invoke call this runner
			// made even returns... except Invoke here already returned
			// (see fakeLambdaInvoker), so this simulates the documented
			// UNCONFIRMED case noted in Run's own doc comment: Invoke
			// returning before the execution is fully terminal. This
			// runner's poll loop must keep polling rather than treating
			// Invoke's own return as sufficient.
			{Operations: []types.Operation{{ID: "exec-root", Type: types.OperationTypeExecution, Status: types.OperationStatusStarted}}},
			{Operations: []types.Operation{{ID: "exec-root", Type: types.OperationTypeExecution, Status: types.OperationStatusStarted}}},
			{Operations: []types.Operation{succeededExecutionOp()}},
		},
	}

	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond
	runner.Timeout = 5 * time.Second

	res, err := runner.Run(context.Background(), "arn:test-exec", event{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED after polling, got %s", res.GetStatus())
	}
	if stateClient.calls != 3 {
		t.Fatalf("expected exactly 3 GetExecutionState polls (2 non-terminal + 1 terminal), got %d", stateClient.calls)
	}
}

func TestCloudTestRunner_Run_TimesOutIfNeverTerminal(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{{ID: "exec-root", Type: types.OperationTypeExecution, Status: types.OperationStatusStarted}}},
		},
	}

	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond
	runner.Timeout = 20 * time.Millisecond

	_, err := runner.Run(context.Background(), "arn:test-exec", event{})
	if err == nil {
		t.Fatal("expected a timeout error when the execution never reaches a terminal status")
	}
}

func TestCloudTestRunner_Run_FailedExecution(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{"errorMessage":"boom"}`), functionError: false}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{
				{ID: "exec-root", Type: types.OperationTypeExecution, Status: types.OperationStatusFailed},
				{ID: "1", Type: types.OperationTypeStep, Name: "create-greeting", Status: types.OperationStatusFailed,
					StepDetails: &types.StepDetails{Error: &types.ErrorObject{ErrorMessage: "boom", ErrorType: "CustomError"}}},
			}},
		},
	}

	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond

	res, err := runner.Run(context.Background(), "arn:test-exec", event{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", res.GetStatus())
	}
	// GetResult must fail cleanly for a FAILED execution, exactly like
	// LocalTestRunner's own GetResult does (see result.go).
	if _, err := GetResult[result](res); err == nil {
		t.Fatal("expected GetResult to fail for a FAILED execution")
	}

	op, ok := res.GetOperation("create-greeting")
	if !ok {
		t.Fatal("expected the failed step operation to still be present in the polled history")
	}
	stepErr := op.GetError()
	if stepErr == nil || stepErr.ErrorMessage != "boom" {
		t.Fatalf("expected the step's structured error to round-trip through GetDurableExecutionState polling, got %+v", stepErr)
	}
}

func TestCloudTestRunner_Run_FunctionErrorDoesNotSurfaceInvokePayloadAsResult(t *testing.T) {
	// If the target Lambda function's own container crashes (Lambda's
	// FunctionError, distinct from an SDK-caught FAILED durable
	// execution - see LambdaInvoker.Invoke's doc), the Invoke response
	// payload is Lambda's OWN error envelope, not a real handler result.
	// This must never be threaded through as if it were a succeeded
	// execution's ResultPayload, even if GetDurableExecutionState (for
	// whatever reason) still reports the root operation as SUCCEEDED at
	// the moment this test polls it - functionError is Lambda's own
	// signal about the INVOKE call itself, and should not be silently
	// discarded. This test pins down the current, deliberately
	// conservative behavior: functionError only affects whether
	// invokeResultPayload is populated, not whether polling proceeds
	// (see Run's own comment on why it does not short-circuit on
	// functionError).
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{
		responsePayload: []byte(`{"errorMessage":"Runtime.HandlerNotFound","errorType":"Runtime.HandlerNotFound"}`),
		functionError:   true,
	}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{succeededExecutionOp()}},
		},
	}

	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond

	res, err := runner.Run(context.Background(), "arn:test-exec", event{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// resultPayload must NOT be Lambda's own error envelope.
	if _, err := GetResult[result](res); err == nil {
		t.Fatal("expected GetResult to fail rather than deserialize Lambda's FunctionError envelope as a real result")
	}
}

func TestCloudTestRunner_Run_NilInvokerOrStateClient(t *testing.T) {
	type event struct{}
	type result struct{}

	r1 := NewCloudTestRunner[event, result]("my-func", nil, &fakeStateClient{})
	if _, err := r1.Run(context.Background(), "arn:test", event{}); err == nil {
		t.Fatal("expected an error when Invoker is nil")
	}

	r2 := NewCloudTestRunner[event, result]("my-func", &fakeLambdaInvoker{}, nil)
	if _, err := r2.Run(context.Background(), "arn:test", event{}); err == nil {
		t.Fatal("expected an error when StateClient is nil")
	}
}

func TestCloudTestRunner_Run_InvokeError(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{err: errors.New("network unreachable")}
	runner := NewCloudTestRunner[event, result]("my-func", invoker, &fakeStateClient{})

	_, err := runner.Run(context.Background(), "arn:test", event{})
	if err == nil {
		t.Fatal("expected Run to propagate an Invoke error")
	}
}

func TestCloudTestRunner_Run_StateClientError(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)}
	stateClient := &fakeStateClient{err: errors.New("throttled")}
	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.Timeout = time.Second

	_, err := runner.Run(context.Background(), "arn:test", event{})
	if err == nil {
		t.Fatal("expected Run to propagate a GetExecutionState error")
	}
}

func TestCloudTestRunner_FetchAllOperations_FollowsPagination(t *testing.T) {
	// Confirms fetchAllOperations follows NextMarker across multiple
	// pages rather than silently truncating the operation log - see
	// that method's doc for why this matters (a large Map/Parallel
	// execution's operation count could exceed a single
	// GetDurableExecutionState page).
	type event struct{}
	type result struct{}

	page1Marker := "marker-1"
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{
				Operations: []types.Operation{{ID: "1", Type: types.OperationTypeStep, Name: "step-1", Status: types.OperationStatusSucceeded}},
				NextMarker: &page1Marker,
			},
			{
				Operations: []types.Operation{succeededExecutionOp(), {ID: "2", Type: types.OperationTypeStep, Name: "step-2", Status: types.OperationStatusSucceeded}},
			},
		},
	}
	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)}
	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = time.Millisecond

	res, err := runner.Run(context.Background(), "arn:test", event{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", res.GetStatus())
	}
	if _, ok := res.GetOperation("step-1"); !ok {
		t.Fatal("expected step-1 (from the first page) to be present")
	}
	if _, ok := res.GetOperation("step-2"); !ok {
		t.Fatal("expected step-2 (from the second, marker-following page) to be present")
	}
	if len(stateClient.requestedTokens) < 1 || stateClient.requestedTokens[0] != externalPollerCheckpointToken {
		t.Fatalf("expected the fixed externalPollerCheckpointToken placeholder on every poll (see that constant's own doc: an empty token is CONFIRMED rejected by the real API's own request validation, found this session), got %+v", stateClient.requestedTokens)
	}
}

func TestCloudTestRunner_ContextCancellation(t *testing.T) {
	type event struct{}
	type result struct{}

	invoker := &fakeLambdaInvoker{responsePayload: []byte(`{}`)}
	stateClient := &fakeStateClient{
		responses: []types.GetDurableExecutionStateResponse{
			{Operations: []types.Operation{{ID: "exec-root", Type: types.OperationTypeExecution, Status: types.OperationStatusStarted}}},
		},
	}
	runner := NewCloudTestRunner[event, result]("my-func", invoker, stateClient)
	runner.PollInterval = 50 * time.Millisecond
	runner.Timeout = time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := runner.Run(ctx, "arn:test", event{})
	if err == nil {
		t.Fatal("expected Run to return an error when its context is cancelled mid-poll")
	}
}

// TestGetExecutionStateClient_SigV4LambdaSatisfiesInterface exercises
// the compile-time assertion above via a real (if trivial) call path,
// confirming sigv4lambda.Client can be used ANYWHERE this package's
// CloudTestRunner.StateClient field is - not just that the interface
// method sets happen to line up syntactically, but that constructing a
// CloudTestRunner with one compiles and type-checks end to end.
func TestGetExecutionStateClient_SigV4LambdaSatisfiesInterface(t *testing.T) {
	type event struct{}
	type result struct{}

	var stateClient checkpoint.GetExecutionStateClient = &sigv4lambda.Client{}
	runner := NewCloudTestRunner[event, result]("my-func", &fakeLambdaInvoker{}, stateClient)
	if runner.StateClient == nil {
		t.Fatal("expected StateClient to be set")
	}
	_ = fmt.Sprintf("%T", runner) // keep fmt import meaningful/used
}

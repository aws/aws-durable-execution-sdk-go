// Package testing provides an in-process test runner for AWS Durable
// Execution SDK for Go handlers, requiring no AWS credentials, no
// deployment, and no containers.
//
// This is the Go port of the officially documented, cross-SDK
// LocalDurableTestRunner (docs.aws.amazon.com/durable-execution/testing/,
// confirmed present in the TypeScript, Python, and Java reference SDKs —
// this is a universal, cross-SDK testing pattern, not something specific
// to any one language). Per docs/remaining-work.md's completion criteria,
// this package is a prerequisite for considering ANY durable operation
// "done": every operation needs an example that runs and passes against
// this local runner (and, eventually, the cloud runner — see
// docs/remaining-work.md task 17a for the cloud half, not yet
// implemented).
//
// Only the operations the SDK itself has implemented so far (Step, Wait)
// are exercised by real, passing examples today. RunInChildContext,
// CreateCallback/WaitForCallback, WaitForCondition, Invoke, Map, and
// Parallel are supported by this runner's design (see Operation's
// SendCallbackSuccess/Failure, and GetChildOperations) ahead of their
// SDK implementations landing, so no further changes to this package
// should be needed as those operations are built — only new examples.
package testing

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// LocalTestRunnerConfig customizes LocalTestRunner behavior, mirroring
// the official API reference's LocalDurableTestRunnerSetupParameters.
type LocalTestRunnerConfig struct {
	// SkipTime installs the local runner's fast-forwarding behavior:
	// context.Wait() delays and step retry delays complete without real
	// wall-clock waiting. Defaults to true — unlike the TypeScript SDK
	// (which defaults this to false and requires explicitly opting in),
	// this Go port defaults to the fast, deterministic behavior since it
	// is what most tests want, and callers who need to test real
	// wall-clock timing should use the (future) cloud runner instead, not
	// a slow local test.
	SkipTime bool
}

// DefaultLocalTestRunnerConfig returns the default configuration
// (SkipTime: true).
func DefaultLocalTestRunnerConfig() LocalTestRunnerConfig {
	return LocalTestRunnerConfig{SkipTime: true}
}

// LocalTestRunner runs a durable handler in-process against an in-memory
// checkpoint store. No AWS credentials, no deployment, and no containers
// are required. Use it for unit tests and CI, matching the official
// runner API reference's LocalDurableTestRunner exactly in intent (see
// this package's doc comment).
//
// A LocalTestRunner is reusable across multiple logical executions within
// one test via Run — each call with a fresh executionID starts an
// independent execution sharing the same in-memory client. Call Reset to
// discard all recorded executions and start over, matching the official
// reset()/"create a new runner instance per test" guidance.
type LocalTestRunner[TEvent, TResult any] struct {
	handler durable.Handler[TEvent, TResult]
	cfg     LocalTestRunnerConfig
	client  *inMemoryClient

	execCounter int
}

// New creates a LocalTestRunner for handler, matching the official runner
// API's `new LocalDurableTestRunner({ handlerFunction: handler })` /
// `LocalDurableTestRunner.create(...)`. cfg is optional; pass nil for
// DefaultLocalTestRunnerConfig().
func New[TEvent, TResult any](handler durable.Handler[TEvent, TResult], cfg *LocalTestRunnerConfig) *LocalTestRunner[TEvent, TResult] {
	resolved := DefaultLocalTestRunnerConfig()
	if cfg != nil {
		resolved = *cfg
	}
	return &LocalTestRunner[TEvent, TResult]{
		handler: handler,
		cfg:     resolved,
		client:  newInMemoryClient(resolved.SkipTime),
	}
}

// Run drives handler against event, running the full replay loop
// in-process until the execution reaches a terminal status (SUCCEEDED or
// FAILED) or genuinely suspends with SkipTime disabled and a pending
// Wait/retry/callback that this call does not resolve (returns PENDING in
// that case). Matches the official runner API's run()/runUntilComplete()
// — Go's version always drives to completion in one call when SkipTime is
// enabled (the default), since there is no real timer to wait out.
//
// Each call to Run starts a new, independent execution (its own execution
// ARN and operation history) within this runner's shared in-memory
// client, so multiple Run calls on the same LocalTestRunner do not
// interfere with each other — this differs from the officially documented
// reset()-per-test requirement, which exists in the reference SDKs because
// their runners model exactly one execution per instance; the Go port's
// per-Run isolation makes that requirement unnecessary for the common case
// of independent Run calls, while Reset (below) remains available to
// explicitly discard all history at once.
func (r *LocalTestRunner[TEvent, TResult]) Run(event TEvent) (TestResult, error) {
	r.execCounter++
	arn := fmt.Sprintf("arn:aws:lambda:local:000000000000:function:local-test-function:%d", r.execCounter)

	entry := durable.WithDurableExecution(r.handler, &durable.Config{Client: r.client})
	// Pass r.client (not nil) so the root EXECUTION operation is seeded
	// into the client from this very first invocation, exactly like
	// Continue already does. This matters once driveToCompletion's
	// re-invocation loop needs to rebuild a later invocation's input from
	// client.snapshotForExecution - which is every invocation after the
	// first PENDING result, not just ones reached via Continue. Passing
	// nil here left the client's byExecID map for this ARN without a
	// root op until SOME OTHR call happened to seed it, which most
	// existing tests never triggered (their suspend/resume operations
	// resolved before a second driveToCompletion loop iteration needed
	// to rebuild input from the client) - but retry-delay suspension
	// (see operations/step.go's retryOrFail) is a genuine case where a
	// second iteration is required, and it surfaced this real gap.
	input, err := newInvocationInput(arn, "token-0", event, r.client)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.LocalTestRunner.Run: building invocation input: %w", err)
	}

	return r.driveToCompletion(entry, input, arn)
}

// RunAsync starts an execution without waiting for it to reach a terminal
// status, returning immediately once the first invocation suspends
// (PENDING) or completes. Matches the official Python runner API's
// run_async() / Java's run() (non-runUntilComplete). Use this to reach a
// PENDING state and then drive a CALLBACK operation via Operation's
// SendCallbackSuccess/Failure before continuing with Continue.
func (r *LocalTestRunner[TEvent, TResult]) RunAsync(event TEvent) (TestResult, error) {
	r.execCounter++
	arn := fmt.Sprintf("arn:aws:lambda:local:000000000000:function:local-test-function:%d", r.execCounter)

	entry := durable.WithDurableExecution(r.handler, &durable.Config{Client: r.client})
	// See Run's matching fix for why this must be r.client, not nil.
	input, err := newInvocationInput(arn, "token-0", event, r.client)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.LocalTestRunner.RunAsync: building invocation input: %w", err)
	}

	out, err := entry(context.Background(), input)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.LocalTestRunner.RunAsync: invoking handler: %w", err)
	}
	return r.toTestResult(out, arn), nil
}

// Continue re-invokes handler against the execution identified by arn
// (the ExecutionARN embedded in a prior PENDING TestResult — see
// TestResult's operations, which carry no ARN directly today; callers
// driving callbacks should instead prefer the single-call Run, which
// blocks until a real terminal result once SkipTime resolves any
// intermediate suspension). Exists primarily to support
// AdvanceTime-driven scenarios and manual callback-driving tests; see
// operation.go's SendCallbackSuccess/Failure for the more common path,
// which resolves the callback and lets the SAME Run/RunAsync call's
// underlying replay loop pick it up without the caller managing ARNs
// directly.
func (r *LocalTestRunner[TEvent, TResult]) Continue(arn string, event TEvent) (TestResult, error) {
	entry := durable.WithDurableExecution(r.handler, &durable.Config{Client: r.client})
	input, err := newInvocationInput(arn, "token-0", event, r.client)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.LocalTestRunner.Continue: building invocation input: %w", err)
	}
	return r.driveToCompletion(entry, input, arn)
}

// driveToCompletion repeatedly invokes entry against arn's accumulated
// operation history until the execution reaches SUCCEEDED or FAILED, or
// until an invocation returns PENDING without the in-memory client having
// resolved anything further to check (i.e. genuine suspension on a
// callback or a SkipTime-disabled wait/retry that this call cannot drive
// forward on its own).
func (r *LocalTestRunner[TEvent, TResult]) driveToCompletion(
	entry func(ctx context.Context, input types.DurableExecutionInvocationInput) (types.DurableExecutionOutput, error),
	input types.DurableExecutionInvocationInput,
	arn string,
) (TestResult, error) {
	for {
		out, err := entry(context.Background(), input)
		if err != nil {
			return TestResult{}, fmt.Errorf("testing.LocalTestRunner: invoking handler: %w", err)
		}
		if out.Status != types.ExecutionStatusPending {
			return r.toTestResult(out, arn), nil
		}

		// PENDING: with SkipTime enabled, any WAIT the handler just
		// checkpointed was already resolved synchronously by
		// inMemoryClient.Checkpoint, so re-invoking immediately makes
		// forward progress. If nothing in the operation log actually
		// changed since the last invocation (e.g. a real CALLBACK still
		// waiting on the test to call SendCallbackSuccess), re-invoking
		// would loop forever - detect that and return PENDING instead.
		var nextInput types.DurableExecutionInvocationInput
		nextInput, err = newInvocationInput(arn, "token-0", zeroEvent[TEvent](), r.client)
		if err != nil {
			return TestResult{}, fmt.Errorf("testing.LocalTestRunner: rebuilding invocation input for replay: %w", err)
		}
		if !hasUnresolvedProgress(input, nextInput) {
			return r.toTestResult(out, arn), nil
		}
		input = nextInput
	}
}

// hasUnresolvedProgress reports whether next's operation log differs from
// prev's in a way that would let another invocation make forward
// progress. Used by driveToCompletion to avoid an infinite re-invocation
// loop when the execution is genuinely blocked on something this runner
// cannot resolve on its own (a real pending CALLBACK, or a WAIT/retry
// left un-skipped because SkipTime is disabled).
//
// Comparing top-level Status alone is not sufficient: a STEP that retries
// twice in a row goes Pending -> Pending across two consecutive
// checkpoints (attempt N fails, gets retried, attempt N+1 fails too,
// gets retried again) - Status never changes even though real progress
// happened (StepDetails.Attempt incremented, a fresh execution of fn
// happened). Missing this would make driveToCompletion stop and report a
// stale PENDING after the SECOND consecutive retry, even though SkipTime
// is enabled and a real backend would have kept re-invoking. So this also
// compares StepDetails.Attempt for STEP operations specifically - the one
// other field the retry-delay suspension path (operations/step.go's
// retryOrFail) advances without necessarily changing Status.
func hasUnresolvedProgress(prev, next types.DurableExecutionInvocationInput) bool {
	type key struct {
		status  types.OperationStatus
		attempt int
	}
	prevKey := make(map[string]key, len(prev.InitialExecutionState.Operations))
	for _, op := range prev.InitialExecutionState.Operations {
		attempt := 0
		if op.StepDetails != nil {
			attempt = op.StepDetails.Attempt
		}
		prevKey[op.ID] = key{status: op.Status, attempt: attempt}
	}
	for _, op := range next.InitialExecutionState.Operations {
		attempt := 0
		if op.StepDetails != nil {
			attempt = op.StepDetails.Attempt
		}
		if prevKey[op.ID] != (key{status: op.Status, attempt: attempt}) {
			return true
		}
	}
	return len(next.InitialExecutionState.Operations) != len(prev.InitialExecutionState.Operations)
}

// zeroEvent returns the zero value of TEvent. Replay invocations pass the
// original event's checkpointed InputPayload from the operation log (see
// newInvocationInput), not a freshly supplied one, so the event argument
// to entry() itself is irrelevant on replay; this exists only to satisfy
// newInvocationInput's signature without threading the original event
// through driveToCompletion's loop.
func zeroEvent[T any]() T {
	var zero T
	return zero
}

// GetOperation looks up a top-level operation by name across ALL
// executions this runner has driven (via Run, RunAsync, or Continue),
// matching the official runner API's getOperation(name) called directly
// on the runner (as opposed to on a TestResult) — used to drive a
// callback after a PENDING RunAsync result, before the execution has
// fully completed.
func (r *LocalTestRunner[TEvent, TResult]) GetOperation(name string) (Operation, bool) {
	for _, op := range r.client.snapshot() {
		if op.Name == name {
			return Operation{raw: op, runner: r}, true
		}
	}
	return Operation{}, false
}

// sendCallbackResult is the shared implementation behind
// Operation.SendCallbackSuccess/Failure — see operation.go. It resolves
// the callback in the underlying in-memory client so the NEXT invocation
// (whether driven by Run's own loop, or by an explicit Continue call)
// observes it as terminal.
func (r *LocalTestRunner[TEvent, TResult]) sendCallbackResult(callbackID string, result *string, errObj *types.ErrorObject) error {
	if _, ok := r.client.completeCallback(callbackID, result, errObj); !ok {
		return fmt.Errorf("testing.LocalTestRunner: no callback operation found with callback ID %q", callbackID)
	}
	return nil
}

// Reset discards all recorded executions and operation history, so this
// runner can be reused across independent tests without carrying over
// state. Matches the official runner API's reset(); Go callers may
// instead prefer constructing a fresh LocalTestRunner per test (matching
// the official Python/Java guidance to "create a new runner instance per
// test"), since New is cheap.
func (r *LocalTestRunner[TEvent, TResult]) Reset() {
	r.client = newInMemoryClient(r.cfg.SkipTime)
	r.execCounter = 0
}

// RegisterFunction registers a mock implementation for functionName (a
// raw JSON payload in, raw JSON payload out function), used to resolve
// operations.Invoke calls targeting it. Matches the official runner
// API's registerFunction (see the Testing API Reference's "Register mock
// handlers for invoke") - the untyped, raw-payload version, for mocking
// a standard (non-durable) Lambda function's behavior without needing
// this SDK's own handler-wrapping machinery.
//
// fn's returned string must itself be valid JSON matching what
// operations.Invoke's configured Serdes (JSON by default) expects to
// deserialize - e.g. return `"ok"` (with quotes) for a string result,
// not the bare word ok, matching how operations.Invoke's TOut is
// deserialized on the caller's side. Prefer RegisterDurableFunction for
// typed input/output, which handles this marshaling automatically.
//
// Returns the runner for chaining, matching the official API's fluent
// registerFunction/registerDurableFunction.
func (r *LocalTestRunner[TEvent, TResult]) RegisterFunction(functionName string, fn func(payload string) (string, error)) *LocalTestRunner[TEvent, TResult] {
	r.client.registerFunction(functionName, fn)
	return r
}

// RegisterDurableFunction registers a mock implementation for
// functionName with typed input/output, matching the official runner
// API's registerDurableFunction - the common case of mocking another
// durable (or standard) Lambda function's business logic without
// hand-writing JSON marshaling at each call site. fn receives the
// deserialized input and returns the (unserialized) output or an error.
func RegisterDurableFunction[TEvent, TResult any, TIn, TOut any](r *LocalTestRunner[TEvent, TResult], functionName string, fn func(input TIn) (TOut, error)) *LocalTestRunner[TEvent, TResult] {
	r.client.registerFunction(functionName, func(payload string) (string, error) {
		var in TIn
		if err := json.Unmarshal([]byte(payload), &in); err != nil {
			return "", fmt.Errorf("testing.RegisterDurableFunction(%q): unmarshaling invoke input into %T: %w", functionName, in, err)
		}
		out, err := fn(in)
		if err != nil {
			return "", err
		}
		b, err := json.Marshal(out)
		if err != nil {
			return "", fmt.Errorf("testing.RegisterDurableFunction(%q): marshaling invoke output: %w", functionName, err)
		}
		return string(b), nil
	})
	return r
}

func (r *LocalTestRunner[TEvent, TResult]) toTestResult(out types.DurableExecutionOutput, arn string) TestResult {
	// out.Error is now a nested *types.ErrorObject (see
	// types.DurableExecutionOutput's own doc for why), not a flat
	// *string - newTestResult's own errorMessage parameter still wants a
	// flat *string, so extract ErrorMessage here rather than threading
	// the nested shape further through this package's own TestResult
	// type, which predates and is independent of this wire-shape fix.
	var errMsg *string
	if out.Error != nil {
		errMsg = &out.Error.ErrorMessage
	}
	return newTestResult(out.Status, out.ResultPayload, errMsg, r.client.snapshotSingleExecution(arn), r)
}

// newTestResult constructs a TestResult from its constituent fields.
// This is the SINGLE shared construction path for TestResult, used by
// both LocalTestRunner (via toTestResult above, operating on
// DurableExecutionOutput plus an in-memory client snapshot) and
// CloudTestRunner (see cloud_runner.go, operating on a real
// GetDurableExecutionState response's Operations[] plus the execution's
// terminal status inferred from those operations, since a real
// synchronous Lambda Invoke response is not itself shaped like
// DurableExecutionOutput — see cloud_runner.go's doc for why).
//
// Kept as a free function rather than a TestResult method (which would
// need to be exported to be called from outside this file, defeating the
// point of keeping TestResult's fields private) — this is the
// "refactor toTestResult into something both runners call" mentioned in
// docs/remaining-work.md task 17a's cloud-runner design goal, rather than
// duplicating field assignment in two places that must be kept in sync by
// hand.
func newTestResult(status types.ExecutionStatus, resultPayload, errorMessage *string, operations map[string]types.Operation, runner callbackDriver) TestResult {
	return TestResult{
		status:        status,
		resultPayload: resultPayload,
		errorMessage:  errorMessage,
		operations:    operations,
		runner:        runner,
	}
}

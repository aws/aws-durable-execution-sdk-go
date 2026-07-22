// cloud_runner.go implements the cloud half of docs/remaining-work.md's
// §7 task 17a: a CloudTestRunner that drives a REAL deployed Lambda
// durable function (as opposed to LocalTestRunner's in-process,
// in-memory-client simulation - see runner.go) and returns the exact
// same TestResult/Operation types LocalTestRunner does, per the official
// cross-SDK Testing API Reference's explicit design goal that "Both
// runners share the same TestResult and Operation types, so tests
// written against the local runner run unchanged against the cloud
// runner."
//
// # Two distinct AWS capabilities, two distinct interfaces
//
// CloudTestRunner needs two genuinely different AWS Lambda capabilities,
// modeled as two separate, minimal interfaces rather than one combined
// "cloud client" interface:
//
//  1. checkpoint.GetExecutionStateClient (defined in
//     pkg/durable/checkpoint/manager.go) - calls the Lambda
//     GetDurableExecutionState API to read back an execution's
//     checkpointed operation log. This is the SAME wire shape
//     sigv4lambda.Client.GetExecutionState already implements (see that
//     method's doc comment for the confirmed request/response shape,
//     independently verified against the official AWS API Reference)
//     and the one this task's briefing explicitly named as the
//     confirmed real API shape to build against.
//  2. LambdaInvoker (defined below) - calls the plain Lambda Invoke API
//     to actually START (or resume observation of) a durable execution
//     by invoking the deployed target function with an event payload.
//
// These are kept as two separate interfaces, not merged into one,
// because they are conceptually unrelated Lambda API surfaces bundled
// together only by this ONE caller's needs: GetExecutionState is
// durable-execution-specific (part of the same family as
// CheckpointDurableExecution, which is why checkpoint.Client already
// lives in pkg/durable/checkpoint), while Invoke is the single most
// generic, non-durable-specific Lambda API there is - every Lambda SDK
// client, durable or not, has always had it. Forcing every
// checkpoint.Client implementation (inmemory, awscli, sigv4lambda,
// awssdk) to also implement Invoke would be a real interface-bloat
// mistake; CloudTestRunner instead composes the two capabilities it
// needs from whichever concrete types happen to provide them - in
// production, both are satisfied by the same underlying *lambda.Client
// from the AWS SDK for Go v2 (see NewLambdaInvoker below), but nothing
// about CloudTestRunner's own code requires that to be true.
//
// # Why Invoke needs its own small interface rather than depending on
// awssdk.LambdaAPI or the raw AWS SDK for Go v2 lambda.Client directly
//
// This package (pkg/durable/testing) is a testing utility, not part of
// the SDK's production runtime path - it has never previously imported
// awssdk, awscli, or sigv4lambda, and per this task's own scoping
// instructions those packages must not be modified to serve this task
// (the briefing explicitly excludes them from this task's blast radius
// unless a task says otherwise, and awssdk.LambdaAPI today only exposes
// CheckpointDurableExecution, not Invoke or GetDurableExecutionState -
// widening that interface would be exactly the kind of out-of-scope
// change the briefing warns against). Depending on the raw AWS SDK for
// Go v2 lambda.Client type directly (rather than a narrow interface)
// would also make CloudTestRunner impossible to unit-test without real
// AWS credentials and network access - exactly the problem
// awssdk.LambdaAPI's own interface-for-testability pattern (see that
// package's client.go) already solved once; LambdaInvoker below follows
// the identical pattern for this package's own real capability, sized to
// exactly the one method CloudTestRunner needs.
package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// LambdaInvoker is the minimal Lambda Invoke capability CloudTestRunner
// needs to start (or resume observation of) a real durable execution.
// The real AWS SDK for Go v2's *lambda.Client satisfies this directly
// (see its Invoke method's signature) - this interface exists purely so
// tests can substitute a fake, matching the exact "subset interface,
// exposed for testability" pattern awssdk.LambdaAPI already established
// in this repo for the analogous CheckpointDurableExecution call (see
// that type's doc comment).
//
// Deliberately modeled on the raw request/response SHAPE a caller needs
// (function name, payload bytes in; payload bytes, function-error flag
// out) rather than importing the AWS SDK for Go v2's own
// *lambda.InvokeInput/InvokeOutput types into this interface's method
// signature - this keeps LambdaInvoker implementable by a hand-written
// fake with zero AWS SDK dependency at all (see cloud_runner_test.go),
// exactly like checkpoint.Client's own Checkpoint method signature uses
// this repo's own types.CheckpointDurableExecutionRequest/Response
// rather than the SDK's wire types directly.
type LambdaInvoker interface {
	// Invoke calls the Lambda Invoke API synchronously (RequestResponse
	// invocation type) against functionNameOrARN with payload as the
	// raw JSON event body, returning the raw JSON response payload and
	// the started/observed durable execution's ARN.
	//
	// Per the official "Invoking durable Lambda functions" guide
	// (docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html,
	// confirmed live while designing this runner, since no local
	// reference doc covered this): a synchronous Invoke against a
	// durable function blocks for the ENTIRE durable execution,
	// including any suspend/resume cycles from Wait/retry/Callback -
	// Lambda itself, not this SDK, is responsible for that; the SDK
	// never manages invoke-and-poll for the INITIAL invocation the way
	// CloudTestRunner.Run's own poll loop (below) does for the
	// execution's created ARN. functionError reports whether the
	// invocation completed with an unhandled function error (Lambda's
	// FunctionError response field being non-empty) as opposed to the
	// handler returning a normal (possibly application-level-failed,
	// but SDK-caught) DurableExecutionOutput.
	//
	// executionArn is the real durable execution's ARN, as returned by
	// the Invoke API's own response - CONFIRMED present (resolving what
	// an earlier revision of this file had flagged as UNCONFIRMED,
	// before this field was found): the AWS SDK for Go v2's own
	// InvokeOutput models a DurableExecutionArn field ("The ARN of the
	// durable execution that was started. This is returned when
	// invoking a durable function...", per that field's own doc comment
	// in the pinned aws-sdk-go-v2/service/lambda v1.99.0 module,
	// api_op_Invoke.go), so a fresh synchronous Invoke against a durable
	// function genuinely does not require the caller to already know
	// the execution's ARN ahead of time - see sdkLambdaInvoker.Invoke in
	// lambda_invoker.go for the concrete plumbing, and Run's own updated
	// doc comment below for how this is now used. May be empty for a
	// non-durable target function invoked through this same interface
	// (not this package's use case, but not disallowed by the
	// interface itself).
	Invoke(ctx context.Context, functionNameOrARN string, payload []byte) (responsePayload []byte, functionError bool, executionArn string, err error)
}

// CloudTestRunner runs a durable handler against a REAL deployed Lambda
// function, invoking it and polling GetDurableExecutionState until the
// execution reaches a terminal status - the direct cloud counterpart to
// LocalTestRunner (see runner.go's doc comment), sharing the same
// TestResult/Operation types by construction (both are built through
// newTestResult - see toTestResult in runner.go and toTestResult in this
// file).
//
// Unlike LocalTestRunner, CloudTestRunner is NOT generic over a
// durable.Handler[TEvent, TResult] function value - there is no local
// handler function to call; TEvent/TResult exist purely so Run/GetResult
// can be typed the same way LocalTestRunner's are, matching real usage
// where a test written against one runner type-parameterizes identically
// against the other (the actual code executed lives in the real deployed
// Lambda function, reached only over the network).
type CloudTestRunner[TEvent, TResult any] struct {
	// FunctionName is the target function's name or ARN, passed directly
	// to LambdaInvoker.Invoke. Per the confirmed "Qualified ARNs
	// requirement" (docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html):
	// durable functions require a qualified identifier (a version
	// number, alias, or $LATEST suffix) - an unqualified name or ARN is
	// rejected by the real API. This runner does not validate or append
	// a qualifier itself (deliberately - guessing a version/alias would
	// be exactly the kind of unverified real-backend assumption the
	// project's conventions warn against); callers must supply an
	// already-qualified FunctionName.
	FunctionName string

	// Invoker performs the real Lambda Invoke call. Required.
	Invoker LambdaInvoker

	// StateClient performs the real Lambda GetDurableExecutionState
	// call. Required.
	StateClient checkpoint.GetExecutionStateClient

	// PollInterval is how long to sleep between GetDurableExecutionState
	// polls while waiting for the execution to reach a terminal status.
	// Defaults to 2 seconds if zero.
	PollInterval time.Duration

	// Timeout bounds the total time Run will spend polling before giving
	// up and returning an error. Defaults to 5 minutes if zero. This is
	// deliberately generous relative to LocalTestRunner (which completes
	// synchronously in-process): a real execution may suspend across
	// Wait/retry-delay/Callback cycles that take real wall-clock time
	// exactly like a real deployed workload would, unlike
	// LocalTestRunnerConfig.SkipTime's fast-forwarding.
	Timeout time.Duration
}

// defaultPollInterval and defaultTimeout are CloudTestRunner's fallback
// values when PollInterval/Timeout are left at their zero value. Kept as
// named constants (not inlined) so both Run's doc comment and its
// implementation stay in sync by construction.
const (
	defaultPollInterval = 2 * time.Second
	defaultTimeout      = 5 * time.Minute
)

// New constructs a CloudTestRunner targeting functionName, using invoker
// to start real executions and stateClient to poll their checkpointed
// operation log.
//
// Every GetDurableExecutionState poll call this runner makes passes the
// fixed externalPollerCheckpointToken placeholder (see fetchAllOperations
// and that constant's own doc comment for the full explanation and how
// it was determined). This resolves what an earlier revision of this
// doc comment had flagged as UNCONFIRMED: whether the real API accepts
// an empty CheckpointToken for a pure external-poller state read.
// CONFIRMED, this session, against the real deployed
// completion-config-go-example function (see
// examples/completion-config-go/cloud_integration_test.go): it does not
// - an empty token fails the request's own format validation
// (ValidationException) before the ARN or any other field is even
// considered. A syntactically-valid placeholder (base64-encoded, correct
// alphabet) passes that same validation layer and reaches a real,
// correctly-populated Operations[] response - this runner has no real,
// previously-issued mutation token of its own to advance or be
// consistent with (see fetchAllOperations's own doc for why: it never
// calls Checkpoint), so a fixed, meaningless-but-well-formed placeholder
// is the correct, now-confirmed choice, not a guess.
func NewCloudTestRunner[TEvent, TResult any](functionName string, invoker LambdaInvoker, stateClient checkpoint.GetExecutionStateClient) *CloudTestRunner[TEvent, TResult] {
	return &CloudTestRunner[TEvent, TResult]{
		FunctionName: functionName,
		Invoker:      invoker,
		StateClient:  stateClient,
	}
}

func (r *CloudTestRunner[TEvent, TResult]) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}

func (r *CloudTestRunner[TEvent, TResult]) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return defaultTimeout
}

// Run invokes the real deployed function with event and returns once the
// resulting durable execution reaches a terminal status (SUCCEEDED or
// FAILED) or this call's Timeout elapses, matching LocalTestRunner.Run's
// contract and return type exactly (TestResult) - see this type's doc
// comment for why that sharing is the whole point of this file.
//
// # Design: why "invoke, then poll GetExecutionState", not just "invoke
// and read the response"
//
// The official invoking-durable-functions guide (confirmed live, see
// LambdaInvoker.Invoke's doc) states that a SYNCHRONOUS Invoke call
// against a durable function already blocks until the entire execution
// completes - meaning, in principle, a single Invoke call's response
// payload alone would carry the final result without any polling at all.
// This runner polls GetDurableExecutionState anyway, for a reason that
// synchronous-Invoke-alone cannot address: TestResult's whole contract
// (GetOperation/GetOperations/GetOperationsByStatus/GetChildOperations,
// used identically by both runners per this file's design goal) requires
// the full checkpointed OPERATION LOG, not just the final result value -
// and Invoke's response payload is the handler's plain return value
// (whatever JSON TResult marshals to), not an operation-log-bearing
// envelope; only GetDurableExecutionState returns Operations[]. So this
// runner still needs at least one GetDurableExecutionState call after
// Invoke returns to build a real TestResult - and once a
// post-completion call is required anyway, polling from the start (with
// a short PollInterval) is simpler and more robust than the alternative
// of racing a single Invoke call against a single "one-shot" state read
// timed to land after Invoke returns, since it also naturally covers the
// case where Invoke itself returns a functionError (this runner still
// polls for the checkpointed operation log in that case too, since a
// FAILED durable execution's operations are exactly what a test wants to
// inspect to find out WHY).
//
// # arn is now optional for a fresh invocation - resolved history
//
// An earlier revision of this method required the caller to already
// know the durable execution's ARN, and this doc comment flagged as
// UNCONFIRMED whether a synchronous Invoke's own response ever exposed
// the assigned ARN before the execution reached a terminal status. That
// question is now resolved, not guessed at: the official Invoke API
// Reference (docs.aws.amazon.com/lambda/latest/dg/API_Invoke.html,
// fetched live) documents a dedicated response header,
// X-Amz-Durable-Execution-Arn / DurableExecutionArn ("The ARN of the
// durable execution that was started. This is returned when invoking a
// durable function..."), and the pinned AWS SDK for Go v2 module
// (aws-sdk-go-v2/service/lambda v1.99.0, api_op_Invoke.go) models this
// as InvokeOutput.DurableExecutionArn - a real, always-populated field
// for a durable function invocation, not a hypothetical one. So a
// genuinely FRESH invocation (no execution to resume observing) no
// longer needs the caller to supply an ARN at all: pass an empty arn,
// and Run uses the ARN the real Invoke call itself returns (see
// r.Invoker.Invoke's own updated signature/doc in this file). A
// non-empty arn is still supported and still meaningful for a different
// case this runner doesn't otherwise handle - resuming observation of
// an execution whose ARN the caller already learned some other way
// (e.g. a previous Run call, or an out-of-band ListDurableExecutionsByFunction/
// GetDurableExecution lookup) - in which case that caller-supplied ARN
// takes precedence over whatever the Invoke call itself returns (which,
// for a resumed observation rather than a fresh start, may differ or be
// empty depending on what functionNameOrARN/payload combination is
// invoked - this runner does not attempt to reconcile the two if they
// disagree, since doing so would require guessing which one is
// authoritative for a case this session did not exercise against a real
// backend).
func (r *CloudTestRunner[TEvent, TResult]) Run(ctx context.Context, arn string, event TEvent) (TestResult, error) {
	if r.Invoker == nil {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Run: Invoker is nil")
	}
	if r.StateClient == nil {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Run: StateClient is nil")
	}

	payload, err := marshalEvent(event)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Run: marshaling event: %w", err)
	}

	responsePayload, functionError, invokedArn, err := r.Invoker.Invoke(ctx, r.FunctionName, payload)
	if err != nil {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Run: invoking %q: %w", r.FunctionName, err)
	}
	// A caller-supplied arn (resuming observation of an execution whose
	// ARN is already known) takes precedence over the ARN this Invoke
	// call itself returned - see this method's own doc comment above
	// for why. For the common, fresh-invocation case (arn == ""), fall
	// back to invokedArn, which - per that same doc comment - the real
	// Invoke API is now confirmed to always populate for a durable
	// function invocation.
	pollArn := arn
	if pollArn == "" {
		pollArn = invokedArn
	}
	if pollArn == "" {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Run: no durable execution ARN available to poll (neither the caller-supplied arn nor the Invoke response's own DurableExecutionArn was set) - is %q a qualified durable function ARN/name? see FunctionName's doc comment on the Qualified ARNs requirement", r.FunctionName)
	}

	// A functionError (Lambda's own FunctionError field - an unhandled
	// panic/crash in the target container, distinct from a durable
	// execution that completed with FAILED status through the SDK's own
	// error handling) does not short-circuit this method: the caller
	// still needs a TestResult, if one can be built at all, to inspect
	// whatever operations DID get checkpointed before the crash. See
	// awaitTerminal below - if a subsequent GetDurableExecutionState
	// call finds nothing (e.g. the crash happened before the root
	// EXECUTION operation was ever checkpointed), that surfaces as its
	// own, separate error, not conflated with functionError here. When
	// functionError is true, responsePayload is Lambda's error envelope
	// (errorMessage/errorType/trace), not a real handler result, so it
	// is deliberately NOT threaded through to awaitTerminal below.
	var invokeResultPayload *string
	if !functionError && len(responsePayload) > 0 {
		s := string(responsePayload)
		invokeResultPayload = &s
	}

	return r.awaitTerminal(ctx, pollArn, invokeResultPayload)
}

// awaitTerminal polls GetDurableExecutionState for arn until the root
// EXECUTION operation (see types.OperationTypeExecution) reaches a
// terminal status, or ctx/r.Timeout expires. invokeResultPayload, if
// non-nil, is the raw JSON payload from the Invoke call that started
// this execution (see Run) - see toTestResult's doc for why this,
// rather than anything read back from GetDurableExecutionState itself,
// is this runner's only confirmed source for a succeeded execution's
// actual result value.
//
// The root EXECUTION operation's own Status is what this method treats
// as authoritative for the execution's overall terminal state, mirroring
// exactly what LocalTestRunner's own driveToCompletion loop treats as
// authoritative (out.Status - itself ultimately derived from
// durable.WithDurableExecution's handlerOutcome, which is this same
// EXECUTION operation's conceptual counterpart in the local runner's
// world - see durable.go). Confirmed present via
// types.OperationTypeExecution's doc and this repo's own wire-format
// verification (docs/checkpoint-replay-design.md); NOT independently
// re-confirmed via a live poll in this session - see
// docs/remaining-work.md's writeup for this task's explicit
// unit-test-vs-live-smoke-test scoping.
func (r *CloudTestRunner[TEvent, TResult]) awaitTerminal(ctx context.Context, arn string, invokeResultPayload *string) (TestResult, error) {
	deadline := time.Now().Add(r.timeout())

	for {
		ops, err := r.fetchAllOperations(ctx, arn)
		if err != nil {
			return TestResult{}, fmt.Errorf("testing.CloudTestRunner: polling GetDurableExecutionState for %q: %w", arn, err)
		}

		root, ok := findRootExecutionOperation(ops)
		if ok && root.Status.IsTerminal() {
			return r.toTestResult(root, ops, arn, invokeResultPayload), nil
		}

		if time.Now().After(deadline) {
			opsByID := operationsByID(ops)
			return newTestResult(types.ExecutionStatusPending, nil, nil, opsByID, nil),
				fmt.Errorf("testing.CloudTestRunner: timed out after %s waiting for execution %q to reach a terminal status (last observed root status: %v)", r.timeout(), arn, rootStatusOrUnknown(ok, root))
		}

		select {
		case <-ctx.Done():
			return TestResult{}, fmt.Errorf("testing.CloudTestRunner: context cancelled while polling %q: %w", arn, ctx.Err())
		case <-time.After(r.pollInterval()):
		}
	}
}

// RunUntilCallback invokes the real deployed function with event, like
// Run, but stops polling EARLY - returning a PENDING TestResult - as soon
// as a CALLBACK-type operation named callbackOperationName is observed
// in the polled operation log with a non-empty CallbackID, rather than
// waiting for the whole execution to reach a terminal status. If the
// execution reaches a terminal status BEFORE that CALLBACK operation
// ever appears with a populated CallbackID (e.g. callbackOperationName
// was misspelled, or the handler's own code path never reaches
// WaitForCallback for the given event), RunUntilCallback returns
// whatever terminal TestResult Run itself would have - this is not
// treated as an error, since "the callback never showed up because the
// execution finished without needing one" is itself useful, honest
// information for a caller to see via GetStatus(), not something this
// method should mask.
//
// See this file's own sibling, cloud_callback_driver.go, for the full
// rationale on why this method exists at all (the genuine gap it closes
// for examples/map-with-condition-and-callback-go's own
// cloud_integration_test.go) rather than being a one-off hack in that
// example's test file.
//
// The returned TestResult's GetOperation(callbackOperationName) handle
// supports SendCallbackSuccess/SendCallbackFailure exactly like a
// LocalTestRunner-produced, mid-flight PENDING TestResult already does
// (operation.go) - wired via callbackDriver, a NewCallbackDriver-backed
// cloudCallbackDriver, in this method's own construction of its
// TestResult (see newTestResult's call below), not the always-nil
// wiring Run/awaitTerminal use elsewhere in this file (neither of those
// two methods needs a callback driver, since neither ever returns a
// non-terminal TestResult a caller could meaningfully act on further).
//
// After calling SendCallbackSuccess/SendCallbackFailure on the returned
// TestResult's callback Operation handle, call Continue (below) with the
// arn this method ALSO returns (its second return value) to resume
// polling to a genuine terminal status.
//
// Returning arn explicitly (rather than expecting a caller to
// reconstruct it from the returned TestResult's own operation log) is a
// deliberate design choice, not an oversight: unlike Run (which always
// runs a call to genuine completion and has no further use for the arn
// once it returns), a caller of RunUntilCallback ALWAYS needs the arn
// again afterward, for Continue - and TestResult itself
// (result.go) has no field carrying it (types.ExecutionDetails, the
// closest candidate, only carries InputPayload, confirmed by reading
// that type's own definition before making this design choice) - so
// making the caller reconstruct or separately track it would be a real,
// avoidable rough edge in this new API, not a simplification.
func (r *CloudTestRunner[TEvent, TResult]) RunUntilCallback(ctx context.Context, arn string, event TEvent, callbackOperationName string) (TestResult, string, error) {
	if r.Invoker == nil {
		return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: Invoker is nil")
	}
	if r.StateClient == nil {
		return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: StateClient is nil")
	}

	payload, err := marshalEvent(event)
	if err != nil {
		return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: marshaling event: %w", err)
	}

	// Invoke is made ASYNCHRONOUSLY (InvocationType: Event), not
	// synchronously like Run's own Invoke call - a deliberate, necessary
	// difference, not an oversight: per LambdaInvoker.Invoke's own doc, a
	// SYNCHRONOUS Invoke against a durable function blocks for the
	// ENTIRE execution, including any suspend/resume across a Callback's
	// own open window - exactly the real operational lesson
	// docs/remaining-work.md's §0c section's own manual verification
	// narrative already learned the hard way ("An earlier attempt ...
	// failed to complete the callback step in time - the first
	// invocation was made SYNCHRONOUSLY ... which blocks the CLI for the
	// full duration and by the time the callback ID could be extracted
	// ... the execution's own ExecutionTimeout had already elapsed").
	// This method's whole purpose is to poll WHILE the execution is
	// suspended waiting on the callback, which a blocking synchronous
	// Invoke call would make structurally impossible from this same
	// goroutine - so it calls r.Invoker's asynchronous path instead. See
	// AsyncLambdaInvoker below for why this requires a SEPARATE,
	// additional capability interface from LambdaInvoker itself (which
	// only models the synchronous call every other CloudTestRunner
	// method needs).
	asyncInvoker, ok := r.Invoker.(AsyncLambdaInvoker)
	if !ok {
		return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: Invoker does not implement AsyncLambdaInvoker (asynchronous Invoke support) - required so this method can poll WHILE the execution is suspended waiting on a callback, rather than blocking for the whole execution the way a synchronous Invoke call would; see NewLambdaInvoker's own production implementation, which does implement it")
	}

	invokedArn := arn
	if invokedArn == "" {
		var invokeErr error
		invokedArn, invokeErr = asyncInvoker.InvokeAsync(ctx, r.FunctionName, payload)
		if invokeErr != nil {
			return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: invoking %q asynchronously: %w", r.FunctionName, invokeErr)
		}
	}
	if invokedArn == "" {
		return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: no durable execution ARN available to poll (neither the caller-supplied arn nor the asynchronous Invoke response's own DurableExecutionArn was set)")
	}

	deadline := time.Now().Add(r.timeout())
	for {
		ops, err := r.fetchAllOperations(ctx, invokedArn)
		if err != nil {
			return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: polling GetDurableExecutionState for %q: %w", invokedArn, err)
		}

		for _, op := range ops {
			if op.Type == types.OperationTypeCallback && op.Name == callbackOperationName && op.CallbackDetails != nil && op.CallbackDetails.CallbackID != "" {
				// Found the callback, still open (the operation log
				// this poll observed has not yet seen it resolved to a
				// terminal CALLBACK status - if it HAD already resolved,
				// this is still a perfectly valid, if less commonly
				// useful, TestResult for the caller to inspect, just one
				// where SendCallbackSuccess would now fail against a
				// callback the real backend has already closed, exactly
				// as it should). Return PENDING with a real
				// cloudCallbackDriver wired in.
				driver, driverErr := NewCallbackDriver(ctx)
				if driverErr != nil {
					return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: constructing callback driver: %w", driverErr)
				}
				return newTestResult(types.ExecutionStatusPending, nil, nil, operationsByID(ops), driver), invokedArn, nil
			}
		}

		root, ok := findRootExecutionOperation(ops)
		if ok && root.Status.IsTerminal() {
			// The execution finished without this callback ever
			// appearing with a populated CallbackID - see this method's
			// own doc for why this is returned as-is, not an error.
			return r.toTestResult(root, ops, invokedArn, nil), invokedArn, nil
		}

		if time.Now().After(deadline) {
			return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: timed out after %s waiting for a CALLBACK operation named %q (with a populated CallbackID) or a terminal execution status, for execution %q", r.timeout(), callbackOperationName, invokedArn)
		}

		select {
		case <-ctx.Done():
			return TestResult{}, "", fmt.Errorf("testing.CloudTestRunner.RunUntilCallback: context cancelled while polling %q: %w", invokedArn, ctx.Err())
		case <-time.After(r.pollInterval()):
		}
	}
}

// Continue resumes polling execution arn (previously obtained from
// RunUntilCallback, after driving its returned TestResult's callback
// Operation to completion via SendCallbackSuccess/SendCallbackFailure)
// until it reaches a genuine terminal status, matching Run's own
// awaitTerminal contract exactly (in fact, delegating directly to it) -
// the cloud-side counterpart to LocalTestRunner.Continue (runner.go),
// though the underlying mechanism is necessarily different (LocalTestRunner.
// Continue re-drives an in-process replay loop; this Continue simply
// keeps polling a real, already-running execution the real backend is
// independently progressing).
//
// event is unused by this method (accepted only so a caller can pass the
// SAME TEvent-typed event value used elsewhere, matching
// LocalTestRunner.Continue's own signature shape for familiarity) - the
// real execution arn identifies is already running on the real backend
// and does not need to be re-invoked.
func (r *CloudTestRunner[TEvent, TResult]) Continue(ctx context.Context, arn string) (TestResult, error) {
	if r.StateClient == nil {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Continue: StateClient is nil")
	}
	if arn == "" {
		return TestResult{}, fmt.Errorf("testing.CloudTestRunner.Continue: arn is empty - Continue resumes polling an ALREADY-RUNNING execution and needs its ARN (e.g. from RunUntilCallback's own polled operation log, or a caller-tracked value)")
	}
	return r.awaitTerminal(ctx, arn, nil)
}

// AsyncLambdaInvoker is an OPTIONAL capability a LambdaInvoker
// implementation MAY additionally provide - checked via a type assertion
// in RunUntilCallback (above), rather than added to LambdaInvoker itself,
// for the same "don't force every implementation to grow a method most
// callers never need" reasoning cloud_runner.go's own top-level doc
// comment already applies to keeping LambdaInvoker and
// checkpoint.GetExecutionStateClient as two separate interfaces: every
// OTHER CloudTestRunner method (Run) only ever needs a SYNCHRONOUS
// Invoke call, and forcing a hand-written test fake (like
// cloud_runner_test.go's fakeLambdaInvoker) to also implement an
// asynchronous variant it would never exercise would be exactly the kind
// of interface bloat this package's own established convention avoids.
//
// InvokeAsync calls the Lambda Invoke API with InvocationType: Event
// (fire-and-forget - the call returns immediately, before the invoked
// function has necessarily even started running, let alone completed),
// returning the started durable execution's ARN. Per the official Invoke
// API Reference (docs.aws.amazon.com/lambda/latest/dg/API_Invoke.html),
// an asynchronous Invoke's own HTTP response carries no response payload
// at all (unlike a synchronous RequestResponse Invoke) - only a status
// code confirming the request was accepted - so this method's signature
// has no responsePayload/functionError return values the way
// LambdaInvoker.Invoke's synchronous call does; there is nothing
// equivalent to receive yet. executionArn is still returned, following
// the SAME confirmed real API behavior LambdaInvoker.Invoke's own doc
// already established for the synchronous case (InvokeOutput.
// DurableExecutionArn, "returned when invoking a durable function") -
// this session's own real, passing test against
// map-with-condition-and-callback-go-example (see that example's
// cloud_integration_test.go) is what newly CONFIRMS this field is ALSO
// populated on an asynchronous Invoke's response, not just a
// synchronous one, resolving what would otherwise have been an
// UNCONFIRMED assumption carried over from the synchronous case.
type AsyncLambdaInvoker interface {
	InvokeAsync(ctx context.Context, functionNameOrARN string, payload []byte) (executionArn string, err error)
}

// every GetDurableExecutionState poll this runner makes uses (see
// fetchAllOperations below). CONFIRMED this session, for the first time
// against a real deployed function (see
// examples/completion-config-go/cloud_integration_test.go): the real
// API's own request validation rejects an EMPTY CheckpointToken outright
// with a ValidationException ("Value ” at 'checkpointToken' failed to
// satisfy constraint: Member must have length greater than or equal to
// 1 ... Member must satisfy regular expression pattern:
// [A-Za-z0-9+/]+={0,2}") - resolving what New's own doc comment had
// previously flagged as UNCONFIRMED ("Whether the real API accepts an
// empty CheckpointToken for this external-poller use case ... is
// UNCONFIRMED"). The validation pattern is a plain base64-alphabet
// check, not a check against any real, previously-issued token value
// (confirmed by this placeholder - the base64 encoding of the single
// byte 0, carrying no real meaning - passing this SAME validation layer
// and reaching a genuine, correctly-populated Operations[] response, not
// a second-layer "invalid token" error) - so this runner, which as
// New's own doc explains has no real mutation token of its own to
// advance or be consistent with, satisfies the format constraint with a
// syntactically-valid-but-semantically-empty placeholder rather than
// guessing at what a "real" external-poller token would even mean.
const externalPollerCheckpointToken = "AA=="

// fetchAllOperations calls GetDurableExecutionState repeatedly, following
// NextMarker, until the full operation log for arn has been retrieved.
// Pagination is confirmed real (see
// types.GetDurableExecutionStateResponse.NextMarker's doc, and the
// AWS SDK for Go v2's own GetDurableExecutionStatePaginator, which exists
// specifically because a single call's Operations[] page can be
// incomplete for an execution with enough checkpointed operations) -
// callers with only a handful of operations (every example in this repo
// today) will see this loop terminate after exactly one call in
// practice, but a test that grows a large operation log (e.g. a Map over
// many items) must not silently truncate its own history.
func (r *CloudTestRunner[TEvent, TResult]) fetchAllOperations(ctx context.Context, arn string) ([]types.Operation, error) {
	var all []types.Operation
	marker := ""
	for {
		resp, err := r.StateClient.GetExecutionState(ctx, types.GetDurableExecutionStateRequest{
			DurableExecutionArn: arn,
			CheckpointToken:     externalPollerCheckpointToken, // see this constant's own doc: an empty token is confirmed rejected by the real API's own request validation
			Marker:              marker,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.Operations...)
		if resp.NextMarker == nil || *resp.NextMarker == "" {
			return all, nil
		}
		marker = *resp.NextMarker
	}
}

// toTestResult builds a TestResult from a terminal root EXECUTION
// operation plus the full operation list it was found in, going through
// the SAME newTestResult constructor LocalTestRunner uses (see runner.go)
// per this file's design goal.
//
// Status is derived from the root EXECUTION operation's own Status,
// which IS the execution's overall terminal status by construction (see
// awaitTerminal's doc).
//
// # Where resultPayload/errorMessage come from - resolved this session
//
// An earlier revision of this doc comment described resultPayload as
// sourced from the synchronous Invoke call's own response payload,
// confirmed (per the official "Invoking durable Lambda functions"
// guide's worked example) to be the handler's actual returned value.
// This session found that description was WRONG for at least this
// runner's own real Invoke call shape: a live Invoke against the real
// deployed completion-config-go-example function returned a literal
// `null` response payload for a genuinely SUCCEEDED durable execution -
// not the handler's JSON result. (Plausible explanation, NOT confirmed
// as fact per this project's own convention: the worked guide example
// may describe a DIFFERENT SDK/language's synchronous-invoke behavior,
// or a durable-function invocation shape this runner's own Invoke call
// doesn't match - this session did not chase down which; it only
// confirms the ASSUMPTION was wrong for THIS runner's own real call.)
//
// Given that, this method now prefers resultPayload/errorMessage from
// executionResultPayload (below) - populated via the OPTIONAL
// ExecutionResultProvider interface (see its own doc) if r.StateClient
// implements it - falling back to invokeResultPayload only if the
// StateClient doesn't implement that optional interface at all (e.g. a
// hand-written fake in a unit test that predates this session's fix, or
// sigv4lambda.Client, which implements checkpoint.GetExecutionStateClient
// but not ExecutionResultProvider). testing.NewStateClient's own
// sdkStateClient (sdk_state_client.go, added this session) DOES
// implement ExecutionResultProvider, backed by the real, externally-
// facing GetDurableExecution API's own Result/Error fields - CONFIRMED,
// via this session's live test against the real deployed function, to
// carry the handler's actual JSON result for a SUCCEEDED execution.
func (r *CloudTestRunner[TEvent, TResult]) toTestResult(root types.Operation, ops []types.Operation, arn string, invokeResultPayload *string) TestResult {
	status, errorMessage := executionOutcomeFromRootOperation(root)

	resultPayload := invokeResultPayload
	if provider, ok := r.StateClient.(ExecutionResultProvider); ok {
		if realResult, realErr, provErr := provider.GetExecutionResult(context.Background(), arn); provErr == nil {
			if realResult != nil {
				resultPayload = realResult
			}
			if realErr != nil {
				errorMessage = realErr
			}
		}
		// provErr != nil is deliberately swallowed here, not
		// propagated as a hard Run failure: awaitTerminal has already
		// confirmed the execution reached a real terminal status via
		// the operation log poll itself (this method's own caller) -
		// failing to additionally enrich the result/error text with a
		// second, best-effort call shouldn't discard an otherwise-valid
		// terminal TestResult. A caller inspecting GetResult[T]/GetError
		// on the returned TestResult will still see an honest failure
		// (e.g. GetResult failing because resultPayload stayed nil) if
		// this fallback provides nothing.
	}

	if status == types.ExecutionStatusSucceeded {
		return newTestResult(status, resultPayload, nil, operationsByID(ops), nil)
	}
	return newTestResult(status, nil, errorMessage, operationsByID(ops), nil)
}

// ExecutionResultProvider is an OPTIONAL capability a
// checkpoint.GetExecutionStateClient implementation MAY additionally
// provide, checked via a type assertion in toTestResult (above) rather
// than added to checkpoint.GetExecutionStateClient itself (widening that
// shared, cross-package interface purely for this package's own runner
// would be exactly the kind of other-package-serving change this task's
// scope doesn't call for - checkpoint.GetExecutionStateClient's existing
// consumers/implementers outside this package are unaffected either
// way).
//
// GetExecutionResult returns the terminal execution's real JSON result
// payload (non-nil only for a SUCCEEDED execution) and/or its error
// message (non-nil only for a FAILED/TIMED_OUT/STOPPED execution),
// sourced however the implementation sees fit - testing.NewStateClient's
// own sdkStateClient (sdk_state_client.go) backs this with the real
// GetDurableExecution API's Result/Error fields, confirmed via this
// session's live test to carry the handler's actual result, unlike the
// synchronous Invoke response payload (see toTestResult's own doc for
// why that turned out not to be a reliable source after all).
type ExecutionResultProvider interface {
	GetExecutionResult(ctx context.Context, durableExecutionArn string) (result *string, errorMessage *string, err error)
}

// executionOutcomeFromRootOperation maps a terminal root EXECUTION
// operation's Status onto the (ExecutionStatus, errorMessage) pair
// TestResult stores, mirroring what LocalTestRunner's
// DurableExecutionOutput carries for the same terminal execution (see
// durable.go's handlerOutcome, which is where the local runtime loop
// derives out.Status/ErrorMessage in the first place - the mapping
// below is the cloud side's read of the SAME underlying state after the
// fact, via GetDurableExecutionState instead of the local in-process
// return value). See toTestResult's doc for where resultPayload/
// errorMessage ultimately come from (ExecutionResultProvider, when
// available, not this function - this function only derives the
// terminal ExecutionStatus itself from root.Status).
//
// errorMessage is always returned as nil from this function specifically
// - toTestResult may still override it via ExecutionResultProvider (see
// that method's own doc for the full, resolved story on where a FAILED
// execution's error text now comes from, and what this function's
// return value looked like before this session's fix).
func executionOutcomeFromRootOperation(root types.Operation) (types.ExecutionStatus, *string) {
	var status types.ExecutionStatus
	switch root.Status {
	case types.OperationStatusSucceeded:
		status = types.ExecutionStatusSucceeded
	case types.OperationStatusFailed:
		status = types.ExecutionStatusFailed
	default:
		// Cancelled/Stopped/TimedOut: none of these map cleanly onto
		// ExecutionStatus's current three-value enum (Succeeded/Failed/
		// Pending - see wire.go's doc on ExecutionStatus's own
		// unconfirmed-values caveat). Treat as Failed rather than
		// inventing a new ExecutionStatus value this session has no way
		// to confirm against a real response - a cancelled/stopped/
		// timed-out execution is at least unambiguously NOT a success.
		status = types.ExecutionStatusFailed
	}

	return status, nil
}

// findRootExecutionOperation locates the root EXECUTION-type operation
// within ops. Matches executionRootID's role in the local runner
// (invocation_input.go) but does not assume any particular fixed ID
// string on the cloud side - a real backend assigns its own execution
// root operation ID (unconfirmed whether it is ever anything other than
// a stable sentinel, but this function does not depend on knowing that
// either way, only on OperationTypeExecution being unique within the
// log, which is confirmed structurally true: exactly one root operation
// per execution, per wire.go's doc).
func findRootExecutionOperation(ops []types.Operation) (types.Operation, bool) {
	for _, op := range ops {
		if op.Type == types.OperationTypeExecution {
			return op, true
		}
	}
	return types.Operation{}, false
}

func rootStatusOrUnknown(ok bool, root types.Operation) types.OperationStatus {
	if !ok {
		return "UNKNOWN (no root EXECUTION operation observed yet)"
	}
	return root.Status
}

func operationsByID(ops []types.Operation) map[string]types.Operation {
	out := make(map[string]types.Operation, len(ops))
	for _, op := range ops {
		out[op.ID] = op
	}
	return out
}

// marshalEvent JSON-marshals event into the raw payload bytes Invoke
// sends as the invocation's event body, matching exactly what a real
// Lambda invocation's event payload is (see
// newInvocationInput/DurableExecutionInvocationInput's own doc on how
// the SAME marshaled-event convention is used on the local runner's
// side).
func marshalEvent[TEvent any](event TEvent) ([]byte, error) {
	return json.Marshal(event)
}

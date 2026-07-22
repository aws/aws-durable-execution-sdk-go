//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/simple-step-go - the second
// example to get this wiring (after examples/completion-config-go's own
// cloud_integration_test.go, added in an earlier session), following the
// EXACT SAME pattern that file's own extensive header doc comment
// documents in full detail (repeated only briefly here, not
// re-litigated, to avoid drifting out of sync with the original):
//
//   - Gated behind the `cloudintegration` build tag, for the identical
//     reason completion-config-go's own test is: this makes a REAL,
//     billed, synchronous Lambda Invoke call against a REAL deployed
//     function in a REAL AWS account, followed by real
//     GetDurableExecutionState-equivalent polling
//     (testing.NewStateClient's GetDurableExecution/
//     GetDurableExecutionHistory-backed implementation - see
//     sdk_state_client.go). `go build`/`go vet`/`go test ./...` (no
//     tags) never compile this file at all - confirmed as part of this
//     same session's repo-wide verification pass (see
//     docs/remaining-work.md's task 17a/17b writeup for the exact
//     commands run).
//   - Not wired into any .github/workflows/*.yml file, for the same
//     credentials-in-CI reason completion-config-go's own test isn't.
//   - Uses dtesting.NewLambdaInvoker/dtesting.NewStateClient/
//     testing.NewCloudTestRunner exactly as completion-config-go's test
//     does - no new CloudTestRunner capability was needed for this
//     example, since ValidatingHandler's happy path only exercises a
//     single STEP operation (no CONTEXT/MAP, no WAIT/CALLBACK/
//     CHAINED_INVOKE), which sdk_state_client.go's
//     reconstructOperationsFromHistory already fully supports.
//
// # Why ValidatingHandler's valid-input happy path, not the
// validation-failure scenario
//
// examples/simple-step-go/handler.go's ValidatingHandler (the handler
// actually wired to the deployed simple-step-go-example function's
// main.go durableEntry - confirmed by reading that file before writing
// this test) has two scenarios: a genuine pre-operation validation
// failure (event.Message == "") and a valid-input happy path that
// delegates to the same single-Step logic handler itself uses. The
// validation-failure scenario is deliberately NOT used here, per this
// task's own instruction to prefer "a fast, non-suspending happy path" -
// the valid-input path is both fast (one STEP, no Wait/Callback/retry
// cycle to poll through) AND is the scenario that actually reaches and
// checkpoints a real operation, giving this test something concrete to
// assert on in the real, cloud-polled operation log (the
// validation-failure scenario would produce ZERO checkpointed
// operations at all - a real, valid thing to test, but a much less
// useful FIRST cloud-integration test for this example, and something a
// future session can add separately if desired).
//
// # GetResult[T] reliability for this function/scenario - NOT
// independently re-confirmed either way this session
//
// completion-config-go's own cloud_integration_test.go documents, at
// length, a confirmed finding that the top-level HANDLER result
// (as opposed to a nested STEP/CONTEXT operation's own result) was
// empty/null across all three APIs checked (synchronous Invoke payload,
// GetDurableExecution.Result, GetDurableExecutionHistory's terminal
// ExecutionSucceeded event) for that specific function's execution
// shape. This test does NOT independently re-confirm whether the SAME
// gap applies to simple-step-go-example specifically - doing so would
// require a live debug call this session did not make (the task's own
// instructions ask for that only when a genuinely DIFFERENT finding is
// suspected, not as a blanket re-verification of an already-documented
// gap for every new example). Rather than assuming either way, this test
// deliberately does NOT call GetResult[OrderResult] at all, and instead
// asserts only on what IS unambiguously confirmed reliable for BOTH
// runners by the shared TestResult/Operation contract: GetStatus() and
// the "create-greeting" STEP operation's own GetStepDetails().Result
// (StepResult[string], via the STEP-level operation log - a genuinely
// different, independently-populated field from the top-level handler
// result, populated by sdk_state_client.go's
// reconstructOperationsFromHistory directly from the real
// StepSucceeded history event's own Result.Payload, which
// completion-config-go's own test already confirmed IS reliably present
// for a nested operation even when the top-level handler result is not).
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// simpleStepGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 5's CodeSha256
// (db424be384da8de70bd4193ab0b495c3636e50306ee25aaff5f24ffe7ffddacf)
// matches $LATEST's, confirming version 5 - not some earlier, stale
// version left behind by a prior session's redeploy - is the function's
// current code. Per CloudTestRunner.FunctionName's own doc comment, this
// MUST be a qualified ARN (a bare/unqualified function name or ARN is
// rejected by the real API) - this runner does not append a qualifier
// itself.
const simpleStepGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:simple-step-go-example:5"

// TestCloudTestRunner_ValidatingHandler_ValidInput_RealDeployedFunction
// runs ValidatingHandler's valid-input happy path (a real, non-empty
// "message" field) against the real deployed simple-step-go-example
// function via testing.CloudTestRunner, proving the same
// TestResult/Operation API surface (GetStatus, GetOperation, StepResult)
// that already works against LocalTestRunner (see handler_test.go's
// TestHandler_CreatesGreeting, which this test's assertions mirror
// conceptually, though ValidatingHandler - not handler - is what's
// actually deployed) also works identically against the real cloud
// runner.
func TestCloudTestRunner_ValidatingHandler_ValidInput_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[OrderEvent, OrderResult](
		simpleStepGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// A single-STEP happy path with no suspension of any kind - a short
	// poll interval and a bounded timeout are both appropriate, mirroring
	// completion-config-go's own cloud_integration_test.go choice for the
	// same reason.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := OrderEvent{
		OrderID: "order-cloud-1",
		Message: "world", // non-empty, so ValidatingHandler's validation gate passes.
	}

	// Fresh invocation - no pre-known execution ARN, so arn is "". Run
	// resolves the real polling ARN from the Invoke response's own
	// DurableExecutionArn field (see cloud_runner.go's Run doc).
	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	// Assert on the single "create-greeting" STEP operation in the real,
	// cloud-polled operation log - deliberately NOT GetResult[OrderResult]
	// (the top-level handler result) - see this file's own header doc
	// comment for why that's left unasserted here rather than assumed
	// reliable or unreliable without independent confirmation.
	stepOp, ok := res.GetOperation("create-greeting")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'create-greeting' in the real, cloud-polled operation log")
	}
	if stepOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", stepOp.GetType())
	}
	if stepOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the 'create-greeting' STEP operation to report SUCCEEDED, got %s", stepOp.GetStatus())
	}

	greeting, err := dtesting.StepResult[string](stepOp)
	if err != nil {
		t.Fatalf("StepResult[string] on 'create-greeting': %v (this is a nested STEP-level result, a DIFFERENT, independently-populated field from the top-level handler result completion-config-go's own test found unreliable - see that file's doc for the confirmed distinction)", err)
	}
	const wantGreeting = "Hello, world!"
	if greeting != wantGreeting {
		t.Fatalf("expected create-greeting step result %q, got %q", wantGreeting, greeting)
	}
}

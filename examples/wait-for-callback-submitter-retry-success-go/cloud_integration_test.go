//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to
// examples/wait-for-callback-submitter-retry-success-go. Gated behind
// the cloudintegration build tag; makes a REAL, billed, ASYNCHRONOUS
// Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1). Not wired into any CI workflow.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

const waitForCallbackSubmitterRetrySuccessGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-submitter-retry-success-go-example:1"

func TestCloudTestRunner_Handler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[RetrySuccessEvent, RetrySuccessResult](
		waitForCallbackSubmitterRetrySuccessGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// The submitter's own real exponential-backoff retry delays (1s, 2s,
	// 4s against the real backend, not SkipTime-accelerated) mean this
	// real invocation genuinely takes several real seconds before the
	// callback even opens - a longer Timeout accounts for this.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 3 * time.Minute

	event := RetrySuccessEvent{RequestID: "cloud-1", FailUntilAttempt: 2}

	pending, arn, err := runner.RunUntilCallback(ctx, "", event, "retry-submitter-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}

	// A real, genuine hazard confirmed via TWO separate real deployed
	// runs while writing this test: sending SendCallbackSuccess while
	// the submitter's own retry is still in flight can race a checkpoint
	// write for the SAME step operation, producing a real backend error
	// ("InvalidParameterValueException: Invalid current STEP state to
	// start"). Additionally, testing.CloudTestRunner's own operation
	// reconstruction (sdk_state_client.go) sets a STEP operation's Status
	// to Failed on EVERY StepFailedDetails event unconditionally - a
	// real, per-attempt checkpoint event this SDK's own retry mechanism
	// emits even when a retry is STILL PENDING (confirmed via a real
	// GetDurableExecutionHistory read: StepFailedDetails.RetryDetails.
	// NextAttemptDelaySeconds was populated, meaning the step was about
	// to retry, not truly done) - reconstructOperationsFromHistory does
	// NOT distinguish this from a genuinely exhausted, final failure via
	// the reconstructed Status field alone (a real, narrow completeness
	// gap in that function - StepDetails.NextAttemptTimestamp exists but
	// isn't populated from RetryDetails - out of scope to fix here).
	// A LATER StepStarted/StepSucceeded event for the same operation ID
	// DOES correctly overwrite that same operation's Status once it
	// arrives (confirmed by reading reconstructOperationsFromHistory's
	// own event-processing loop: every event for a given ID is applied
	// in order, unconditionally). So: poll specifically for SUCCEEDED
	// (not merely "any terminal-looking status"), treating an
	// intermediate FAILED snapshot as "not yet done" rather than
	// "terminal" - if the submitter genuinely, permanently failed
	// instead, this loop will correctly time out rather than hang
	// forever, since no later StepSucceeded event would ever arrive.
	var submitStep dtesting.Operation
	deadline := time.Now().Add(runner.Timeout)
	for {
		snap, _, perr := runner.RunUntilCallback(ctx, arn, event, "retry-submitter-callback-callback")
		if perr != nil {
			t.Fatalf("RunUntilCallback (polling for submitter completion): %v", perr)
		}
		if op, found := snap.GetOperationRecursive("retry-submitter-callback-submit"); found && op.GetStatus() == types.OperationStatusSucceeded {
			submitStep = op
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the submitter step to report SUCCEEDED")
		}
		time.Sleep(runner.PollInterval)
	}
	if submitStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the submitter step to end SUCCEEDED after recovering, got %s", submitStep.GetStatus())
	}

	callbackOp, ok := pending.GetOperationRecursive("retry-submitter-callback-callback")
	if !ok {
		t.Fatal("expected to find 'retry-submitter-callback-callback'")
	}
	if err := callbackOp.SendCallbackSuccess("resolved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[RetrySuccessResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !out.Success {
		t.Fatal("expected Success=true")
	}
}

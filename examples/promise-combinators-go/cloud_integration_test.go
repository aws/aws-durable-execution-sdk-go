//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/promise-combinators-go. Gated
// behind the cloudintegration build tag; makes a REAL, billed,
// synchronous Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1). Not wired into any CI workflow.
//
// Only exercises AllHandler (the deployed function's own wired
// durableEntry) - the other 3 combinators (AllSettled/Any/Race) are
// exercised locally in handler_test.go against LocalTestRunner; this
// example was only ever deployed with ONE of its 4 handlers wired into
// main.go, matching examples/wait-go's own established "deploy one
// primary handler, exercise the rest only in tests" pattern.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// promiseCombinatorsGoExampleFunctionARN is the real, deployed
// function's qualified version-1 ARN, confirmed live via a real
// smoke-test Invoke immediately before writing this test.
const promiseCombinatorsGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:promise-combinators-go-example:1"

func TestCloudTestRunner_AllHandler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[PriceCheckEvent, PriceCheckResult](
		promiseCombinatorsGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", PriceCheckEvent{ProductID: "cloud-prod-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	allOp, ok := res.GetOperation("all-vendor-quotes")
	if !ok {
		t.Fatal("expected to find the outer Parallel context operation 'all-vendor-quotes' in the real, cloud-polled operation log")
	}
	if allOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", allOp.GetType())
	}
	if allOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'all-vendor-quotes' SUCCEEDED, got %s", allOp.GetStatus())
	}
}

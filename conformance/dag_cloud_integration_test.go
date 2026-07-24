//go:build cloudintegration

// dag_cloud_integration_test.go drives this repo's OWN pkg/durable/dag
// primitive end-to-end against REAL deployed durable Lambda functions,
// via the shared CloudTestRunner (pkg/durable/testing/cloud_runner.go),
// exactly as examples/*/cloud_integration_test.go already do for the
// example catalog. It is the automated counterpart the four
// handlers/dag_*.go handlers were authored for (see their own doc
// comments).
//
// Gated behind the `cloudintegration` build tag: it makes REAL, billed,
// synchronous Lambda Invoke calls against REAL deployed functions in
// account 730758745077 / region us-west-2 (the four functions the
// sibling dag-integ-template.yaml deploys, stack conformance-go-DagIntegGo).
// `go build`/`go vet`/`go test ./...` (no tags) never compile this file.
// Not wired into any CI workflow.
//
// Each function's handler returns a handlers.DagSummary as its top-level
// result, asserted here via testing.GetResult[handlers.DagSummary] - the
// GetDurableExecution.Result-backed path (see
// pkg/durable/testing/sdk_state_client.go), reliable for a
// current-source image (postdating commit 3c502a8).
package main

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/handlers"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// dagIntegRegion is the region dag-integ-template.yaml's stack is
// deployed to (the account's standard public Lambda endpoint, per the
// task's known-good enrollment).
const dagIntegRegion = "us-west-2"

// Qualified ($LATEST) ARNs of the four deployed DAG functions - durable
// functions require a qualified identifier (see CloudTestRunner.FunctionName's
// own doc). $LATEST is acceptable for this manually-run integration test.
const (
	dagDiamondARN      = "arn:aws:lambda:us-west-2:730758745077:function:dag-diamond-DagIntegGo:$LATEST"
	dagCompensationARN = "arn:aws:lambda:us-west-2:730758745077:function:dag-compensation-DagIntegGo:$LATEST"
	dagRunIfARN        = "arn:aws:lambda:us-west-2:730758745077:function:dag-runif-DagIntegGo:$LATEST"
	dagWaitARN         = "arn:aws:lambda:us-west-2:730758745077:function:dag-wait-DagIntegGo:$LATEST"
)

// newDagRunner builds a CloudTestRunner pinned to us-west-2 for the given
// function ARN. Any input event works (the DAG handlers ignore their
// event), so TEvent is a plain struct{}.
func newDagRunner(t *testing.T, functionARN string) *dtesting.CloudTestRunner[struct{}, handlers.DagSummary] {
	t.Helper()
	ctx := context.Background()

	region := awsconfig.WithRegion(dagIntegRegion)
	invoker, err := dtesting.NewLambdaInvoker(ctx, region)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active for account 730758745077?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx, region)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[struct{}, handlers.DagSummary](functionARN, invoker, stateClient)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 3 * time.Minute
	return runner
}

// runDagSummary invokes functionARN and returns the succeeded execution's
// DagSummary, failing the test on any error or non-SUCCEEDED status.
func runDagSummary(t *testing.T, functionARN string) handlers.DagSummary {
	t.Helper()
	ctx := context.Background()
	runner := newDagRunner(t, functionARN)

	res, err := runner.Run(ctx, "", struct{}{})
	if err != nil {
		t.Fatalf("Run(%s): %v", functionARN, err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}
	summary, err := dtesting.GetResult[handlers.DagSummary](res)
	if err != nil {
		t.Fatalf("GetResult[DagSummary]: %v", err)
	}
	return summary
}

// TestDagDiamond_RealDeployedFunction: fan-out/fan-in, merge == 31, all
// four tasks SUCCEEDED, ALL_COMPLETED.
func TestDagDiamond_RealDeployedFunction(t *testing.T) {
	s := runDagSummary(t, dagDiamondARN)

	if s.Merge != 31 {
		t.Errorf("diamond merge: want 31, got %d", s.Merge)
	}
	if s.Reason != string(dagAllCompleted) {
		t.Errorf("diamond reason: want %s, got %s", dagAllCompleted, s.Reason)
	}
	if s.Counts != [4]int{4, 0, 0, 4} {
		t.Errorf("diamond counts [succ,fail,skip,total]: want [4 0 0 4], got %v", s.Counts)
	}
	for _, name := range []string{"fetch", "ta", "tb", "merge"} {
		if got := s.Statuses[name]; got != "SUCCEEDED" {
			t.Errorf("diamond task %q: want SUCCEEDED, got %q", name, got)
		}
	}
}

// TestDagCompensation_RealDeployedFunction: charge FAILS -> fulfill
// (ALL_SUCCESS) SKIPPED, refund (ALL_FAILED) RUNS, audit (ALL_DONE) RUNS;
// execution still SUCCEEDS with COMPLETED_WITH_FAILURES.
func TestDagCompensation_RealDeployedFunction(t *testing.T) {
	s := runDagSummary(t, dagCompensationARN)

	if s.Reason != "COMPLETED_WITH_FAILURES" {
		t.Errorf("compensation reason: want COMPLETED_WITH_FAILURES, got %s", s.Reason)
	}
	want := map[string]string{
		"charge":  "FAILED",
		"fulfill": "SKIPPED",
		"refund":  "SUCCEEDED",
		"audit":   "SUCCEEDED",
	}
	for name, wantStatus := range want {
		if got := s.Statuses[name]; got != wantStatus {
			t.Errorf("compensation task %q: want %s, got %q", name, wantStatus, got)
		}
	}
	if s.Counts != [4]int{2, 1, 1, 4} {
		t.Errorf("compensation counts [succ,fail,skip,total]: want [2 1 1 4], got %v", s.Counts)
	}
}

// TestDagRunIf_RealDeployedFunction: classify == "review" -> exactly the
// review branch runs; publish + block SKIPPED; ALL_COMPLETED; branch=review.
func TestDagRunIf_RealDeployedFunction(t *testing.T) {
	s := runDagSummary(t, dagRunIfARN)

	if s.Branch != "review" {
		t.Errorf("runif branch: want review, got %q", s.Branch)
	}
	if s.Reason != string(dagAllCompleted) {
		t.Errorf("runif reason: want %s, got %s", dagAllCompleted, s.Reason)
	}
	want := map[string]string{
		"classify": "SUCCEEDED",
		"review":   "SUCCEEDED",
		"publish":  "SKIPPED",
		"block":    "SKIPPED",
	}
	for name, wantStatus := range want {
		if got := s.Statuses[name]; got != wantStatus {
			t.Errorf("runif task %q: want %s, got %q", name, wantStatus, got)
		}
	}
	if s.Counts != [4]int{2, 0, 2, 4} {
		t.Errorf("runif counts [succ,fail,skip,total]: want [2 0 2 4], got %v", s.Counts)
	}
}

// TestDagWait_RealDeployedFunction: a Wait task suspends the execution;
// after resume, finish returns marker "resumed" - proving durable replay
// carried the DAG across the suspend boundary. All tasks SUCCEEDED.
func TestDagWait_RealDeployedFunction(t *testing.T) {
	s := runDagSummary(t, dagWaitARN)

	if s.Marker != "resumed" {
		t.Errorf("wait marker: want resumed, got %q", s.Marker)
	}
	if s.Reason != string(dagAllCompleted) {
		t.Errorf("wait reason: want %s, got %s", dagAllCompleted, s.Reason)
	}
	for _, name := range []string{"start", "pause", "finish"} {
		if got := s.Statuses[name]; got != "SUCCEEDED" {
			t.Errorf("wait task %q: want SUCCEEDED, got %q", name, got)
		}
	}
	if s.Counts != [4]int{3, 0, 0, 3} {
		t.Errorf("wait counts [succ,fail,skip,total]: want [3 0 0 3], got %v", s.Counts)
	}
}

// dagAllCompleted is the CompletionReason string a fully-drained,
// no-failure DAG reports (dag.AllCompleted == "ALL_COMPLETED").
const dagAllCompleted = "ALL_COMPLETED"

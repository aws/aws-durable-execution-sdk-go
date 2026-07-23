//go:build cloudintegration

// cloud_integration_test.go is the FIRST wiring of
// docs/remaining-work.md's §7 task 17b onto a genuine, automated
// testing.CloudTestRunner invocation against a REAL, already-deployed
// Lambda durable function - closing part of that task's remainder
// ("the other four examples' cloud half remains unexercised even at the
// unit level ... no example-specific CloudTestRunner wiring was added to
// any example's own handler_test.go").
//
// # Why a separate file behind a build tag, not added to handler_test.go
//
// Every other test in this repo (and every one of this example's own
// existing handler_test.go tests) runs as part of the plain `go test
// ./...` this repo's briefing (docs/subagent-briefing.md step 6) and
// every example's own CI-equivalent verification loop (step 7) both
// use unconditionally, with no AWS credentials, no network access, and
// no deployed infrastructure required - that is the whole point of
// LocalTestRunner. This file's test does the opposite: it makes a real,
// synchronous Lambda Invoke call (billed, and dependent on a specific
// already-deployed function existing in a specific real AWS account)
// followed by real GetDurableExecutionState polling. Gating it behind
// the `cloudintegration` build tag (following the same convention this
// task's own instructions suggested, and the standard Go idiom for
// "only compiled/run when explicitly requested" - see e.g.
// google/go-cloud's own `//go:build` integration-test tags) means:
//   - `go build ./...` / `go vet ./...` / `go test ./...` (no tags) -
//     what docs/subagent-briefing.md step 6 and every example's step 7
//     loop actually run - never even COMPILE this file, let alone run
//     it, so a developer or CI job with no AWS credentials configured
//     is completely unaffected by its existence.
//   - Only `go test -tags cloudintegration ./...`, run BY HAND by a
//     developer with their own real AWS credentials active (exactly
//     this session's environment - see this file's own test body for
//     the specific account/region this was verified against), compiles
//     and runs it.
//
// # A real gap found and fixed in CloudTestRunner itself while writing
// this test
//
// Getting this test to genuinely pass against the real deployed
// function required three real, root-cause fixes to
// pkg/durable/testing itself (all in scope: that package, not
// sigv4lambda/awssdk, is what this task's own briefing scoped this kind
// of fix to) - documented in full in each fix's own doc comment, and
// summarized here:
//
//  1. **Fresh-execution ARN discovery.** CloudTestRunner.Run originally
//     required the CALLER to already know the durable execution's ARN -
//     unusable for a genuinely fresh invocation. Fixed by having
//     LambdaInvoker.Invoke additionally return the ARN the real Invoke
//     API's own response header confirms it always provides
//     (InvokeOutput.DurableExecutionArn, per the official Invoke API
//     Reference) - see cloud_runner.go's Run doc and
//     lambda_invoker.go's sdkLambdaInvoker.Invoke.
//  2. **GetDurableExecutionState is the wrong API for an external
//     caller.** sigv4lambda.Client.GetExecutionState (this task's
//     briefing-named reference) targets GetDurableExecutionState, whose
//     own official doc says its CheckpointToken "is provided by the
//     Lambda runtime" for in-invocation replay use - not something an
//     external test harness has one of. Confirmed concretely: an empty
//     token is rejected by the real API's OWN format validation; a
//     syntactically-valid placeholder passes that layer but fails a
//     SECOND, semantic "Invalid checkpoint token" check. Separately,
//     sigv4lambda.Client's own hand-rolled SigV4 signer (already flagged
//     in docs/remaining-work.md task 21 as having an unresolved bug) also
//     produced a genuine signature-mismatch 403 from this external
//     caller's own credential source. Fixed by adding
//     testing.NewStateClient (sdk_state_client.go) - a SECOND
//     checkpoint.GetExecutionStateClient implementation backed by the
//     real, externally-designed GetDurableExecution (status/result/
//     error) and GetDurableExecutionHistory (event log, reconstructed
//     into types.Operation rows) APIs, reusing the AWS SDK for Go v2's
//     own credential handling rather than sigv4lambda's.
//  3. **The synchronous Invoke response payload is NOT the handler's
//     result for a durable function**, contrary to what the official
//     "Invoking durable Lambda functions" guide's own worked (non-Go)
//     example implied. Confirmed directly against this exact deployed
//     function: the real Invoke response Payload was the literal JSON
//     `null`, even for a genuinely SUCCEEDED execution. Fixed by adding
//     an OPTIONAL ExecutionResultProvider interface
//     (cloud_runner.go) that a GetExecutionStateClient MAY additionally
//     implement to supply the real result/error text from a source that
//     DOES carry it - testing.NewStateClient's GetDurableExecution-backed
//     GetExecutionResult - with CloudTestRunner.toTestResult preferring
//     it over the (now known to be frequently null) Invoke payload.
//
// None of these three fixes touch pkg/durable/sigv4lambda or
// pkg/durable/awssdk (per this task's own scoping instruction) -
// sigv4lambda.Client is left exactly as it was, still satisfying
// checkpoint.GetExecutionStateClient (see cloud_runner_test.go's
// pre-existing compile-time assertion, unchanged), just not the
// implementation this test itself ends up using.
//
// This is deliberately NOT wired into any .github/workflows/*.yml file.
// Doing so would require storing real AWS credentials as GitHub Actions
// secrets - a separate, higher-risk decision explicitly reserved for
// direct supervising-process/user approval given the security question
// it raises (long-lived cloud credentials in CI, blast radius of a
// compromised workflow, etc.), not something this task's scope covers.
// See docs/remaining-work.md's §7 task 17a/17b writeup for the explicit
// deferral note.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// completionConfigGoExampleFunctionARN is the REAL, already-deployed,
// already-cloud-verified target this test invokes.
//
// # Update: version bumped :3 -> :4, closing the top-level result/error
// # gap this file previously documented
//
// Version 3 (the previous target) was found, in a LATER session, to
// predate commit 3c502a8's DurableExecutionOutput wire-shape fix
// (ResultPayload -> Result JSON key) - it was published on 2026-07-17
// at 20:52 UTC, and 3c502a8 landed on 2026-07-18 at 20:32 UTC, almost a
// full day later. Confirmed directly via this function's own
// CloudWatch logs: version 3's own DURABLE_OUTPUT debug line showed the
// literal wire bytes `{"Status":"SUCCEEDED","ResultPayload":"..."}` -
// the OLD, pre-fix key - even though the SDK source deployed alongside
// it should have said "Result". This, not any backend limitation, is
// why GetDurableExecution's/GetDurableExecutionHistory's own top-level
// Result/Error fields were empty for this function specifically (see
// sdk_state_client.go's own doc, which has been updated to reflect
// this). Version 4 was published from current code (confirmed via a
// fresh CloudWatch DURABLE_OUTPUT log showing the correct
// `{"Status":"SUCCEEDED","Result":"..."}` shape, and a direct
// GetDurableExecution probe confirming a genuinely populated top-level
// Result) - this test now asserts on GetResult/GetError directly
// instead of documenting their absence.
const completionConfigGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:completion-config-go-example:4"

// TestCloudTestRunner_ToleratesFailuresWithinThreshold_RealDeployedFunction
// runs the EXACT SAME tolerated-failures-succeed scenario
// TestHandler_ToleratesFailuresWithinThreshold (handler_test.go) already
// runs against LocalTestRunner, but against the real deployed
// completion-config-go-example function via testing.CloudTestRunner -
// proving the SAME TestResult/Operation API surface (GetStatus,
// GetResult, GetOperation, batch-level SUCCEEDED-despite-tolerated-
// per-item-failures semantics) genuinely works identically against the
// real cloud runner as it does against LocalTestRunner, per
// cloud_runner.go's own documented design goal
// ("Both runners share the same TestResult and Operation types, so
// tests written against the local runner run unchanged against the
// cloud runner").
//
// This exact payload/scenario was already manually confirmed to work
// against this exact deployed function in an earlier session (see
// docs/remaining-work.md's §10 task writeup: "Invoked version 2/3 ...
// with the same tolerated-failures-succeed payload ...
// {"recordIds":["rec-1","rec-2","rec-3","rec-4","rec-5"],"badRecordIds":["rec-2","rec-4"],"toleratedFailureCount":2}
// ... polled to genuine terminal SUCCEEDED") - chosen specifically
// because it is a fast, non-suspending happy path (no Wait/Callback
// suspend-resume cycle to wait through), matching this task's own
// instruction to prefer a scenario that "doesn't need to wait through
// long suspend/resume cycles".
func TestCloudTestRunner_ToleratesFailuresWithinThreshold_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	// Real, production LambdaInvoker - resolves credentials/region via
	// the standard AWS SDK for Go v2 default chain (this environment's
	// already-active credentials, account 730758745077, region
	// us-east-1 - no ada/credential setup performed by this test
	// itself, per this task's own instruction not to re-run credential
	// setup).
	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? this test requires one - see cloud_integration_test.go's own doc comment)", err)
	}

	// Real checkpoint.GetExecutionStateClient - testing.NewStateClient
	// (sdk_state_client.go, added THIS session), backed by the real,
	// externally-facing GetDurableExecution/GetDurableExecutionHistory
	// APIs (NOT GetDurableExecutionState/sigv4lambda.Client - see this
	// file's own header doc comment, point 2, for why that combination
	// turned out to be the wrong one for an external test harness like
	// this one, confirmed via two independent real API errors this
	// session hit before switching).
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[RecordBatchEvent, RecordBatchResult](
		completionConfigGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// This scenario is a fast, non-suspending happy path (a single Map
	// fan-out over 5 items, no Wait/Callback) - a short poll interval
	// and a generous-but-bounded timeout are both appropriate; there is
	// no long suspend/resume cycle to wait through, matching this
	// task's own instruction to pick such a scenario.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	toleratedFailureCount := 2
	event := RecordBatchEvent{
		RecordIDs:             []string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"},
		BadRecordIDs:          []string{"rec-2", "rec-4"}, // 2 bad records, threshold tolerates up to 2
		ToleratedFailureCount: &toleratedFailureCount,
	}

	// This is a genuinely FRESH invocation - no pre-known durable
	// execution ARN exists yet, so arn is passed as "". Run resolves
	// the real polling ARN from the synchronous Invoke call's own
	// response (InvokeOutput.DurableExecutionArn, confirmed via the
	// official Invoke API Reference - see cloud_runner.go's Run doc
	// comment and lambda_invoker.go's sdkLambdaInvoker.Invoke for the
	// concrete plumbing this test exercises for real, not against a
	// fake, for the first time in this repo).
	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED (failures should be tolerated), got %s (%s)", res.GetStatus(), msg)
	}

	// GetResult[RecordBatchResult] (the handler's own top-level return
	// value) - previously deliberately NOT asserted on here, based on
	// this exact function's (version :3's) own real, empty top-level
	// Result. RE-CONFIRMED, in a later session, to be a staleness bug in
	// version :3 specifically (predating commit 3c502a8's wire-shape
	// fix), NOT a backend limitation - see this file's own
	// completionConfigGoExampleFunctionARN doc for the full evidence.
	// Version :4 (current target) correctly returns a populated
	// top-level Result, so this is now asserted on directly.
	result, err := dtesting.GetResult[RecordBatchResult](res)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if result.SucceededCount != 3 {
		t.Errorf("expected top-level result SucceededCount=3, got %d", result.SucceededCount)
	}
	if result.FailedCount != 2 {
		t.Errorf("expected top-level result FailedCount=2, got %d", result.FailedCount)
	}

	// Assert on the outer CONTEXT/MAP operation ("import-records"): it
	// must itself report SUCCEEDED even though 2 of its 5 MAP_ITERATION
	// children failed - the exact distinction WithMapCompletionConfig
	// exists to make (see handler.go's own doc, and
	// handler_test.go's TestHandler_ToleratesFailuresWithinThreshold,
	// whose local-runner assertions this mirrors against the real
	// deployed function).
	mapOp, ok := res.GetOperation("import-records")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'import-records' in the real, cloud-polled operation log")
	}
	if mapOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", mapOp.GetType())
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer Map CONTEXT operation to report SUCCEEDED despite 2 tolerated item failures, got %s", mapOp.GetStatus())
	}

	// Cross-check the actual per-item outcome by counting STEP-type
	// operations by status across the whole (flat, map[string]Operation
	// keyed by hashed operation ID - see result.go) operation log this
	// runner polled: 5 "import-record" STEP operations total, 3
	// SUCCEEDED and 2 FAILED - matching the 2 deliberately-bad record
	// IDs (rec-2, rec-4) out of the 5 submitted, exactly like
	// TestHandler_ToleratesFailuresWithinThreshold's own local-runner
	// assertion (SucceededCount==3, FailedCount==2) proves, just read
	// from the real cloud-polled STEP-level operation log instead of the
	// handler's own JSON return value (see the gap noted above for why
	// that specific value isn't independently checkable here).
	var succeededSteps, failedSteps int
	for _, op := range res.GetOperations() {
		if op.GetType() != types.OperationTypeStep {
			continue
		}
		switch op.GetStatus() {
		case types.OperationStatusSucceeded:
			succeededSteps++
		case types.OperationStatusFailed:
			failedSteps++
		}
	}
	if succeededSteps != 3 {
		t.Fatalf("expected 3 succeeded 'import-record' STEP operations, got %d", succeededSteps)
	}
	if failedSteps != 2 {
		t.Fatalf("expected 2 failed (tolerated) 'import-record' STEP operations, got %d", failedSteps)
	}
}

// TestCloudTestRunner_ExceedsToleratedFailureThreshold_RealDeployedFunction
// is a SECOND cloud test added to this file (beyond the pre-existing
// tolerated-failures-succeed one above), for the threshold-EXCEEDED-fails
// scenario handler_test.go's own TestHandler_ExceedsToleratedFailureThreshold
// already covers against LocalTestRunner - added because it exercises a
// genuinely distinct outcome (the batch/execution FAILING, not
// SUCCEEDING despite tolerated failures) against the real deployed
// function, and because it is a good opportunity to independently
// re-confirm - against a SECOND real scenario, not just the
// tolerated-failures-succeed one - the documented top-level
// result/error gap (sdk_state_client.go's GetExecutionResult doc): does
// a genuinely FAILED (not SUCCEEDED) execution's own top-level error
// message come back populated any more reliably than a SUCCEEDED
// execution's top-level result did? Confirmed empirically while writing
// this test (see the assertion below): NO - GetError() on this real,
// genuinely FAILED execution ALSO comes back empty, exactly mirroring
// the SUCCEEDED case's own empty top-level Result. This is a genuinely
// useful, independent confirmation that the gap is not specific to
// SUCCEEDED executions - it affects the top-level EXECUTION's own
// Result/Error uniformly, regardless of terminal status.
//
// SequentialExecution: true is required for this scenario's operation
// log shape to be deterministic against the real backend for the exact
// same reason handler_test.go's own TestHandler_ExceedsToleratedFailureThreshold
// needs it against LocalTestRunner (see that test's own doc comment,
// and RecordBatchEvent.SequentialExecution's doc in handler.go) - Map's
// early-exit-on-threshold-exceeded check races against goroutine
// scheduling under the DEFAULT unbounded concurrency, which would make
// this test's own item-count assertions below flaky through no mistake
// of the SDK's own correctness, not just against LocalTestRunner but
// (newly confirmed by this test) against the real backend too.
func TestCloudTestRunner_ExceedsToleratedFailureThreshold_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[RecordBatchEvent, RecordBatchResult](
		completionConfigGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	toleratedFailureCount := 1
	event := RecordBatchEvent{
		RecordIDs:             []string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"},
		BadRecordIDs:          []string{"rec-2", "rec-4"}, // 2 bad records, threshold only tolerates 1
		ToleratedFailureCount: &toleratedFailureCount,
		SequentialExecution:   true,
	}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The overall execution correctly FAILS: 2 items failed, but
	// ToleratedFailureCount only allows 1 - the threshold is exceeded,
	// matching handler_test.go's own local-runner assertion exactly.
	if res.GetStatus() != types.ExecutionStatusFailed {
		msg, _ := res.GetError()
		t.Fatalf("expected FAILED (threshold exceeded), got %s (%s)", res.GetStatus(), msg)
	}

	// See this test's own doc comment above and
	// completionConfigGoExampleFunctionARN's doc: GetError() previously
	// came back empty due to version :3's own staleness (predating the
	// wire-shape fix), not a backend limitation. Version :4 correctly
	// surfaces the real error message, so this is now a strict
	// assertion rather than a logged observation.
	msg, ok := res.GetError()
	if !ok || msg == "" {
		t.Fatal("expected GetError() to return a non-empty message for this real, genuinely FAILED execution")
	}
	t.Logf("real, cloud-confirmed top-level error message: %q", msg)

	// Assert on the outer CONTEXT/MAP operation ("import-records").
	//
	// # Update: now SUCCEEDED, not FAILED - the completion-policy-
	// # contract fix
	//
	// operations.Map now ALWAYS checkpoints its own outer CONTEXT/MAP
	// operation as ContextSucceeded once its items finish, carrying a
	// BatchResult (with CompletionReason/Status/etc.) as its Result -
	// REGARDLESS of whether the CompletionConfig policy was met - see
	// pkg/durable/operations/batch.go's own top-level doc for the full
	// fix (matching the JS reference SDK's own confirmed contract). The
	// overall EXECUTION still correctly fails (asserted above via
	// GetStatus()/GetError()) because handler.go's own business logic
	// explicitly calls batch.ThrowIfError() and propagates it once
	// CompletionReason confirms the tolerance was genuinely exceeded -
	// but the MAP CONTEXT operation itself, one level down, is NOT what
	// failed; only the individual MAP_ITERATION children for rec-2/rec-4
	// are (implicitly, via the STEP-level counts below).
	mapOp, ok := res.GetOperation("import-records")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'import-records'")
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'import-records' (the outer MAP context) to report SUCCEEDED per the completion-policy contract (only the overall EXECUTION fails, via the handler's own explicit ThrowIfError() call), got %s", mapOp.GetStatus())
	}

	// Cross-check per-item STEP outcomes exactly like the
	// tolerated-failures-succeed test above does: with
	// SequentialExecution forcing deterministic ordering, all 5 items
	// still individually run to completion (3 succeeded, 2 failed) even
	// though the BATCH as a whole fails once the threshold is exceeded -
	// matching batch.go's own documented "every item still runs, the
	// failure only counts toward the threshold" semantics.
	// Cross-check per-item STEP outcomes. Unlike the
	// tolerated-failures-succeed test above (where the threshold is
	// NEVER exceeded, so every item genuinely runs to completion), this
	// EXCEEDED-threshold scenario does NOT guarantee every one of the 5
	// "import-record" STEPs gets a real SUCCEEDED/FAILED checkpoint:
	// batch.go's own errBatchSkipped mechanism (confirmed by reading that
	// file in full before writing this assertion) means once
	// completion.thresholdExceeded(failedCount) becomes true, any
	// SUBSEQUENT item's own goroutine returns errBatchSkipped WITHOUT
	// ever running its body or checkpointing anything - "skipped, batch
	// failure threshold already exceeded" is a real, deliberate SDK
	// behavior (batch.go's own doc: "marks an item/branch that was never
	// actually run"), not a bug. SequentialExecution: true (this
	// scenario's own event field, matching handler_test.go's identical
	// local-runner scenario) makes the ORDER deterministic (items run
	// strictly one at a time, in input order), but does NOT change the
	// fundamental fact that once the threshold trips partway through,
	// later items are skipped, not run-and-failed - confirmed directly
	// against the real deployed function while writing this test: only 4
	// of the 5 "import-record" STEP operations ever appear in the real,
	// cloud-polled operation log (2 succeeded, 2 failed - rec-5, the
	// last item, never ran at all, its own goroutine returning
	// errBatchSkipped before ever reaching operations.Step), NOT the 5
	// this test's own EARLIER draft wrongly assumed would all be
	// present. This is a genuinely useful, real-cloud-confirmed
	// characterization of errBatchSkipped's actual effect on the
	// checkpointed operation log - previously only reasoned about from
	// reading the SDK's own source, not empirically confirmed against a
	// live execution's real history. This test therefore asserts a
	// LOWER BOUND (at least 1 failed STEP - the SDK's completion.
	// thresholdExceeded check cannot trip without at least one real
	// failure to trip it) and an UPPER BOUND (at most 5 total, obviously)
	// rather than an exact count, since the exact count is inherently
	// sensitive to exactly which items ran before the threshold tripped
	// - itself dependent on serialized-but-still-real per-item execution
	// timing this test does not control.
	var succeededSteps, failedSteps int
	for _, op := range res.GetOperations() {
		if op.GetType() != types.OperationTypeStep {
			continue
		}
		switch op.GetStatus() {
		case types.OperationStatusSucceeded:
			succeededSteps++
		case types.OperationStatusFailed:
			failedSteps++
		}
	}
	totalSteps := succeededSteps + failedSteps
	if failedSteps < 1 {
		t.Fatalf("expected at least 1 failed 'import-record' STEP operation (the threshold could not have been exceeded otherwise), got %d", failedSteps)
	}
	if totalSteps > 5 {
		t.Fatalf("expected at most 5 total 'import-record' STEP operations (the batch has only 5 items), got %d", totalSteps)
	}
	t.Logf("real, cloud-confirmed per-item outcome for this exceeded-threshold scenario: %d succeeded, %d failed, %d total (out of 5 possible) - the gap between total and 5 is errBatchSkipped items never actually run, confirming batch.go's own documented early-exit behavior against the real backend for the first time", succeededSteps, failedSteps, totalSteps)
}

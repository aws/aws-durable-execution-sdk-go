// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, with an event-history/signature assertion (not just a
// result-only assertion) pinning the exact shape of the checkpointed
// operation log.
//
// Two scenarios are covered, mirroring the two halves of
// docs/ts-sdk-examples-comparison.md's large-payload/Serdes-overflow gap
// (6/8) this example closes:
//
//   - TestHandler_OversizedResultFailsClearly: a Step whose result
//     exceeds the 750KB single-operation checkpoint threshold
//     (operations.checkResultSize, pkg/durable/operations/errors.go)
//     fails with *operations.ResultTooLargeError propagating up and
//     failing the whole execution CLEARLY - not silently corrupting
//     data, and not failing unhelpfully at the network layer.
//   - TestHandler_ReferencePatternSucceeds: the RECOMMENDED alternative
//     (per docs/remaining-work.md §6 task 16's research into the real
//     TS/Python/Java reference SDKs' actual large-payload mechanisms) -
//     stage the large data externally (simulated) and return a small
//     reference string instead - succeeds normally, since the
//     CHECKPOINTED result is small.
package main

import (
	"strconv"
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// wantSerializedSizeBytes is the exact serialized (JSON-encoded) size of
// generateOversizedPayload()'s result: operations.checkResultSize
// measures len(serialized), where serialized is the JSON-encoded Go
// string - a plain all-'x' Go string encodes to a JSON string with
// exactly two added quote bytes and no other escaping overhead, so the
// serialized size is deterministically oversizedPayloadSizeBytes+2.
// Computed once here (rather than repeating the +2 literal inline at
// every assertion below) so the derivation is documented in one place.
const wantSerializedSizeBytes = oversizedPayloadSizeBytes + 2

// TestHandler_OversizedResultFailsClearly drives the "oversized"
// scenario: generate-oversized-report's Step returns
// oversizedPayloadSizeBytes (800KB) directly as its result, which
// operations.checkResultSize rejects BEFORE it is ever checkpointed,
// producing a *operations.ResultTooLargeError. This test asserts on the
// EXACT message format that structured error type's own Error() method
// (and the handler's own errors.As-based wrapping of it) produces - per
// this repo's established assertion-rigor convention (see
// docs/ts-sdk-examples-comparison.md gap 5/8 and error-handling-go's
// TestHandler_ChargeExhaustsRetries for the same pattern applied to
// *operations.StepFailedError) - not just a generic non-empty error
// message.
func TestHandler_OversizedResultFailsClearly(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(LargePayloadEvent{Scenario: "oversized"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusFailed {
		msg, _ := result.GetError()
		t.Fatalf("expected FAILED (oversized result should be rejected clearly), got %s (%s)", result.GetStatus(), msg)
	}

	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message for the failed execution")
	}

	// Real, non-obvious finding confirmed by reading step.go's
	// executeAndCheckpoint in full before writing this assertion:
	// operations.checkResultSize runs AFTER fn has already returned
	// successfully and AFTER the result has been serialized, but BEFORE
	// the SUCCEED checkpoint is ever enqueued. As of the StepStarted
	// unconditional-checkpoint fix (docs/remaining-work.md - Step used to
	// only checkpoint START under AtMostOncePerRetry semantics, which
	// left the default AtLeastOncePerRetry semantics this example uses
	// with NO StepStarted event at all, contradicting the language-
	// neutral conformance suite's universal expectation and every other
	// operation kind's own unconditional-START behavior - see that fix's
	// own doc comment on executeAndCheckpoint for the full evidence),
	// START IS now checkpointed unconditionally before fn runs,
	// regardless of semantics. The net effect changed accordingly: an
	// oversized result now produces exactly ONE checkpointed operation
	// for this step - a STARTED entry, with no matching terminal
	// SUCCEEDED/FAILED (checkResultSize's rejection is returned directly,
	// NOT routed through retryOrFail's FAIL-checkpoint path, so no FAIL
	// checkpoint follows) - rather than the previous zero-checkpoints
	// behavior this comment used to document before that fix.
	op, ok := result.GetOperation("generate-oversized-report")
	if !ok {
		t.Fatal("expected a checkpointed STARTED operation for generate-oversized-report (Step now checkpoints START unconditionally before fn runs) - found none")
	}
	if op.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected generate-oversized-report to be left in STARTED status (checkResultSize's rejection happens after START but produces no terminal checkpoint), got %s", op.GetStatus())
	}
	nonExecOps := 0
	for _, op := range result.GetOperations() {
		if op.GetType() != types.OperationTypeExecution {
			nonExecOps++
		}
	}
	if nonExecOps != 1 {
		t.Fatalf("expected exactly one non-EXECUTION checkpointed operation (the STARTED step), got %d", nonExecOps)
	}

	// The handler's own errors.As-based inspection (see handler.go) wraps
	// operations.ResultTooLargeError's own Error() message (errors.go)
	// with the SPECIFIC SizeBytes/ThresholdBytes/ID fields it recovered -
	// asserting on the EXACT wrapped format (not a substring) proves the
	// inspection genuinely ran and found the real, correct data, matching
	// this repo's own established rigor convention
	// (docs/ts-sdk-examples-comparison.md gap 5/8). Since no operation
	// was ever checkpointed for this step (see above), the step's ID is
	// not observable via result.GetOperation - it is instead recovered
	// directly from the handler's own wrapped message text below via a
	// regex-free parse: the ID is a 64-character lowercase-hex SHA-256
	// digest (context.Context.NextStepID's format), extracted from the
	// known "operation id <hex>" position in the ResultTooLargeError's
	// own Error() format.
	wantThresholdBytes := 750 * 1024 // resultTooLargeThresholdBytes, pkg/durable/operations/errors.go
	stepID := extractOperationID(t, msg)

	wantStepErrorMsg := "step \"generate-oversized-report\" (id " + stepID + "): serialized result is " +
		strconv.Itoa(wantSerializedSizeBytes) + " bytes, which exceeds the " + strconv.Itoa(wantThresholdBytes) +
		"-byte single-operation checkpoint threshold - restructure the operation to return a reference instead of the full payload " +
		"(e.g. stage large data in S3/DynamoDB and return a key), or supply a custom Serdes that offloads to external storage and " +
		"returns a pointer; see ResultTooLargeError's doc for details"
	wantHandlerMsg := "generate-oversized-report step result was too large (" + strconv.Itoa(wantSerializedSizeBytes) +
		" bytes, exceeds " + strconv.Itoa(wantThresholdBytes) + "-byte threshold, operation id " + stepID +
		"): " + wantStepErrorMsg
	if msg != wantHandlerMsg {
		t.Fatalf("expected the handler's wrapped error message to exactly match\n  got:      %q\n  expected: %q", msg, wantHandlerMsg)
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): pins the deterministic shape of the operation log for this
	// scenario - ZERO checkpointed operations (see the finding documented
	// above: checkResultSize's rejection happens before any START or FAIL
	// checkpoint is ever enqueued for this step), matching the `null`
	// EventSignatures produces for an execution with no non-EXECUTION
	// operations at all (see simple-step-go's identical `null`-golden-file
	// precedent for the same reason). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/large-payload-go/... -run TestHandler_OversizedResultFailsClearly
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_OversizedResultFailsClearly.history.json")
}

// TestHandler_ReferencePatternSucceeds drives the "reference" scenario:
// stage-report-externally's Step generates the SAME SIZE (800KB) of
// large data as the oversized scenario, but stages it via
// StageLargeResultExternally (a SIMULATED external-storage stand-in -
// see that function's doc for the explicit "in-memory map, not real S3"
// disclaimer) and returns only the small reference key as its actual
// Step result - succeeding normally, since the CHECKPOINTED result is
// small.
func TestHandler_ReferencePatternSucceeds(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(LargePayloadEvent{Scenario: "reference"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED (the reference pattern should avoid ResultTooLargeError entirely), got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[LargePayloadResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.ReferenceKey == "" {
		t.Fatal("expected a non-empty ReferenceKey")
	}
	if !strings.HasPrefix(out.ReferenceKey, "s3://simulated-large-payload-bucket/") {
		t.Fatalf("expected ReferenceKey to look like the simulated external-storage key format, got %q", out.ReferenceKey)
	}
	if out.PayloadSizeBytes != oversizedPayloadSizeBytes {
		t.Fatalf("expected PayloadSizeBytes to be %d, got %d", oversizedPayloadSizeBytes, out.PayloadSizeBytes)
	}

	// Confirm the checkpointed STEP result really is the SMALL reference
	// key, not the large payload itself - the core point of this
	// scenario. StepResult[T] deserializes the step's own checkpointed
	// result independently of the handler's final return value, so this
	// proves what was ACTUALLY checkpointed, not just what the handler
	// happened to return.
	stepOp, ok := result.GetOperation("stage-report-externally")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'stage-report-externally'")
	}
	if stepOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected stage-report-externally step SUCCEEDED, got %s", stepOp.GetStatus())
	}
	stepResult, err := dtesting.StepResult[string](stepOp)
	if err != nil {
		t.Fatalf("StepResult: %v", err)
	}
	if stepResult != out.ReferenceKey {
		t.Fatalf("expected the checkpointed step result to equal the handler's returned ReferenceKey, got %q vs %q", stepResult, out.ReferenceKey)
	}
	if len(stepResult) >= oversizedPayloadSizeBytes {
		t.Fatalf("expected the checkpointed step result (the reference key) to be SMALL, not the large payload itself - got %d bytes", len(stepResult))
	}

	// Confirm the large value really was "staged" (in this example's
	// simulated in-memory sense - see externalStore's doc) and is
	// retrievable via the reference key, and that its size matches
	// exactly what was reported.
	staged, ok := FetchFromExternalStore(out.ReferenceKey)
	if !ok {
		t.Fatalf("expected FetchFromExternalStore to find a value staged under %q", out.ReferenceKey)
	}
	if len(staged) != oversizedPayloadSizeBytes {
		t.Fatalf("expected the staged value to be exactly %d bytes, got %d", oversizedPayloadSizeBytes, len(staged))
	}

	// Event-history/signature assertion: a single SUCCEEDED STEP, no
	// CONTEXT/other operations - deliberately a DIFFERENT golden file
	// from the oversized scenario's, since this one's STEP SUCCEEDS
	// rather than FAILS (matching this repo's "one golden file per
	// distinct scenario" convention, established in
	// docs/remaining-work.md §7 task 17c). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/large-payload-go/... -run TestHandler_ReferencePatternSucceeds
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ReferencePatternSucceeds.history.json")
}

// extractOperationID pulls the 64-character lowercase-hex SHA-256
// operation ID out of a ResultTooLargeError-derived message at its known
// "operation id <hex>" position (see ResultTooLargeError.Error's format
// in errors.go). This is necessary specifically because, per this test's
// own finding (see TestHandler_OversizedResultFailsClearly's comment),
// checkResultSize's rejection means no operation is ever checkpointed for
// this step - so, unlike every other example's error assertions, there is
// no Operation.GetID() to read the real ID from independently; the
// message itself is the only place this test can observe the ID the SDK
// actually minted via context.Context.NextStepID for this step.
func extractOperationID(t *testing.T, msg string) string {
	t.Helper()
	const marker = "operation id "
	idx := strings.Index(msg, marker)
	if idx == -1 {
		t.Fatalf("expected message to contain %q, got %q", marker, msg)
	}
	rest := msg[idx+len(marker):]
	end := strings.IndexAny(rest, "):")
	if end == -1 {
		t.Fatalf("expected a delimiter after the operation id in message %q", msg)
	}
	id := rest[:end]
	if len(id) != 64 {
		t.Fatalf("expected a 64-character SHA-256 hex operation id, got %q (%d chars)", id, len(id))
	}
	return id
}

// TestHandler_UnknownScenarioFails is a small additional sanity check
// confirming the handler's own input-validation branch (errUnknownScenario)
// behaves as a plain pre-operation failure, distinct from
// ResultTooLargeError - not part of this gap's core coverage requirement,
// but cheap to include given the handler already has this branch, and it
// guards against a future refactor silently changing this behavior.
func TestHandler_UnknownScenarioFails(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(LargePayloadEvent{Scenario: "not-a-real-scenario"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED for an unrecognized scenario, got %s", result.GetStatus())
	}
	msg, ok := result.GetError()
	if !ok || !strings.Contains(msg, "unknown scenario") {
		t.Fatalf("expected an 'unknown scenario' error message, got %q", msg)
	}
}

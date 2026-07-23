package durable_test

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestSHA256Hashing_DifferentContextIDsProduceDifferentHashes verifies
// the first of the two correctness properties this task calls out as
// essential: two STRUCTURALLY IDENTICAL handlers, differing only in
// their execution's own root EXECUTION operation ID (the real-world
// analog is two different execution ARNs/durable-execution IDs - see
// dcontext.NewRoot's doc, which threads durable.go's executionOp.ID
// straight into the root Context's hash-input prefix), must produce
// DIFFERENT hashed IDs for what would otherwise be "the same" step
// number (both call Step exactly once, as their first and only durable
// operation).
//
// This is NOT the same property as replay-determinism (verified
// separately by TestSHA256Hashing_SameHandlerReplayedTwiceProducesSameHashes,
// below) - a hash that was deterministic but computed from a GLOBAL
// constant (e.g. hash(counter) with no execution-specific input at all)
// would ALSO pass a same-execution replay-determinism check, while
// failing this one. This test exists specifically to rule that out: it
// proves the hash genuinely incorporates context-specific entropy (the
// root EXECUTION operation's own ID), not just a deterministic-but-
// guessable transform of the bare step number - confirming the exact
// hash-input construction rule documented on
// dcontext.hashInputPrefix/NextStepID (prefix + "-" + counter, where
// prefix is the EXECUTION operation's own ID for a root context) is
// really wired all the way through from durable.go's executionOp.ID
// down to the checkpointed Id, not merely present in a doc comment.
func TestSHA256Hashing_DifferentContextIDsProduceDifferentHashes(t *testing.T) {
	// Two structurally-identical handlers - literally the same handler
	// value, even - each run against its OWN fakeClient/execution with a
	// DIFFERENT root EXECUTION operation ID ("exec-alpha" vs.
	// "exec-beta"), matching what two different real executions
	// (different durable-execution ARNs) would each supply via their own
	// InitialExecutionState's EXECUTION operation.
	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		validated, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: validated}, nil
	}

	runOnce := func(executionID, arn string) string {
		client := newFakeClient()
		entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
		input := newInvocationInput(executionID, arn, "token-0", mustMarshalPayload(t, orderEvent{OrderID: "same-payload"}))

		out, err := entry(context.Background(), input)
		if err != nil {
			t.Fatalf("unexpected error running handler for executionID=%q: %v", executionID, err)
		}
		if out.Status != types.ExecutionStatusSucceeded {
			t.Fatalf("expected Succeeded for executionID=%q, got status=%s error=%v", executionID, out.Status, out.Error)
		}

		// Find the single checkpointed STEP operation's own ID - the
		// fake client's snapshot has exactly two entries (the seeded
		// EXECUTION operation and the one checkpointed STEP), so
		// filtering by Type unambiguously identifies it without
		// depending on knowing its (hashed, unpredictable-by-design) ID
		// value up front.
		for _, op := range client.snapshot() {
			if op.Type == types.OperationTypeStep {
				return op.ID
			}
		}
		t.Fatalf("expected exactly one checkpointed STEP operation for executionID=%q, found none", executionID)
		return ""
	}

	hashAlpha := runOnce("exec-alpha", "arn:test:alpha")
	hashBeta := runOnce("exec-beta", "arn:test:beta")

	if hashAlpha == "" || hashBeta == "" {
		t.Fatal("expected both runs to produce a non-empty hashed step ID")
	}
	if hashAlpha == hashBeta {
		t.Fatalf("expected DIFFERENT hashed step IDs for two executions with different root EXECUTION operation IDs (context-specific entropy), but both produced the same hash %q - this would mean the hash does not actually depend on the execution's own context ID at all", hashAlpha)
	}

	// Cross-check against the independently-recomputed expected values
	// (expectedStepID, durable_test.go) - not merely "the two differ from
	// each other" (which a bug swapping in some OTHER shared, wrong input
	// could still coincidentally satisfy), but that each one is EXACTLY
	// the value the documented hash-input construction rule predicts for
	// its own execution ID.
	if want := expectedStepID("exec-alpha", 1); hashAlpha != want {
		t.Errorf("exec-alpha's step hash = %q, want %q (SHA-256(\"exec-alpha-1\"))", hashAlpha, want)
	}
	if want := expectedStepID("exec-beta", 1); hashBeta != want {
		t.Errorf("exec-beta's step hash = %q, want %q (SHA-256(\"exec-beta-1\"))", hashBeta, want)
	}
}

// TestSHA256Hashing_SameHandlerReplayedTwiceProducesSameHashes verifies
// the second, and per this task's own framing THE SINGLE MOST IMPORTANT,
// correctness property: the SAME handler, replayed twice against the
// SAME execution (same root EXECUTION operation ID, same checkpointed
// operation log fed back in as InitialExecutionState on the second
// invocation - exactly what a real backend re-invocation after a
// suspend/resume would supply), must produce the exact SAME hashed IDs
// both times.
//
// This is the property replay-skip fundamentally depends on: every
// operation's replay-skip lookup is
// c.ExecManager().GetOperation(c.NextStepID()) (see e.g. step.go's
// runStep) - if NextStepID's Nth call ever produced a DIFFERENT hash on
// a later invocation than it did on an earlier one for the exact same
// logical step, that lookup would silently miss the earlier checkpoint
// entirely, and the SDK would re-run already-completed work (or worse,
// checkpoint a second, orphaned operation at a new ID, corrupting the
// operation log). A hash that isn't perfectly reproducible across replay
// would break the entire SDK, not just this one feature - which is
// exactly why this property gets its own dedicated test here, distinct
// from (and in addition to) TestStep_ReplaySkip's existing, more
// end-to-end "the step body doesn't re-run" behavioral check: this test
// asserts on the ACTUAL ID VALUES themselves being byte-for-byte
// identical across the two invocations, the most direct possible check
// of the property this task calls out as foundational.
func TestSHA256Hashing_SameHandlerReplayedTwiceProducesSameHashes(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		first, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return orderResult{}, err
		}
		second, err := operations.Step(dc, "confirm", func(sc types.StepContext) (string, error) {
			return "confirmed", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: first + "/" + second}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "replay-me"})

	// First (real) invocation: both steps run for real and checkpoint
	// their hashed IDs.
	firstInput := newInvocationInput("exec-replay-hash", "arn:test:replay-hash", "token-0", eventPayload)
	out1, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("first invocation error: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected first invocation Succeeded, got %s", out1.Status)
	}

	firstSnapshot := client.snapshot()
	firstValidateID, ok := firstSnapshot[expectedStepID("exec-replay-hash", 1)]
	if !ok {
		t.Fatal("expected the first Step's hashed ID (SHA-256(\"exec-replay-hash-1\")) to be checkpointed after the first invocation")
	}
	firstConfirmID, ok := firstSnapshot[expectedStepID("exec-replay-hash", 2)]
	if !ok {
		t.Fatal("expected the second Step's hashed ID (SHA-256(\"exec-replay-hash-2\")) to be checkpointed after the first invocation")
	}

	// Second invocation ("replay"): seed InitialExecutionState with the
	// checkpointed operations from the fake backend, plus the root
	// execution operation with the SAME event payload, exactly as a real
	// re-invocation of the SAME execution would receive them.
	var extraOps []types.Operation
	for _, op := range firstSnapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-replay-hash", "arn:test:replay-hash", "token-1", eventPayload, extraOps...)

	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation error: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected second invocation Succeeded, got %s", out2.Status)
	}

	secondSnapshot := client.snapshot()
	secondValidateID, ok := secondSnapshot[expectedStepID("exec-replay-hash", 1)]
	if !ok {
		t.Fatal("expected the first Step's hashed ID to STILL be present (at the identical hash) after the second (replay) invocation")
	}
	secondConfirmID, ok := secondSnapshot[expectedStepID("exec-replay-hash", 2)]
	if !ok {
		t.Fatal("expected the second Step's hashed ID to STILL be present (at the identical hash) after the second (replay) invocation")
	}

	// The core assertion: byte-for-byte identical hashes across both
	// invocations for each of the two steps - not merely "some operation
	// with a plausible ID exists," but literally the SAME 64-character
	// hex string both times.
	if firstValidateID.ID != secondValidateID.ID {
		t.Fatalf("first Step's hashed ID changed across replay: %q (first invocation) != %q (second invocation) - a hash that isn't perfectly reproducible across replay breaks replay-skip entirely", firstValidateID.ID, secondValidateID.ID)
	}
	if firstConfirmID.ID != secondConfirmID.ID {
		t.Fatalf("second Step's hashed ID changed across replay: %q (first invocation) != %q (second invocation) - a hash that isn't perfectly reproducible across replay breaks replay-skip entirely", firstConfirmID.ID, secondConfirmID.ID)
	}

	// Also confirm the two steps' hashes DIFFER from each other (sanity
	// check that this test isn't vacuously passing because both steps
	// happened to collide onto the same ID, which would also make the
	// two equality checks above trivially true for the wrong reason).
	if firstValidateID.ID == firstConfirmID.ID {
		t.Fatalf("expected the two distinct steps ('validate' and 'confirm') to have DIFFERENT hashed IDs, both were %q", firstValidateID.ID)
	}
}

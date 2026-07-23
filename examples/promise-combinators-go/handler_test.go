// handler_test.go exercises this example's 4 handlers against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestAllHandler_ReturnsLowestOfAllQuotes(t *testing.T) {
	runner := dtesting.New(AllHandler, nil)

	result, err := runner.Run(PriceCheckEvent{ProductID: "prod-001"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[PriceCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Price != 18.75 {
		t.Fatalf("expected the lowest of the 3 quotes (18.75), got %v", out.Price)
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestAllHandler_ReturnsLowestOfAllQuotes.history.json")
}

func TestAllHandler_FailsIfAnyVendorFails(t *testing.T) {
	// An ad-hoc handler built directly with operations.All in this test
	// (rather than a second exported handler in handler.go) to exercise
	// All's own fail-outright contract with one failing vendor, without
	// adding an exported symbol solely for this one assertion.
	handler := func(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
		_, err := operationsAllWithOneFailingVendor(dc)
		if err != nil {
			return PriceCheckResult{}, err
		}
		return PriceCheckResult{ProductID: event.ProductID}, nil
	}
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(PriceCheckEvent{ProductID: "prod-002"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED when a vendor quote fails under All, got %s", result.GetStatus())
	}
}

func TestAllSettledHandler_IgnoresFailedVendor(t *testing.T) {
	runner := dtesting.New(AllSettledHandler, nil)

	result, err := runner.Run(PriceCheckEvent{ProductID: "prod-003"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED even with one vendor down, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[PriceCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// globex (21.50) is down in AllSettledHandler; the lowest of the
	// remaining two (acme 19.99, initech 18.75) is 18.75.
	if out.Price != 18.75 {
		t.Fatalf("expected the lowest surviving quote (18.75), got %v", out.Price)
	}

	// No event-signature golden-file assertion here (unlike every other
	// test in this file): AllSettledHandler deliberately runs its
	// branches at unbounded concurrency (see that handler's own doc for
	// why bounded concurrency would be a real correctness hazard given
	// AllSettled's MinSuccessful: 0 default), so branch completion order
	// - and therefore the operation log's exact shape - is not
	// deterministic across runs. This test asserts on the aggregated
	// RESULT (SUCCEEDED status + the correct surviving lowest quote)
	// instead, which IS deterministic regardless of completion order.
}

func TestAnyHandler_ReturnsFirstSuccess(t *testing.T) {
	runner := dtesting.New(AnyHandler, nil)

	result, err := runner.Run(PriceCheckEvent{ProductID: "prod-004"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED (only one vendor up out of three), got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[PriceCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// Only globex (21.50) is up in AnyHandler; acme and initech are down.
	if out.Price != 21.50 {
		t.Fatalf("expected the one successful quote (21.50), got %v", out.Price)
	}

	// No event-signature golden-file assertion here, for the same reason
	// as TestAllSettledHandler_IgnoresFailedVendor above: AnyHandler
	// runs its branches at unbounded concurrency.
}

func TestRaceHandler_ReturnsFirstToFinish(t *testing.T) {
	runner := dtesting.New(RaceHandler, nil)

	result, err := runner.Run(PriceCheckEvent{ProductID: "prod-005"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[PriceCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// With WithParallelMaxConcurrency(1), the first branch (acme, 19.99)
	// always wins deterministically.
	if out.Price != 19.99 {
		t.Fatalf("expected the first branch's quote (19.99) to win the race, got %v", out.Price)
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestRaceHandler_ReturnsFirstToFinish.history.json")
}

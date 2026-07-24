package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_SuccessfulCharge(t *testing.T) {
	runner := dtesting.New(Handler, nil)
	res, err := runner.Run(Event{Seed: 5, FailCharge: false})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[Result](res)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// double=10, triple=15 => merge=25
	if out.Merged != 25 {
		t.Fatalf("merged=%d want 25", out.Merged)
	}
	if out.ChargeStatus != "SUCCEEDED" {
		t.Fatalf("charge status=%q", out.ChargeStatus)
	}
	if out.Refunded {
		t.Fatal("refund should be skipped on successful charge")
	}
	if !out.Notified {
		t.Fatal("notify (ALL_DONE) should run")
	}
}

func TestHandler_FailedChargeCompensates(t *testing.T) {
	runner := dtesting.New(Handler, nil)
	res, err := runner.Run(Event{Seed: 5, FailCharge: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected handler SUCCEEDED (task failure is in-result), got %s (%s)", res.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[Result](res)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.ChargeStatus != "FAILED" {
		t.Fatalf("charge status=%q want FAILED", out.ChargeStatus)
	}
	if !out.Refunded {
		t.Fatal("refund (ALL_FAILED) should run when charge fails")
	}
	if !out.Notified {
		t.Fatal("notify (ALL_DONE) should still run")
	}
	if out.Reason != "COMPLETED_WITH_FAILURES" {
		t.Fatalf("reason=%q want COMPLETED_WITH_FAILURES", out.Reason)
	}
}

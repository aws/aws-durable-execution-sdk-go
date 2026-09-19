package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandlerWaitsPastExecutionTimeout runs one invocation and expects the
// handler to suspend on a 600 s wait, longer than the 300 s execution
// timeout the suite template configures for it.
func TestHandlerWaitsPastExecutionTimeout(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	result := runner.Run(t, "event")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING while the wait is open", result.Status)
	}
	waits := result.OperationsByType(string(durable.OperationTypeWait))
	if len(waits) != 1 {
		t.Fatalf("got %d wait operations, want 1", len(waits))
	}
	if waits[0].IsTerminal() {
		t.Errorf("wait status = %s, want an open wait", waits[0].Status)
	}

	var duration int32
	for _, ev := range result.Events {
		if string(ev.EventType) == "WaitStarted" && ev.WaitStartedDetails != nil && ev.WaitStartedDetails.Duration != nil {
			duration = *ev.WaitStartedDetails.Duration
		}
	}
	if duration != 600 {
		t.Errorf("wait duration = %d s, want 600", duration)
	}
}

// TestHandlerAsDurableTarget registers the handler as a durable invoke
// target. The local runner advances the target's wait, so the caller
// receives the result that a deployed target never reaches before its
// execution timeout.
func TestHandlerAsDurableTarget(t *testing.T) {
	caller := func(ctx durable.Context, event any) (string, error) {
		return durable.Invoke[string](ctx, "slow", "slow-target", event)
	}
	runner := durabletest.NewLocalRunner(caller)
	runner.RegisterFunction("slow-target", durabletest.DurableFunction(handler))

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "should_not_reach" {
		t.Errorf("result = %q, want %q", out, "should_not_reach")
	}
}

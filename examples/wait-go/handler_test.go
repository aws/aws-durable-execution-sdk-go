// handler_test.go exercises this example's 4 handlers against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria. Since LocalTestRunnerConfig.SkipTime defaults to true (see
// examples/wait-for-condition-go's own test doc for the identical
// precedent), every Wait here resolves synchronously within a single
// runner.Run call - none of these tests need Continue/two invocations,
// unlike examples/wait-for-callback-go or examples/create-callback-go.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestBasicHandler_CoolsDown(t *testing.T) {
	runner := dtesting.New(BasicHandler, nil)

	result, err := runner.Run(CoolDownEvent{OrderID: "ord-001"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[CoolDownResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "cooled-down" {
		t.Fatalf("expected status 'cooled-down', got %q", out.Status)
	}

	waitOp, ok := result.GetOperation("cool-down")
	if !ok {
		t.Fatal("expected to find a WAIT operation named 'cool-down'")
	}
	if waitOp.GetType() != types.OperationTypeWait {
		t.Fatalf("expected WAIT type, got %s", waitOp.GetType())
	}
	if waitOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected WAIT SUCCEEDED, got %s", waitOp.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/wait-go/... -run TestBasicHandler_CoolsDown
	dtesting.AssertEventSignatures(t, result, "testdata/TestBasicHandler_CoolsDown.history.json")
}

func TestConfigurableHandler_UsesEventDuration(t *testing.T) {
	runner := dtesting.New(ConfigurableHandler, nil)

	result, err := runner.Run(CoolDownEvent{OrderID: "ord-002", Seconds: 5})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	waitOp, ok := result.GetOperation("configurable-cool-down")
	if !ok {
		t.Fatal("expected to find a WAIT operation named 'configurable-cool-down'")
	}
	if waitOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected WAIT SUCCEEDED, got %s", waitOp.GetStatus())
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestConfigurableHandler_UsesEventDuration.history.json")
}

func TestConfigurableHandler_DefaultsWhenZero(t *testing.T) {
	runner := dtesting.New(ConfigurableHandler, nil)

	// Seconds omitted (zero value) - handler.go falls back to 1 second
	// rather than issuing a zero-duration Wait.
	result, err := runner.Run(CoolDownEvent{OrderID: "ord-003"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
}

func TestNamedHandler_UsesDescriptiveName(t *testing.T) {
	runner := dtesting.New(NamedHandler, nil)

	result, err := runner.Run(CoolDownEvent{OrderID: "ord-004"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	waitOp, ok := result.GetOperation("await-payment-settlement")
	if !ok {
		t.Fatal("expected to find a WAIT operation named 'await-payment-settlement' (the custom name, not a generic id)")
	}
	if waitOp.GetType() != types.OperationTypeWait {
		t.Fatalf("expected WAIT type, got %s", waitOp.GetType())
	}

	out, err := dtesting.GetResult[CoolDownResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "settled" {
		t.Fatalf("expected status 'settled', got %q", out.Status)
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestNamedHandler_UsesDescriptiveName.history.json")
}

// TestUnawaitedHandler_CompletesWithoutWaitingForTheGoroutine confirms
// the handler returns successfully without ever synchronizing on the
// background Wait goroutine's completion - the durable execution
// terminates with status "scheduled" long before (indeed, whether or
// not) that goroutine's own Wait call ever resolves. Unlike the other 3
// tests here, this does NOT assert that the "background-cool-down" WAIT
// operation reached SUCCEEDED - by construction, whether it does at all
// before the test process exits is a race this handler pattern
// deliberately does not synchronize on (see UnawaitedHandler's own doc
// comment in handler.go for why this is included for JS-example parity
// but is not a recommended production pattern).
func TestUnawaitedHandler_CompletesWithoutWaitingForTheGoroutine(t *testing.T) {
	runner := dtesting.New(UnawaitedHandler, nil)

	result, err := runner.Run(CoolDownEvent{OrderID: "ord-005"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[CoolDownResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Status != "scheduled" {
		t.Fatalf("expected status 'scheduled', got %q", out.Status)
	}
}

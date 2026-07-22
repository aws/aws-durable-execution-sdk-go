// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, asserting on the actual checkpointed operations (via the
// event-history golden-file pattern), not just the final result.
//
// See loggingconfig_test.go for a SEPARATE test that constructs
// durable.WithDurableExecution directly (bypassing LocalTestRunner,
// which has no hook for injecting a custom types.LoggerConfig today) to
// directly observe this example's actual replay-mode-aware log
// suppression behavior against a recordingLogger test double - the
// other half of this example's coverage, and the part that most directly
// demonstrates docs/remaining-work.md §5 tasks 13/14.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_GeneratesReport(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(ReportEvent{ReportID: "report-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[ReportResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.ReportID != "report-1" {
		t.Fatalf("expected ReportID to round-trip, got %q", out.ReportID)
	}
	if out.Summary != "summary-of-raw-data-for-report-1" {
		t.Fatalf("expected summary 'summary-of-raw-data-for-report-1', got %q", out.Summary)
	}

	gatherStep, ok := result.GetOperation("gather-data")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'gather-data'")
	}
	if gatherStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected gather-data step SUCCEEDED, got %s", gatherStep.GetStatus())
	}

	summarizeStep, ok := result.GetOperation("summarize-data")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'summarize-data'")
	}
	if summarizeStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected summarize-data step SUCCEEDED, got %s", summarizeStep.GetStatus())
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/logging-go/... -run TestHandler_GeneratesReport
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_GeneratesReport.history.json")
}

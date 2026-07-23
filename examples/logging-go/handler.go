// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// ReportEvent is this example's input shape.
type ReportEvent struct {
	ReportID string `json:"reportId"`
}

// ReportResult is this example's output shape.
type ReportResult struct {
	ReportID string `json:"reportId"`
	Summary  string `json:"summary"`
}

// handler demonstrates replay-aware, contextual logging
// (pkg/durable/utils/logger.go's ContextLogger, docs/remaining-work.md §5
// tasks 13/14): it logs at various points - before, during, and after
// two sequential steps - using both dc.Logger() (the DurableContext-level
// logger, subject to replay-mode-aware suppression) and sc.Logger() (a
// StepContext-level logger, obtained only from real, non-replay-skipped
// execution - see types.StepContext.Logger's doc - and therefore NEVER
// suppressed, by construction).
//
// Two steps, not one, deliberately - this is what lets this example's
// test demonstrate the "frontier-crossing" case (see handler_test.go's
// TestHandler_ReplaySkipSuppressesOnlyThroughTheCompletedStep), not just
// the simpler "everything before AND after the one step is suppressed"
// case: on a replay where "gather-data" has already completed but
// "summarize-data" has not, only the logging positioned before/around/
// inside "gather-data" is suppressed (there being no "next incomplete
// operation" to reach until "summarize-data"); the logging around and
// inside "summarize-data" itself is NOT suppressed, since that step is
// where real execution resumes.
func handler(event ReportEvent, dc types.DurableContext) (ReportResult, error) {
	dc.Logger().Info("handler started", map[string]any{"reportId": event.ReportID})

	dc.Logger().Info("about to gather data", map[string]any{"reportId": event.ReportID})
	rawData, err := operations.Step(dc, "gather-data", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("gathering data", map[string]any{"reportId": event.ReportID})
		return fmt.Sprintf("raw-data-for-%s", event.ReportID), nil
	})
	if err != nil {
		return ReportResult{}, fmt.Errorf("report %s: gathering data: %w", event.ReportID, err)
	}
	dc.Logger().Info("finished gathering data", map[string]any{"rawData": rawData})

	dc.Logger().Info("about to summarize data", map[string]any{"reportId": event.ReportID})
	summary, err := operations.Step(dc, "summarize-data", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("summarizing data", map[string]any{"rawData": rawData})
		return fmt.Sprintf("summary-of-%s", rawData), nil
	})
	if err != nil {
		return ReportResult{}, fmt.Errorf("report %s: summarizing data: %w", event.ReportID, err)
	}
	dc.Logger().Info("finished summarizing data", map[string]any{"summary": summary})

	dc.Logger().Info("handler completed", map[string]any{"summary": summary})
	return ReportResult{ReportID: event.ReportID, Summary: summary}, nil
}

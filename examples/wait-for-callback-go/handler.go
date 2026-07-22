// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/simple-step-go's
// and examples/run-in-child-context-go's handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// ExpenseEvent is this example's input shape.
type ExpenseEvent struct {
	RequestID string  `json:"requestId"`
	Amount    float64 `json:"amount"`
	Requester string  `json:"requester"`
}

// ExpenseResult is this example's output shape.
type ExpenseResult struct {
	RequestID string `json:"requestId"`
	Decision  string `json:"decision"`
}

// handler demonstrates operations.WaitForCallback: a human-in-the-loop
// expense-approval workflow. The submitter function "sends" the callback
// ID to an external approval system (in this example, a step that just
// logs it - in production this would email an approver, post to a
// webhook, etc.). The durable execution then suspends - incurring no
// compute charges - until that external system calls
// SendDurableExecutionCallbackSuccess/Failure, at which point execution
// resumes exactly where it left off.
//
// This mirrors the confirmed real-backend flowchart (internal SDK
// Operation Diagrams design doc, "WaitForCallback") exactly:
// CONTEXT/WAIT_FOR_CALLBACK START -> CALLBACK/CALLBACK START -> submitter
// step (with full Step-style retry) -> await the callback ->
// CONTEXT/WAIT_FOR_CALLBACK SUCCEED/FAIL.
func handler(event ExpenseEvent, dc types.DurableContext) (ExpenseResult, error) {
	dc.Logger().Info("handler started", map[string]any{"requestId": event.RequestID, "amount": event.Amount})

	decision, err := operations.WaitForCallback[string](dc, "manager-approval", func(sc types.StepContext, callbackID string) error {
		// In production: email the manager an approve/reject link
		// encoding callbackID, or register a webhook. Here we just log
		// it, since this example's whole point is demonstrating the
		// suspend/resume mechanics, not a real notification integration.
		sc.Logger().Info("awaiting manager approval", map[string]any{
			"callbackId": callbackID,
			"amount":     event.Amount,
			"requester":  event.Requester,
		})
		return nil
	})
	if err != nil {
		return ExpenseResult{}, fmt.Errorf("expense request %s: %w", event.RequestID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"decision": decision})
	return ExpenseResult{RequestID: event.RequestID, Decision: decision}, nil
}

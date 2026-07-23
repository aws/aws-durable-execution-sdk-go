// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// ChargeCardEvent is this example's input shape.
type ChargeCardEvent struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
	// AlwaysFails, when true, makes the simulated card-charging
	// dependency fail on every attempt with no chance of success -
	// deterministically driving operations.Step to exhaust its retry
	// strategy and surface a *operations.StepFailedError, which this
	// handler inspects via errors.As and reports on rather than letting
	// propagate as an opaque error. Exists so tests can request this
	// scenario without needing a real always-broken dependency.
	AlwaysFails bool `json:"alwaysFails"`
}

// ChargeCardResult is this example's output shape - only produced on the
// success path; the failure path is reported entirely through the
// execution's own FAILED status/error message (see handler's doc for why
// this handler still returns the inspected StepFailedError rather than
// swallowing it into a "successful" result carrying failure information,
// which would hide the failure from a caller only checking the
// execution's status).
type ChargeCardResult struct {
	OrderID         string `json:"orderId"`
	ChargeReceiptID string `json:"chargeReceiptId"`
}

// errCardDeclined simulates a payment processor declining a charge - a
// permanent, non-transient failure a real payment gateway might return
// (as opposed to retry-go's errFlakyDependency, which simulates a
// TRANSIENT failure that eventually succeeds). Modeled as a distinct
// sentinel so the step's own retry strategy (utils.Presets.NoRetry,
// below) reflects the realistic choice a caller would make for this kind
// of failure: retrying a declined card the exact same way is generally
// pointless, so this step is intentionally NOT configured with a retry
// strategy that would ever succeed against AlwaysFails: true, exhausting
// after a small, fixed number of attempts via a short FixedDelay
// strategy instead (see handler's doc) purely to demonstrate the
// multi-attempt StepFailedError.Attempt field meaningfully, not because
// retrying a declined card is itself good practice.
var errCardDeclined = errors.New("payment processor: card declined")

func chargeCard(sc types.StepContext, event ChargeCardEvent) (string, error) {
	sc.Logger().Info("attempting to charge card", map[string]any{"attempt": sc.Attempt(), "amount": event.Amount})
	if event.AlwaysFails {
		return "", errCardDeclined
	}
	return fmt.Sprintf("receipt-%s", event.OrderID), nil
}

// handler demonstrates the structured error hierarchy
// (pkg/durable/operations/errors.go, docs/remaining-work.md §4 tasks
// 10/11): a step that calls a simulated payment processor which, when
// AlwaysFails is set, declines every attempt and exhausts its retry
// strategy, surfacing a *operations.StepFailedError. The handler uses
// errors.As to inspect that error and branch on its Attempt field,
// rather than letting an opaque error propagate - the core point of
// this example, per the structured error hierarchy's own design goal
// (see errors.go's top-level doc: "errors.As(err, &stepErr) ... recovers
// the SPECIFIC type when the caller cares about the distinction").
//
// The step is deliberately configured with a small, fixed-delay retry
// strategy (3 attempts, no wait between them) rather than NoRetry: this
// keeps the example fast and deterministic while still demonstrating
// StepFailedError.Attempt as a MEANINGFUL, non-trivial value (3, not
// always 1) once retries are exhausted - a NoRetry strategy would make
// Attempt always equal 1, which is a real but far less illustrative
// case of the same field.
func handler(event ChargeCardEvent, dc types.DurableContext) (ChargeCardResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID, "alwaysFails": event.AlwaysFails})

	receiptID, err := operations.Step(dc, "charge-card",
		func(sc types.StepContext) (string, error) {
			return chargeCard(sc, event)
		},
		operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 0}, 3)),
	)
	if err != nil {
		// The core point of this example: use errors.As to recover the
		// structured *operations.StepFailedError (rather than treating
		// err as an opaque error) and inspect its Attempt field - e.g.
		// to decide whether this failure is worth paging an on-call
		// engineer (repeatedly exhausted retries) vs. a one-off blip
		// that happened to also exhaust retries due to bad luck, or
		// simply to log a more actionable message than the bare error
		// text would give on its own.
		var stepErr *operations.StepFailedError
		if errors.As(err, &stepErr) {
			return ChargeCardResult{}, fmt.Errorf(
				"order %s: charge-card step failed after %d attempt(s) (operation id %s): %w",
				event.OrderID, stepErr.Attempt, stepErr.ID, err,
			)
		}
		// Not a StepFailedError - some other failure (e.g. a SerdesError
		// from a misbehaving custom Serdes, or a NonDeterministicReplayError
		// - see nondeterministic_test.go for that scenario demonstrated
		// separately). Propagate as-is rather than pretending every
		// error from a Step call is necessarily a StepFailedError.
		return ChargeCardResult{}, fmt.Errorf("order %s: charge-card step failed: %w", event.OrderID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"chargeReceiptId": receiptID})
	return ChargeCardResult{OrderID: event.OrderID, ChargeReceiptID: receiptID}, nil
}

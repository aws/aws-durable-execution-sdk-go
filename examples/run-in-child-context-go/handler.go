// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/simple-step-go's
// handler.go/handler_test.go split (see that package's doc comment) and
// the TypeScript SDK examples package's handler.ts + handler.test.ts
// pattern.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// OrderEvent is this example's input shape.
//
// UseCustomChildSerdes selects between this example's two mutually
// exclusive scenarios (mirroring examples/large-payload-go's own
// LargePayloadEvent.Scenario discriminator-field pattern - see that
// example's handler.go doc for why a single event type with a selector
// field, rather than two separate handlers/event types, is this repo's
// established way to demonstrate two variants of the SAME operation from
// one deployable Lambda function/main.go entry point):
//
//   - false (the default, zero value): the pre-existing "payment"
//     scenario below (validate-amount + charge-card, both using the
//     DEFAULT Serdes).
//   - true: the "reconciliation" scenario (customSerdesHandlerScenario
//     below), demonstrating operations.WithChildSerdes - see that
//     function's own doc for what it proves and why it's a genuinely
//     different code path, not just a different input value threaded
//     through the SAME RunInChildContext call.
//
// A single event type/handler (rather than a second Handler[TEvent,
// TResult] with its own event type, which was this session's first,
// rejected approach) is required here because
// durable.WithDurableExecution's own generic signature - confirmed by
// reading durable.go in full - binds exactly ONE concrete TEvent/TResult
// pair per constructed entry point, and a single deployed Lambda
// function/main.go wires exactly one durableEntry; unlike
// examples/chained-invoke-go's RetryingInventoryCheckHandler (which
// targets a SEPARATE, second real Lambda function as its Invoke target,
// not a second scenario on the SAME function), there is no clean way to
// pre-dispatch between two different TEvent types before
// WithDurableExecution's own json.Unmarshal runs, short of peeking at
// the raw ExecutionDetails.InputPayload string in main.go by hand - a
// materially more invasive, less idiomatic change than simply adding one
// more field to the existing event type, which is what
// examples/large-payload-go already established as this repo's pattern
// for exactly this situation.
type OrderEvent struct {
	OrderID              string  `json:"orderId"`
	Amount               float64 `json:"amount"`
	UseCustomChildSerdes bool    `json:"useCustomChildSerdes"`
}

// OrderResult is this example's output shape.
type OrderResult struct {
	OrderID         string `json:"orderId"`
	PaymentReceipt  string `json:"paymentReceipt"`
	ShippingLabelID string `json:"shippingLabelId"`
	// ReconciledTotal is populated only by the UseCustomChildSerdes:true
	// scenario (customSerdesHandlerScenario) - left at its zero value
	// (0) for the pre-existing "payment" scenario, which never sets it.
	ReconciledTotal float64 `json:"reconciledTotal,omitempty"`
}

// errCardChargeDeclined simulates a payment processor genuinely rejecting
// a charge attempt (as opposed to validate-amount's own falsy-but-no-error
// zero-amount case below, which is NOT this) - a real, non-transient
// application error returned from inside the "charge-card" step's body
// when event.Amount is negative. This is what makes charge-card itself
// fail as a checkpointed STEP operation (Action: FAIL, not just a falsy
// result), which is what then propagates up through and fails the
// enclosing "payment" RunInChildContext - see TestHandler_PaymentStepFails
// in handler_test.go, and this file's own handler doc below, for why this
// distinct trigger (negative amount) was added alongside the pre-existing
// zero-amount/validate-amount case rather than changing that one: the two
// scenarios are deliberately different (a falsy step result that does NOT
// fail the context, vs. a real step error that DOES), and keeping them as
// two separate, clearly-named triggers demonstrates both without making
// either example inconsistent with its own test's documented behavior.
var errCardChargeDeclined = errors.New("payment processor: card charge declined (invalid negative amount)")

// --- Custom-Serdes-scoped-to-RunInChildContext scenario ---
//
// The following function (customSerdesHandlerScenario), types, and
// screamingSnakeCaseSerdes close docs/ts-sdk-examples-comparison.md's gap
// 8/8: "Custom Serdes is demonstrated only at Step scope in Go
// (custom-config-go), whereas TS demonstrates it at Callback and
// ChildContext scope too." operations.RunInChildContext's WithChildSerdes
// option (invoke.go) has existed since RunInChildContext was implemented,
// but no example applied it until now.
//
// screamingSnakeCaseSerdes below is a byte-for-byte duplicate of
// examples/custom-config-go/handler.go's own type of the same name -
// deliberately the SAME custom Serdes implementation, not a second,
// different one, per this gap's own framing ("proving the SAME Serdes
// mechanism works at a DIFFERENT operation scope, not showcasing a
// different serialization scheme"). It is duplicated rather than
// imported because every examples/*-go directory is its own standalone
// Go module (see this package's go.mod) with no shared internal example
// package between them - matching this repo's established convention of
// each example being fully self-contained (no example imports another
// example's package; only pkg/durable/* is shared).

// childSnapshot is the CHILD CONTEXT's own result type - what
// screamingSnakeCaseSerdes actually (de)serializes when passed to
// operations.WithChildSerdes. A distinct type from OrderResult/the
// child's Go-level return value, mirroring custom-config-go's identical
// separation-of-concerns between a step's/context's OWN checkpointed
// result type and the handler's unrelated top-level JSON output shape.
type childSnapshot struct {
	ReconciledTotal float64 `json:"reconciled_total"`
	AuditNote       string  `json:"audit_note"`
}

// screamingSnakeCaseSerdes is examples/custom-config-go's own custom
// types.Serdes implementation (see that package's handler.go for the
// full doc on why this specific transform - re-keying top-level JSON
// fields to SCREAMING_SNAKE_CASE - was chosen as a realistic,
// infrastructure-free demonstration of the types.Serdes extensibility
// point), reused here VERBATIM rather than reinvented, to demonstrate
// that the identical mechanism also works when supplied to
// operations.WithChildSerdes (a RunInChildContext option) instead of
// operations.WithStepSerdes (a Step option) - the Go SDK's Serdes option
// surface (types.Serdes) is not scoped to any one operation type at the
// type-system level, so the same concrete implementation is valid for
// both.
type screamingSnakeCaseSerdes struct{}

func (screamingSnakeCaseSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: marshaling value for entity %q: %w", entityID, err)
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(b, &asMap); err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: value for entity %q did not marshal to a JSON object: %w", entityID, err)
	}
	rekeyed := make(map[string]json.RawMessage, len(asMap))
	for k, v := range asMap {
		rekeyed[toScreamingSnakeCase(k)] = v
	}
	out, err := json.Marshal(rekeyed)
	if err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: re-marshaling rekeyed value for entity %q: %w", entityID, err)
	}
	return string(out), nil
}

func (screamingSnakeCaseSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(pointer), &asMap); err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: checkpointed value for entity %q is not a JSON object: %w", entityID, err)
	}
	rekeyed := make(map[string]json.RawMessage, len(asMap))
	for k, v := range asMap {
		rekeyed[fromScreamingSnakeCase(k)] = v
	}
	out, err := json.Marshal(rekeyed)
	if err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: re-marshaling un-rekeyed value for entity %q: %w", entityID, err)
	}
	var v any
	if err := json.Unmarshal(out, &v); err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: unmarshaling un-rekeyed value for entity %q: %w", entityID, err)
	}
	return v, nil
}

// toScreamingSnakeCase converts a snake_case (this file's own
// childSnapshot struct tags, e.g. "reconciled_total") or camelCase key to
// SCREAMING_SNAKE_CASE (e.g. "RECONCILED_TOTAL") - identical to
// custom-config-go's own helper of the same name.
func toScreamingSnakeCase(key string) string {
	var b strings.Builder
	for i, r := range key {
		if r >= 'A' && r <= 'Z' && i > 0 {
			b.WriteByte('_')
		}
		if r == '_' {
			b.WriteByte('_')
			continue
		}
		b.WriteRune(toUpperASCII(r))
	}
	return b.String()
}

// fromScreamingSnakeCase converts a SCREAMING_SNAKE_CASE key (e.g.
// "RECONCILED_TOTAL") back to snake_case (e.g. "reconciled_total") - the
// exact inverse of toScreamingSnakeCase, identical to custom-config-go's
// own helper of the same name.
func fromScreamingSnakeCase(key string) string {
	return strings.ToLower(key)
}

func toUpperASCII(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 'a' + 'A'
	}
	return r
}

// customSerdesHandlerScenario implements the UseCustomChildSerdes:true
// branch of handler (below): a RunInChildContext call whose OWN
// checkpointed result (the CONTEXT operation's Payload, not any nested
// Step's) is serialized with screamingSnakeCaseSerdes instead of the
// default JSON Serdes. This closes docs/ts-sdk-examples-comparison.md's
// gap 8/8 - the pre-existing "payment" scenario (and custom-config-go's
// own example) only ever apply a custom Serdes to a Step's result via
// WithStepSerdes; nothing in this repo previously exercised the
// CONTEXT-level Serdes option at all.
//
// The child context here contains one plain, default-Serdes STEP
// ("compute-reconciled-total") nested inside it, specifically so the
// test can distinguish "the custom Serdes ran for the CONTEXT's own
// result" from "the custom Serdes ran for a Step nested inside a
// context" (which custom-config-go already demonstrates, just at the
// root level rather than nested) - the nested step's own checkpointed
// payload is asserted to remain in the DEFAULT format (the step returns
// a plain float64, which has no field names to rekey either way, so
// this step's own result is a bare JSON number), while only the
// ENCLOSING context's own checkpointed result - built from the step's
// return value plus the audit note below, only at the point the child
// fn returns to RunInChildContext - is genuinely rekeyed to
// SCREAMING_SNAKE_CASE.
func customSerdesHandlerScenario(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started (custom child serdes scenario)", map[string]any{"orderId": event.OrderID})

	snapshot, err := operations.RunInChildContext(dc, "reconciliation", func(child types.DurableContext) (childSnapshot, error) {
		total, err := operations.Step(child, "compute-reconciled-total", func(sc types.StepContext) (float64, error) {
			return event.Amount, nil
		})
		if err != nil {
			return childSnapshot{}, err
		}
		return childSnapshot{
			ReconciledTotal: total,
			AuditNote:       fmt.Sprintf("reconciled order %s", event.OrderID),
		}, nil
	}, operations.WithChildSerdes[childSnapshot](screamingSnakeCaseSerdes{}))
	if err != nil {
		return OrderResult{}, fmt.Errorf("order %s: reconciliation child context failed: %w", event.OrderID, err)
	}

	// Real-cloud verification aid: GetDurableExecutionHistory's own
	// Result/Payload fields come back Truncated:true on the real backend
	// (confirmed repeatedly elsewhere in this repo - see
	// docs/remaining-work.md's completion-config-go/large-payload-go
	// writeups), so - exactly like every prior real-cloud verification in
	// this repo that needed to inspect an actual checkpointed payload's
	// CONTENT (not just its presence/status) - the only way to confirm
	// the ACTUAL wire bytes RunInChildContext just checkpointed for the
	// "reconciliation" CONTEXT operation is via this function's own
	// CloudWatch Logs. This re-serializes the SAME snapshot value through
	// the SAME screamingSnakeCaseSerdes instance RunInChildContext itself
	// already used internally (invoke.go's cfg.serdes.Serialize call) -
	// not a second, independent guess at the format - purely so that
	// wire-format string is visible in this function's own logs for
	// external (CloudWatch) verification; it does not re-checkpoint
	// anything or change what was already durably persisted above.
	rawForLogging, rawErr := screamingSnakeCaseSerdes{}.Serialize(snapshot, "reconciliation", "")
	if rawErr == nil {
		dc.Logger().Info("reconciliation context checkpointed payload (custom Serdes wire format)", map[string]any{"rawCheckpointedPayload": rawForLogging})
	}

	dc.Logger().Info("handler completed (custom child serdes scenario)", map[string]any{"reconciledTotal": snapshot.ReconciledTotal})
	return OrderResult{OrderID: event.OrderID, ReconciledTotal: snapshot.ReconciledTotal}, nil
}

// handler demonstrates operations.RunInChildContext: the order's payment
// processing is grouped into its own isolated child context (a "payment"
// sub-workflow with its own step-ID namespace and its own single
// CONTEXT-level checkpoint), separate from the top-level shipping step.
// This mirrors the pattern the reference SDKs document for
// RunInChildContext: grouping related operations (here, validate +
// charge, both durable steps) so they can be checkpointed, replayed, and
// reasoned about as one logical unit distinct from sibling operations at
// the root level.
//
// event.UseCustomChildSerdes:true short-circuits entirely into
// customSerdesHandlerScenario (above) instead - see OrderEvent's own doc
// for why a single event type with this selector field, rather than a
// second Handler[TEvent, TResult], is how this repo demonstrates two
// scenarios from one deployable function.
//
// # Two distinct "invalid input" scenarios, deliberately kept separate
//
// This handler models TWO different kinds of "bad amount" input, on
// purpose, matching the TS SDK's equivalent
// run-in-child-context/with-failing-step example's own distinct-scenario
// approach (see docs/ts-sdk-examples-comparison.md's RunInChildContext
// section):
//
//   - Amount == 0: validate-amount returns (false, nil) - a FALSY but
//     NON-ERROR step result. This step still SUCCEEDS as a checkpointed
//     operation; the handler as written does not itself branch on the
//     false result (see TestHandler_InvalidAmount's own doc, unchanged by
//     this addition, for why this is intentional, documented behavior of
//     this example, not a bug: it demonstrates that a falsy-but-successful
//     step result does NOT, by itself, fail the enclosing RunInChildContext
//   - the child-context machinery doesn't second-guess a step's own
//     success/failure determination).
//   - Amount < 0: charge-card returns a REAL error
//     (errCardChargeDeclined), configured with utils.Presets.NoRetry() so
//     it fails deterministically on the first attempt with no retry delay
//     (keeping this test fast; a real payment-decline is exactly the kind
//     of permanent, non-transient failure NoRetry is meant for - retrying
//     the exact same declined charge would be pointless, mirroring
//     error-handling-go's identical reasoning for its own errCardDeclined
//     sentinel). This makes operations.Step itself fail (a checkpointed
//     STEP FAIL, not just a falsy result), which propagates as a
//     *operations.StepFailedError up through this fn closure, which
//     RunInChildContext then wraps into a *operations.ChildContextFailedError
//     and checkpoints the ENCLOSING "payment" CONTEXT operation itself as
//     FAILED too - the genuine nested-failure-propagates-to-child-context
//     path that TestHandler_InvalidAmount's own doc comment explicitly
//     says this example did NOT previously exercise. See
//     TestHandler_PaymentStepFails for the dedicated test.
//
// Deciding to ADD a new, separate trigger (negative amount) rather than
// changing validate-amount's own zero-amount behavior to return an error:
// TestHandler_InvalidAmount's pre-existing doc comment is explicit that
// its current falsy-non-error behavior is "current behavior" the test
// itself documents and relies on - changing it would be a real, silent
// behavior change to an existing, already-passing, already-documented
// example, not merely "adding a test." Adding a clearly-named new
// scenario (a distinct, negative-amount trigger on a DIFFERENT step,
// charge-card, which was already present in this handler) is strictly
// less disruptive, keeps every existing assertion and doc comment
// accurate with no edits needed, and still fully closes the gap
// docs/ts-sdk-examples-comparison.md identifies (a genuine failing nested
// step propagating up through RunInChildContext) - which does not require
// reusing validate-amount specifically, only SOME nested step inside the
// child context genuinely failing.
func handler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	if event.UseCustomChildSerdes {
		return customSerdesHandlerScenario(event, dc)
	}

	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID})

	// The "payment" child context groups two steps (validate + charge)
	// that only make sense together - if this context is replay-skipped
	// (already completed in a prior invocation), NEITHER step's body
	// re-runs, even though each step is individually replay-safe on its
	// own. This is RunInChildContext's coarser barrier over Step's
	// per-operation replay-skip.
	receipt, err := operations.RunInChildContext(dc, "payment", func(child types.DurableContext) (string, error) {
		if _, err := operations.Step(child, "validate-amount", func(sc types.StepContext) (bool, error) {
			return event.Amount > 0, nil
		}); err != nil {
			return "", err
		}

		return operations.Step(child, "charge-card", func(sc types.StepContext) (string, error) {
			if event.Amount < 0 {
				return "", errCardChargeDeclined
			}
			return fmt.Sprintf("receipt-%s-%.2f", event.OrderID, event.Amount), nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
	})
	if err != nil {
		// Demonstrates errors.As-based inspection of
		// *operations.ChildContextFailedError - the structured error type
		// for exactly this "a nested operation failed, propagating failure
		// to the whole enclosing RunInChildContext" scenario (see
		// errors.go's own doc: "ChildContextFailedError is returned when
		// RunInChildContext's fn returns a (non-suspension) error"). This
		// mirrors error-handling-go's identical errors.As pattern for
		// *operations.StepFailedError - see that example's handler.go doc
		// for the same rationale applied to a different operation kind.
		var childErr *operations.ChildContextFailedError
		if errors.As(err, &childErr) {
			return OrderResult{}, fmt.Errorf(
				"order %s: payment child context failed (context id %s, reconstructed=%t): %w",
				event.OrderID, childErr.ID, childErr.Reconstructed, err,
			)
		}
		// Not a ChildContextFailedError - propagate as-is rather than
		// pretending every error from RunInChildContext is necessarily
		// one (e.g. a SerdesError from a misbehaving custom Serdes).
		return OrderResult{}, err
	}

	// A sibling step at the ROOT context, outside the child context -
	// runs independently of whatever happened inside "payment".
	label, err := operations.Step(dc, "create-shipping-label", func(sc types.StepContext) (string, error) {
		return fmt.Sprintf("label-%s", event.OrderID), nil
	})
	if err != nil {
		return OrderResult{}, err
	}

	dc.Logger().Info("handler completed", map[string]any{"receipt": receipt, "label": label})
	return OrderResult{OrderID: event.OrderID, PaymentReceipt: receipt, ShippingLabelID: label}, nil
}

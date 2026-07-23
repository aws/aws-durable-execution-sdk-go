// Package main's handler.go isolates the durable function's business
// logic from main.go's Lambda Runtime API plumbing, so it can be
// exercised by handler_test.go via the SDK's testing.LocalTestRunner
// without needing a real Lambda execution environment - mirroring the
// TypeScript SDK examples package's pattern of one handler.ts + one
// handler.test.ts per example (see docs/remaining-work.md task 17b and
// its ADDING_EXAMPLES.md reference).
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// OrderEvent is this example's input shape.
type OrderEvent struct {
	OrderID string `json:"orderId"`
	Message string `json:"message"`
}

// OrderResult is this example's output shape.
type OrderResult struct {
	OrderID  string `json:"orderId"`
	Greeting string `json:"greeting"`
}

// handler is a real durable function using operations.Step, mirroring the
// deployed Java simple-step-example this port is modeled on (see
// docs/checkpoint-replay-design.md): a short chain of named steps, each
// checkpointed independently.
func handler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID})

	greeting, err := operations.Step(dc, "create-greeting", func(sc types.StepContext) (string, error) {
		return fmt.Sprintf("Hello, %s!", event.Message), nil
	})
	if err != nil {
		return OrderResult{}, err
	}

	dc.Logger().Info("handler completed", map[string]any{"greeting": greeting})
	return OrderResult{OrderID: event.OrderID, Greeting: greeting}, nil
}

// errMissingMessage is the genuine Go error ValidatingHandler returns
// immediately - before operations.Step is ever called - when the
// incoming event fails validation. This closes
// docs/ts-sdk-examples-comparison.md's "Error handling / determinism"
// gap 6/8: the TS SDK's handler-error example demonstrates "the handler
// itself throws before any operation runs" and asserts
// result.getOperations() has length 0, proving no operation was ever
// checkpointed - a scenario no existing Go example covered explicitly
// (every existing example's handler either never fails at the top, or
// only fails from INSIDE a Step/Context/etc., i.e. after at least one
// operation already ran and was checkpointed).
var errMissingMessage = fmt.Errorf("validation failed: %q field is required and must be non-empty", "message")

// ValidatingHandler is a second, deliberately separate durable function
// value from handler above, added specifically to demonstrate a
// handler-level (pre-operation) failure in isolation, rather than
// bolting an input-validation branch onto the existing handler's single
// happy-path body.
//
// Why a separate handler rather than extending handler in place: handler
// is this repo's simplest, most-referenced example (README.md's own
// worked walkthrough, this package's very first committed test) and its
// existing test (TestHandler_CreatesGreeting) is itself used elsewhere in
// this repo's docs as the canonical "here is what a minimal Go durable
// function looks like" illustration. Adding an unconditional validation
// check ahead of handler's own Step call would change what that
// walkthrough demonstrates (now "a step, PLUS a validation gate" instead
// of just "a step") for every reader, not just the one new test scenario
// this task asks for. A second handler value keeps the original
// single-purpose example completely unchanged (TestHandler_CreatesGreeting
// still passes, byte-for-byte, its golden file untouched) while still
// letting the SAME example directory/module host the new scenario, which
// is what this task's own briefing suggested as the acceptable
// alternative ("OR add this as a clearly-separated second handler/test if
// that's cleaner given simple-step-go's existing single-handler
// simplicity").
//
// ValidatingHandler is a strict superset of handler's own behavior for
// any VALID event: it performs one synchronous, in-process validation
// check with no side effects and no durable operation involved at all
// (validation is not itself an operations.Step - the whole point is that
// it runs BEFORE any operation, so wrapping it in one would defeat the
// scenario), then, only if validation passes, delegates to the exact
// same operations.Step("create-greeting", ...) logic handler itself
// uses. This is why main.go's durableEntry is switched to wrap
// ValidatingHandler instead of handler for the deployed function (see
// that file's comment) - the pre-existing happy-path behavior every
// prior deployment of this function already demonstrated is preserved
// exactly, just reached through one extra, harmless validation gate for
// well-formed input.
func ValidatingHandler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID})

	// Genuine validation failure, returned directly from the handler
	// body - no operations.Step, operations.RunInChildContext, or any
	// other durable operation has been called yet at this point, and
	// none ever will be for this invocation once this branch is taken.
	// This is exactly the TS handler-error example's own scenario ("the
	// handler itself throws before any operation runs"): the failure
	// is a plain Go error from ordinary control flow, not a checkpointed
	// operation's own FAIL outcome.
	if event.Message == "" {
		return OrderResult{}, errMissingMessage
	}

	greeting, err := operations.Step(dc, "create-greeting", func(sc types.StepContext) (string, error) {
		return fmt.Sprintf("Hello, %s!", event.Message), nil
	})
	if err != nil {
		return OrderResult{}, err
	}

	dc.Logger().Info("handler completed", map[string]any{"greeting": greeting})
	return OrderResult{OrderID: event.OrderID, Greeting: greeting}, nil
}

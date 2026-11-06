// Command child-context-subtype demonstrates [durable.WithChildSubType].
// The order is processed in a child context recorded with the subtype
// "OrderSaga" instead of the default RunInChildContext. The subtype is
// written to the checkpoint and reported to plugins in
// OperationHookInfo.SubType, so a plugin or a reader of the execution
// history can tell order sagas apart from other child contexts. The audit
// child context next to it keeps the default subtype for contrast.
//
// The subtype is part of the operation's identity on replay: every
// invocation must supply the same one for the same operation. A wait at
// the end suspends the execution, so the second invocation replays both
// child contexts and checks their recorded subtypes. The subtype is
// therefore a constant, never derived from the input.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// orderSagaSubType labels every order child context. It must stay the
// same across deployments while executions are in flight.
const orderSagaSubType = "OrderSaga"

// Output reports the order outcome and the audit entry.
type Output struct {
	Order string `json:"order"`
	Audit string `json:"audit"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	order, err := durable.RunInChildContext(ctx, "order-saga",
		func(child durable.Context) (string, error) {
			if _, err := durable.Step(child, "reserve-stock",
				func(durable.StepContext) (string, error) { return "reserved", nil }); err != nil {
				return "", err
			}
			return durable.Step(child, "charge-card",
				func(durable.StepContext) (string, error) { return "order confirmed", nil })
		},
		durable.WithChildSubType(orderSagaSubType))
	if err != nil {
		return Output{}, err
	}

	audit, err := durable.RunInChildContext(ctx, "audit",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "write-audit",
				func(durable.StepContext) (string, error) { return "audit recorded", nil })
		})
	if err != nil {
		return Output{}, err
	}

	// The wait ends the first invocation; the second replays both child
	// contexts and validates their subtypes against the checkpoint.
	if err := durable.Wait(ctx, "settle", 1*time.Second); err != nil {
		return Output{}, err
	}
	return Output{Order: order, Audit: audit}, nil
}

func main() { durable.Start(handler) }

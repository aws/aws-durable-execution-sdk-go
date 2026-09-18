// Command future-join demonstrates durable.Join: wait for futures of
// different result types as a unit. All, Any, AllSettled, and Race require
// every future to share one result type. Join accepts any mix of futures
// through the Awaitable interface, waits for all of them, and returns the
// first error in argument order. Values are read afterwards with Result.
//
// Join is the replacement for awaiting several futures by hand. A sequence
// of Result calls that returns on the first error leaves the remaining
// futures unawaited, so when a branch suspends the others never reach their
// blocking points in that invocation and their progress is not
// checkpointed. Join awaits every future before propagating a suspension,
// so each branch checkpoints as far as it can. The notify branch below
// suspends on a wait; the charge and inventory branches complete and
// checkpoint in the first invocation.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Receipt is the result of charging the customer.
type Receipt struct {
	ChargeID string  `json:"chargeId"`
	Amount   float64 `json:"amount"`
}

// Result reports the outcome of the fan-out.
type Result struct {
	Receipt   Receipt `json:"receipt"`
	Reserved  int     `json:"reserved"`
	Notified  bool    `json:"notified"`
	Completed bool    `json:"completed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Three concurrent branches with three different result types.
	charge := durable.StepAsync(ctx, "charge", func(_ durable.StepContext) (Receipt, error) {
		return Receipt{ChargeID: "ch_123", Amount: 42.50}, nil
	})
	reserve := durable.Go(ctx, "reserve", func(c durable.Context) (int, error) {
		return durable.Step(c, "reserve-items", func(_ durable.StepContext) (int, error) {
			return 3, nil
		})
	})
	notify := durable.Go(ctx, "notify", func(c durable.Context) (bool, error) {
		// A wait suspends this branch; Join drains the others first.
		if err := durable.Wait(c, "notify-delay", 1*time.Second); err != nil {
			return false, err
		}
		return durable.Step(c, "send-notification", func(_ durable.StepContext) (bool, error) {
			return true, nil
		})
	})

	// Join waits for all three and returns the first error in argument
	// order. On the invocation where notify suspends, Join propagates the
	// suspension after charge and reserve have checkpointed.
	if err := durable.Join(ctx, "settle", []durable.Awaitable{charge, reserve, notify}); err != nil {
		return Result{}, err
	}

	// Every future has settled successfully, so each Result returns
	// immediately.
	receipt, _ := charge.Result()
	reserved, _ := reserve.Result()
	notified, _ := notify.Result()

	return Result{Receipt: receipt, Reserved: reserved, Notified: notified, Completed: true}, nil
}

func main() { durable.Start(handler) }

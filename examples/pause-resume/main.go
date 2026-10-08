// Command pause-resume runs an order approval workflow: it reserves stock,
// waits for a manager approval callback, then ships the order. Its test
// uses durabletest OmitTokenOnCheckpoint to pause the execution during the
// reserve-stock step and resume it afterward.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Event is the order to process.
type Event struct {
	OrderID string `json:"orderId"`
}

// Result is the order outcome.
type Result struct {
	Reservation string `json:"reservation"`
	Approval    string `json:"approval"`
	Shipment    string `json:"shipment"`
}

// reserveStockDuration is how long the reserve-stock step works.
const reserveStockDuration = 200 * time.Millisecond

func handler(ctx durable.Context, event Event) (Result, error) {
	orderID := event.OrderID
	if orderID == "" {
		orderID = "order-1"
	}
	reservation, err := durable.Step(ctx, "reserve-stock", func(_ durable.StepContext) (string, error) {
		time.Sleep(reserveStockDuration)
		return "reserved-" + orderID, nil
	})
	if err != nil {
		return Result{}, err
	}
	approval, err := durable.WaitForCallback[string](ctx, "manager-approval",
		func(_ durable.StepContext, _ string) error {
			// A real workflow would send the callback ID to an approver here.
			return nil
		},
		durable.WithCallbackTimeout(5*time.Minute),
		durable.WithCallbackSerdes(durable.JSONSerdes),
	)
	if err != nil {
		return Result{}, err
	}
	shipment, err := durable.Step(ctx, "ship-order", func(_ durable.StepContext) (string, error) {
		return "shipped-" + orderID, nil
	})
	if err != nil {
		return Result{}, err
	}
	return Result{Reservation: reservation, Approval: approval, Shipment: shipment}, nil
}

func main() { durable.Start(handler) }

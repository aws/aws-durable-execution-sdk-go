// Command order-fulfillment demonstrates a realistic durable workflow that
// processes a customer order through validation, payment, and shipment stages.
//
// It exercises the core SDK patterns naturally:
//   - Step (with retry) for individual operations that may fail transiently
//   - Wait for a cooling-off period between payment and shipment
//   - Parallel for concurrent shipment preparation tasks
//   - durable.Go (child context) for grouped validation logic
//
// The workflow accepts an Order and returns a FulfillmentResult summarizing
// the outcome of each stage.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Order is the input event for the workflow.
type Order struct {
	OrderID    string `json:"orderId"`
	CustomerID string `json:"customerId"`
	Items      []Item `json:"items"`
}

// Item represents a line item in the order.
type Item struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// FulfillmentResult is the workflow output.
type FulfillmentResult struct {
	OrderID        string  `json:"orderId"`
	Status         string  `json:"status"`
	Total          float64 `json:"total"`
	PaymentID      string  `json:"paymentId"`
	TrackingNumber string  `json:"trackingNumber"`
	LabelURL       string  `json:"labelUrl"`
}

// handler is the durable workflow entry point.
func handler(ctx durable.Context, order Order) (FulfillmentResult, error) {
	// Stage 1: Validate order in a child context (groups related checks).
	total, err := durable.RunInChildContext(ctx, "validate-order",
		func(childCtx durable.Context) (float64, error) {
			return validateOrder(childCtx, order)
		},
	)
	if err != nil {
		return FulfillmentResult{}, fmt.Errorf("validation failed: %w", err)
	}

	// Stage 2: Charge payment with retry (transient failures expected).
	paymentID, err := durable.Step(ctx, "charge-payment",
		func(_ durable.StepContext) (string, error) {
			return chargePayment(order.OrderID, order.CustomerID, total)
		},
		durable.WithRetry(durable.ExponentialBackoff()),
	)
	if err != nil {
		return FulfillmentResult{}, fmt.Errorf("payment failed: %w", err)
	}

	// Stage 3: Wait a short cooling-off period before shipment.
	if err := durable.Wait(ctx, "cooling-off", 5*time.Second); err != nil {
		return FulfillmentResult{}, fmt.Errorf("wait interrupted: %w", err)
	}

	// Stage 4: Prepare shipment in parallel (label + tracking generated concurrently).
	shipment, err := durable.Parallel(ctx, "prepare-shipment",
		[]durable.Branch[string]{
			{
				Name: "generate-label",
				Func: func(branchCtx durable.Context) (string, error) {
					return durable.Step(branchCtx, "label",
						func(_ durable.StepContext) (string, error) {
							return generateShippingLabel(order)
						},
					)
				},
			},
			{
				Name: "generate-tracking",
				Func: func(branchCtx durable.Context) (string, error) {
					return durable.Step(branchCtx, "tracking",
						func(_ durable.StepContext) (string, error) {
							return generateTrackingNumber(order.OrderID)
						},
					)
				},
			},
		},
	)
	if err != nil {
		return FulfillmentResult{}, fmt.Errorf("shipment prep failed: %w", err)
	}

	return FulfillmentResult{
		OrderID:        order.OrderID,
		Status:         "FULFILLED",
		Total:          total,
		PaymentID:      paymentID,
		LabelURL:       shipment.Items[0].Result,
		TrackingNumber: shipment.Items[1].Result,
	}, nil
}

// validateOrder checks inventory and calculates total in grouped steps.
func validateOrder(ctx durable.Context, order Order) (float64, error) {
	if len(order.Items) == 0 {
		return 0, errors.New("order has no items")
	}

	// Check inventory for each item.
	_, err := durable.Step(ctx, "check-inventory",
		func(_ durable.StepContext) (bool, error) {
			for _, item := range order.Items {
				if item.Quantity <= 0 {
					return false, fmt.Errorf("invalid quantity for SKU %s", item.SKU)
				}
			}
			return true, nil
		},
	)
	if err != nil {
		return 0, err
	}

	// Calculate total.
	total, err := durable.Step(ctx, "calculate-total",
		func(_ durable.StepContext) (float64, error) {
			var sum float64
			for _, item := range order.Items {
				sum += item.Price * float64(item.Quantity)
			}
			return sum, nil
		},
	)
	if err != nil {
		return 0, err
	}

	return total, nil
}

// chargePayment simulates a payment charge. In a real system this would call
// a payment gateway API.
func chargePayment(orderID, customerID string, amount float64) (string, error) {
	_ = customerID
	return fmt.Sprintf("pay-%s-%.0f", orderID, amount*100), nil
}

// generateShippingLabel simulates label generation.
func generateShippingLabel(order Order) (string, error) {
	return fmt.Sprintf("https://labels.example.com/%s", order.OrderID), nil
}

// generateTrackingNumber simulates tracking number assignment.
func generateTrackingNumber(orderID string) (string, error) {
	return fmt.Sprintf("TRK-%s-001", orderID), nil
}

func main() {
	durable.Start(handler)
}

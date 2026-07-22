// Command serde-custom-config demonstrates handler-level serdes using
// [durable.WithSerdes]. This sets a default serializer applied to all
// operations without per-operation overrides — the Go equivalent of the JS
// SDK's context.configureSerdes({ defaultSerdes: ... }).
//
// Go adaptation note: JS configureSerdes can be called mid-execution to
// change the default. In Go, the serdes is set at construction time via
// [durable.WithSerdes] and cannot be changed after. This is by design:
// construction-time configuration makes serialization behavior deterministic
// and avoids replay-sensitive mutation.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Order is a domain type with methods that survive Go's native
// deserialization.
type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

func (o Order) Summary() string {
	return fmt.Sprintf("Order %s: $%.2f (%s)", o.ID, o.Amount, o.Status)
}

// envelopeSerdes wraps values in an envelope with a "type" discriminator,
// demonstrating that the handler-level serdes applies to ALL operations.
type envelopeSerdes struct{}

type envelope struct {
	Type string          `json:"_type"`
	Data json.RawMessage `json:"data"`
}

func (s *envelopeSerdes) Marshal(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{Type: "order-serdes", Data: data})
}

func (s *envelopeSerdes) Unmarshal(data []byte, v any) error {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	return json.Unmarshal(env.Data, v)
}

type event struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
}

type output struct {
	Summary string  `json:"summary"`
	ID      string  `json:"id"`
	Amount  float64 `json:"amount"`
	Status  string  `json:"status"`
}

func handler(ctx durable.Context, ev event) (output, error) {
	// Step 1: no per-step serdes needed — uses the handler-level default.
	order, err := durable.Step(ctx, "create-order", func(_ durable.StepContext) (Order, error) {
		return Order{ID: ev.OrderID, Amount: ev.Amount, Status: "pending"}, nil
	})
	if err != nil {
		return output{}, err
	}

	// Wait forces a replay — order is deserialized with the handler serdes.
	if err := durable.Wait(ctx, "pause", 1*time.Second); err != nil {
		return output{}, err
	}

	// Step 2: order.Summary() works because Go preserves methods natively.
	processed, err := durable.Step(ctx, "process-order", func(_ durable.StepContext) (Order, error) {
		order.Status = "processed"
		return order, nil
	})
	if err != nil {
		return output{}, err
	}

	return output{
		Summary: processed.Summary(),
		ID:      processed.ID,
		Amount:  processed.Amount,
		Status:  processed.Status,
	}, nil
}

func main() {
	durable.Start(handler, durable.WithSerdes(&envelopeSerdes{}))
}

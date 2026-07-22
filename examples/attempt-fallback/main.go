// Command attempt-fallback demonstrates a supplier fallback pattern: the
// workflow tries each supplier in preference order until one can fulfill the
// order. Each supplier check is a separate step; the first to succeed wins.
//
// The JS SDK version uses stepCtx.attempt inside retry to index into
// suppliers. The Go SDK does not expose attempt in the step body, so this
// example uses sequential steps — one per supplier — which is the natural Go
// idiom: explicit control flow, no hidden counters.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// suppliers in preference order; only the last one has stock.
var suppliers = []string{"supplier-a", "supplier-b", "supplier-c"}

// BackorderResult is the outcome of placing the order.
type BackorderResult struct {
	Supplier string `json:"supplier"`
	Attempt  int    `json:"attempt"`
	Status   string `json:"status"`
}

func handler(ctx durable.Context, _ any) (BackorderResult, error) {
	for i, supplier := range suppliers {
		name := fmt.Sprintf("try-%s", supplier)
		s := supplier // capture for closure

		result, err := durable.Step(ctx, name,
			func(sctx durable.StepContext) (BackorderResult, error) {
				sctx.Logger().Info("Attempting to place backorder",
					"supplier", s, "attempt", i+1)

				// Only the last supplier can fulfill the order.
				if s != suppliers[len(suppliers)-1] {
					return BackorderResult{}, fmt.Errorf("%s is out of stock", s)
				}
				return BackorderResult{
					Supplier: s,
					Attempt:  i + 1,
					Status:   "backordered",
				}, nil
			},
			durable.WithRetry(durable.NoRetry()),
		)
		if err != nil {
			// This supplier failed; try the next one.
			ctx.Logger().Info("Supplier failed, trying next",
				"supplier", s, "error", err)
			continue
		}
		return result, nil
	}
	return BackorderResult{}, errors.New("no suppliers could fulfill the order")
}

func main() { durable.Start(handler) }

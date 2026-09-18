// Command future-select demonstrates durable.Select: run named branches
// concurrently and return the name and value of the first to settle. Unlike
// Race, which returns only the value, Select tells the caller which branch
// won, so the handler can branch on the winner's identity. The winner is
// checkpointed with its value, so replay reports the same branch even if a
// different branch would finish first when re-run.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports which quote provider answered first and the price used.
type Result struct {
	Winner string  `json:"winner"`
	Quote  float64 `json:"quote"`
	Note   string  `json:"note"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	winner, quote, err := durable.Select(ctx, "quote", []durable.Branch[float64]{
		{Name: "primary", Func: func(c durable.Context) (float64, error) {
			return durable.Step(c, "primary-quote", func(_ durable.StepContext) (float64, error) {
				// The primary provider is slow today.
				time.Sleep(2 * time.Second)
				return 100.00, nil
			})
		}},
		{Name: "fallback", Func: func(c durable.Context) (float64, error) {
			return durable.Step(c, "fallback-quote", func(_ durable.StepContext) (float64, error) {
				return 104.50, nil
			})
		}},
	})
	if err != nil {
		return Result{}, err
	}

	// Branch on the winner: a fallback quote carries a surcharge the
	// primary quote does not, so the caller must know which one it holds.
	switch winner {
	case "primary":
		return Result{Winner: winner, Quote: quote, Note: "primary quote, no surcharge"}, nil
	default:
		return Result{Winner: winner, Quote: quote * 1.02, Note: "fallback quote with 2% surcharge"}, nil
	}
}

func main() { durable.Start(handler) }

// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-go's own
// handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// PriceCheckEvent is this example's input shape.
type PriceCheckEvent struct {
	ProductID string `json:"productId"`
}

// PriceCheckResult is this example's output shape.
type PriceCheckResult struct {
	ProductID string  `json:"productId"`
	Price     float64 `json:"price"`
}

// vendorQuote is a small helper shared by every handler below: a branch
// that looks up a price from one of three simulated vendors, each with
// its own latency/reliability characteristics (encoded directly in the
// branch closures each handler builds).
func vendorQuote(name string, price float64, fail bool) func(types.DurableContext) (float64, error) {
	return func(child types.DurableContext) (float64, error) {
		return operations.Step(child, "quote-"+name, func(sc types.StepContext) (float64, error) {
			if fail {
				return 0, fmt.Errorf("vendor %s: quote unavailable", name)
			}
			return price, nil
		})
	}
}

// operationsAllWithOneFailingVendor exercises operations.All with one
// failing branch - used only by handler_test.go's
// TestAllHandler_FailsIfAnyVendorFails to demonstrate All's own
// fail-outright contract (unlike AllHandler above, whose 3 branches are
// all deliberately successful).
func operationsAllWithOneFailingVendor(dc types.DurableContext) ([]float64, error) {
	return operations.All(dc, "all-vendor-quotes-one-failing", []func(types.DurableContext) (float64, error){
		vendorQuote("acme", 19.99, false),
		vendorQuote("globex", 0, true), // down
		vendorQuote("initech", 18.75, false),
	}, operations.WithParallelMaxConcurrency[float64](1))
}

// AllHandler demonstrates operations.All - the Promise.all-style
// combinator: every branch must succeed, and the result is the ordered
// slice of values. If any branch fails, All fails outright (unlike the
// lower-level operations.Parallel it is built on, which never itself
// returns an error for a policy-not-met batch - see All's own doc in
// batch.go). Mirrors the JS reference SDK's own promise/all example
// ("Waiting for all promises to complete"). Uses
// WithParallelMaxConcurrency(1) for a deterministic operation-checkpoint
// order (see RaceHandler's own doc for why this matters for the
// event-signature golden file below).
func AllHandler(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
	quotes, err := operations.All(dc, "all-vendor-quotes", []func(types.DurableContext) (float64, error){
		vendorQuote("acme", 19.99, false),
		vendorQuote("globex", 21.50, false),
		vendorQuote("initech", 18.75, false),
	}, operations.WithParallelMaxConcurrency[float64](1))
	if err != nil {
		return PriceCheckResult{}, fmt.Errorf("product %s: %w", event.ProductID, err)
	}

	lowest := quotes[0]
	for _, q := range quotes[1:] {
		if q < lowest {
			lowest = q
		}
	}
	return PriceCheckResult{ProductID: event.ProductID, Price: lowest}, nil
}

// AllSettledHandler demonstrates operations.AllSettled - every branch
// runs to completion regardless of failures, and the caller inspects the
// aggregated BatchResult to see which succeeded/failed. Mirrors the JS
// reference SDK's own promise/all-settled example ("Waiting for all
// promises to settle (success or failure)").
//
// Deliberately does NOT set WithParallelMaxConcurrency(1) the way
// AllHandler/RaceHandler above do: AllSettled's own MinSuccessful: 0
// default (see that function's own doc in batch.go) means
// thresholdExceeded is satisfied as soon as the FIRST branch finishes at
// all (succeeded or failed) - under bounded concurrency, every
// LATER-scheduled branch would then be skipped via errBatchSkipped
// rather than actually run, which would silently defeat this handler's
// own "every branch runs to completion" scenario. Unbounded (the
// default) concurrency avoids this entirely: every branch is already
// running before any of them can finish, so thresholdExceeded's
// same-instant check never has a chance to skip one. This does mean
// branch completion order - and so the operation log's exact shape - is
// not deterministic across runs; this handler's own test
// (TestAllSettledHandler_IgnoresFailedVendor) asserts on the aggregated
// RESULT rather than a golden event-signature file for exactly this
// reason.
func AllSettledHandler(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
	batch, err := operations.AllSettled(dc, "settled-vendor-quotes", []func(types.DurableContext) (float64, error){
		vendorQuote("acme", 19.99, false),
		vendorQuote("globex", 0, true), // this vendor is down
		vendorQuote("initech", 18.75, false),
	})
	if err != nil {
		return PriceCheckResult{}, fmt.Errorf("product %s: %w", event.ProductID, err)
	}

	successes := batch.GetResults()
	if len(successes) == 0 {
		return PriceCheckResult{}, fmt.Errorf("product %s: every vendor quote failed", event.ProductID)
	}
	lowest := successes[0]
	for _, q := range successes[1:] {
		if q < lowest {
			lowest = q
		}
	}
	return PriceCheckResult{ProductID: event.ProductID, Price: lowest}, nil
}

// AnyHandler demonstrates operations.Any - resolves with the FIRST
// branch to succeed (only failing if every branch fails). Mirrors the JS
// reference SDK's own promise/any example ("Waiting for the first
// successful promise").
//
// Deliberately does NOT set WithParallelMaxConcurrency(1): Any's own
// MinSuccessful: 1 default means thresholdExceeded is satisfied as soon
// as ONE branch succeeds - under bounded concurrency, a later-scheduled
// branch could be skipped via errBatchSkipped before even getting a
// chance to run, which would work fine for THIS handler's own two-down/
// one-up scenario (order doesn't matter when only one branch can ever
// succeed) but is the same real hazard AllSettledHandler's own doc
// documents in detail - kept unbounded here too, for consistency and to
// avoid the same class of bug if this scenario's data ever changed.
func AnyHandler(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
	price, err := operations.Any(dc, "any-vendor-quote", []func(types.DurableContext) (float64, error){
		vendorQuote("acme", 0, true), // down
		vendorQuote("globex", 21.50, false),
		vendorQuote("initech", 0, true), // down
	})
	if err != nil {
		return PriceCheckResult{}, fmt.Errorf("product %s: %w", event.ProductID, err)
	}
	return PriceCheckResult{ProductID: event.ProductID, Price: price}, nil
}

// RaceHandler demonstrates operations.Race - resolves (or fails) with
// whichever branch finishes FIRST, success or failure. Mirrors the JS
// reference SDK's own promise/race example ("Racing promises to
// completion"). Branch completion order in this Go SDK is determined by
// each branch's own Step's real execution/checkpoint order (there is no
// artificial delay primitive here the way the JS examples use setTimeout
// to stagger completion) - this example uses max concurrency 1 so the
// FIRST branch in the slice always "wins" deterministically, making the
// scenario reproducible for tests without relying on goroutine
// scheduling.
func RaceHandler(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
	price, err := operations.Race(dc, "race-vendor-quote", []func(types.DurableContext) (float64, error){
		vendorQuote("acme", 19.99, false),
		vendorQuote("globex", 21.50, false),
	}, operations.WithParallelMaxConcurrency[float64](1))
	if err != nil {
		return PriceCheckResult{}, fmt.Errorf("product %s: %w", event.ProductID, err)
	}
	return PriceCheckResult{ProductID: event.ProductID, Price: price}, nil
}

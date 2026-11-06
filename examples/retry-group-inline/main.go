// Command retry-group-inline demonstrates [durable.Retry] with
// [durable.WithAttemptChildContext] set to false. Each attempt then runs
// directly in the handler's context: its step is recorded at the top level
// rather than under a per-attempt child context, and the strategy receives
// the error the attempt returned rather than one rebuilt from a record.
//
// Because the strategy sees that live error, it can match it by Go type
// and by sentinel:
//
//   - [durable.ErrorAs] matches a *SupplierBusyError.
//   - [durable.ErrorIs] matches ErrRateLimited through the wrapping
//     fmt.Errorf adds.
//
// On replay the attempt runs again, its step returns the checkpointed
// answer, and the attempt returns the same error, so the strategy decides
// the same way. With the default per-attempt child context neither matcher
// would match; the retry-group example shows the matchers that do.
//
// The strategy is a hard-coded configuration, so it is built once at
// package initialization with [durable.MustNewRetryStrategy]. It applies
// [durable.JitterHalf]: each delay is drawn between half the computed delay
// and the full delay, so concurrent executions spread their retries out
// while each still waits at least half its backoff. The computed delays are
// 2 s and then 4 s, so after rounding to whole seconds the first retry
// waits 1 s or 2 s and the second 2 s to 4 s.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input scripts the supplier. The zero value runs the default scenario.
type Input struct {
	// Responses is the supplier's answer to each attempt, in order:
	// "busy", "rate-limited", "rejected", or "ok". An attempt past the end
	// of the list gets "ok". Default: busy, rate-limited, ok.
	Responses []string `json:"responses"`
}

// Output is the confirmed quote.
type Output struct {
	PriceCents int `json:"priceCents"`
	Attempts   int `json:"attempts"`
}

// SupplierBusyError is a transient failure the strategy matches by type.
type SupplierBusyError struct{ Supplier string }

func (e *SupplierBusyError) Error() string {
	return fmt.Sprintf("supplier %s is busy", e.Supplier)
}

// ErrRateLimited is a transient failure the strategy matches by identity.
var ErrRateLimited = errors.New("supplier rate limited")

// strategy retries the two transient failures and nothing else, at most
// three attempts in total.
var strategy = durable.MustNewRetryStrategy(durable.RetryConfig{
	MaxAttempts:  3,
	InitialDelay: 2 * time.Second,
	BackoffRate:  2,
	Jitter:       durable.JitterHalf,
	RetryableErrors: []durable.ErrorMatcher{
		durable.ErrorAs[*SupplierBusyError](),
		durable.ErrorIs(ErrRateLimited),
	},
})

var defaultResponses = []string{"busy", "rate-limited", "ok"}

func handler(ctx durable.Context, in Input) (Output, error) {
	responses := in.Responses
	if len(responses) == 0 {
		responses = defaultResponses
	}

	return durable.Retry(ctx, "quote",
		func(ctx durable.Context, attempt int) (Output, error) {
			// ctx is the handler's context: the step of every attempt is
			// a top-level operation.
			answer, err := durable.Step(ctx, "request-quote", func(durable.StepContext) (string, error) {
				if attempt > len(responses) {
					return "ok", nil
				}
				return responses[attempt-1], nil
			})
			if err != nil {
				return Output{}, err
			}

			switch answer {
			case "busy":
				return Output{}, &SupplierBusyError{Supplier: "acme"}
			case "rate-limited":
				return Output{}, fmt.Errorf("request quote: %w", ErrRateLimited)
			case "rejected":
				return Output{}, errors.New("order rejected: item discontinued")
			}
			return Output{PriceCents: 4250, Attempts: attempt}, nil
		},
		strategy,
		durable.WithAttemptChildContext(false),
	)
}

func main() { durable.Start(handler) }

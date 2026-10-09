// Command retry-group demonstrates [durable.Retry]: retrying a group of
// durable operations as one unit. Each attempt checks stock and requests a
// quote from a supplier in two steps, then turns the supplier's answer into
// an error or a confirmed quote. A failed attempt re-runs the whole group,
// both steps included, after a backoff wait named "quote-backoff-<n>".
//
// Each attempt runs in its own child context, named "quote-attempt-<n>".
// The strategy therefore receives the [*durable.ChildContextError] the SDK
// rebuilds from the recorded failure, not the error value the attempt
// returned, on the first invocation and on replay alike. That error keeps
// the escaping error's type name and message, so the strategy, built with
// [durable.NewRetryStrategy], selects the retryable failures with matchers
// that read them:
//
//   - [durable.ErrorTypeIs] matches a SupplierBusyError by its recorded type
//     name.
//   - [durable.ErrorContains] matches a rate limit by a substring of the
//     message.
//   - [durable.ErrorMatches] matches any 5xx status by a pattern.
//
// [durable.ErrorAs] and [durable.ErrorIs] would match none of these: they
// need the live error, which an attempt in a child context does not pass
// to the strategy. The retry-group-inline example shows them.
//
// An OrderRejectedError matches no matcher, so Retry stops after that
// attempt. [durable.WithAttemptChildOptions] passes
// [durable.WithChildErrorMapper] to every attempt's child context; the
// mapper rebuilds the rejection as the handler's own type before the
// strategy sees it, so the handler finds it with errors.As and reports the
// rejection as a result instead of failing the execution. The mapper
// returns every other failure unchanged, so the matchers above still read
// the rebuilt [*durable.ChildContextError].
//
// The retry delays are 1 s, 2 s, then 4 s ([durable.JitterNone] keeps them
// exact). The default input answers busy, rate-limited, and 503 before a
// quote, so the group succeeds on its fourth and last allowed attempt.
package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input scripts the supplier. Both fields are optional; the zero value runs
// the default scenario.
type Input struct {
	// Responses is the supplier's answer to each attempt, in order:
	// "busy", "rate-limited", "unavailable", "rejected", or "ok". An
	// attempt past the end of the list gets "ok". Default: busy,
	// rate-limited, unavailable, ok.
	Responses []string `json:"responses"`

	// MaxAttempts caps the attempts, including the first. Default 4.
	MaxAttempts int `json:"maxAttempts"`
}

// Output is the confirmed quote, or the rejection.
type Output struct {
	Status     string `json:"status"`
	PriceCents int    `json:"priceCents,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Attempts   int    `json:"attempts"`
}

// SupplierBusyError is a transient failure the strategy matches by its
// recorded type name.
type SupplierBusyError struct{ Supplier string }

func (e *SupplierBusyError) Error() string {
	return fmt.Sprintf("supplier %s is busy", e.Supplier)
}

// OrderRejectedError is a permanent failure. No matcher selects it, so
// Retry does not retry it.
type OrderRejectedError struct{ Reason string }

const rejectedPrefix = "order rejected: "

func (e *OrderRejectedError) Error() string { return rejectedPrefix + e.Reason }

// mapRejection rebuilds a recorded OrderRejectedError as the handler's own
// type. The checkpoint holds only the type name and the message, so the
// mapper reads the reason back from the message. It is deterministic, so
// replay maps the recorded failure to the same error. Every other failure
// is returned unchanged.
func mapRejection(err *durable.ChildContextError) error {
	if err.ErrorType == "OrderRejectedError" {
		return &OrderRejectedError{Reason: strings.TrimPrefix(err.Message, rejectedPrefix)}
	}
	return err
}

var defaultResponses = []string{"busy", "rate-limited", "unavailable", "ok"}

func handler(ctx durable.Context, in Input) (Output, error) {
	responses := in.Responses
	if len(responses) == 0 {
		responses = defaultResponses
	}
	maxAttempts := in.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 4
	}

	// MaxAttempts comes from the event, so an invalid value is a request
	// error: NewRetryStrategy returns it and the handler returns it before
	// any operation starts.
	strategy, err := durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  maxAttempts,
		InitialDelay: 1 * time.Second,
		BackoffRate:  2,
		Jitter:       durable.JitterNone,
		RetryableErrors: []durable.ErrorMatcher{
			durable.ErrorTypeIs("SupplierBusyError"),
			durable.ErrorContains("rate exceeded"),
			durable.ErrorMatches(regexp.MustCompile(`HTTP 5\d\d`)),
		},
	})
	if err != nil {
		return Output{}, err
	}

	out, err := durable.Retry(ctx, "quote",
		func(ctx durable.Context, attempt int) (Output, error) {
			return requestQuote(ctx, attempt, responses)
		},
		strategy,
		durable.WithAttemptChildOptions(durable.WithChildErrorMapper(mapRejection)),
	)

	// Retry stopped on a rejection. The RetryError it returns wraps the
	// mapped error and counts the attempts.
	var retryErr *durable.RetryError
	var rejected *OrderRejectedError
	if errors.As(err, &retryErr) && errors.As(err, &rejected) {
		return Output{Status: "rejected", Reason: rejected.Reason, Attempts: retryErr.Attempts}, nil
	}
	return out, err
}

// requestQuote is one attempt of the group. Both steps run again on every
// attempt; their results are checkpointed under that attempt's child
// context.
func requestQuote(ctx durable.Context, attempt int, responses []string) (Output, error) {
	if _, err := durable.Step(ctx, "check-stock", func(durable.StepContext) (int, error) {
		return 12, nil
	}); err != nil {
		return Output{}, err
	}

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
		return Output{}, errors.New("supplier rate exceeded, slow down")
	case "unavailable":
		return Output{}, errors.New("supplier returned HTTP 503")
	case "rejected":
		return Output{}, &OrderRejectedError{Reason: "item discontinued"}
	}
	return Output{Status: "confirmed", PriceCents: 4250, Attempts: attempt}, nil
}

func main() { durable.Start(handler) }

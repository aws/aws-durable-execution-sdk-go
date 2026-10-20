// Command child-context-error-mapper demonstrates
// [durable.WithChildErrorMapper]. A payment child context fails when its
// charge step is declined. Without a mapper, [durable.RunInChildContext]
// returns a [*durable.ChildContextError] that the handler can only match
// by its ErrorType string. The mapper turns it into PaymentDeclinedError,
// a type the handler owns, so the handler matches it with errors.As.
//
// The checkpoint records the failure that escaped the child body, not the
// mapper's result. On replay the body does not run: the SDK rebuilds the
// [*durable.ChildContextError] from the record and calls the mapper again
// with the same input. A wait after the child suspends the execution, so
// the handler's result is computed on the second invocation, from the
// replayed and re-mapped error. The mapper must therefore be
// deterministic: it reads nothing but the error it receives.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// PaymentDeclinedError is the handler's own error type for a declined
// charge. Code is the issuer's decline code.
type PaymentDeclinedError struct {
	Code string
}

func (e *PaymentDeclinedError) Error() string { return "payment declined: " + e.Code }

// Result reports how the payment ended.
type Result struct {
	Status      string `json:"status"`
	DeclineCode string `json:"declineCode,omitempty"`
}

// mapPaymentFailure maps a failed charge step to PaymentDeclinedError.
// The charge step attaches the decline code with durable.WithErrorData,
// and the recorded failure keeps it as ErrorData. Any other failure is
// returned unchanged.
func mapPaymentFailure(err *durable.ChildContextError) error {
	if err.ErrorType == "StepError" && err.ErrorData != "" {
		return &PaymentDeclinedError{Code: err.ErrorData}
	}
	return err
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.RunInChildContext(ctx, "payment",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "charge-card",
				func(durable.StepContext) (string, error) {
					return "", durable.WithErrorData(errors.New("card declined by issuer"), "insufficient_funds")
				},
				durable.WithRetry(durable.NoRetry()))
		},
		durable.WithChildErrorMapper(mapPaymentFailure))

	// The wait ends the first invocation. The second replays the failed
	// payment child, without running its body, before reaching this
	// point again, so err below is the mapper's result on replay.
	if werr := durable.Wait(ctx, "before-notify", 1*time.Second); werr != nil {
		return Result{}, werr
	}

	var declined *PaymentDeclinedError
	if errors.As(err, &declined) {
		return Result{Status: "declined", DeclineCode: declined.Code}, nil
	}
	if err != nil {
		return Result{}, err
	}
	return Result{Status: "charged"}, nil
}

func main() { durable.Start(handler) }

// Command error-stack-traces demonstrates [durable.WithStackTraces]. When
// user code fails, the SDK records a stack trace by default: one
// "function file:line" string per frame, innermost first, at most
// [durable.MaxStackTraceFrames] frames, beginning with the user function
// that returned the error. The trace is written to the failed operation's
// checkpoint and exposed as StackTrace on the typed operation errors.
//
// This function turns capture off with WithStackTraces(false), as a
// function does when its checkpoints must stay as small as possible or when
// the file paths of its build must not leave the function. A step that
// fails without retrying then records its error type and message but no
// frames, and the *durable.StepError the handler receives carries an empty
// StackTrace. The handler reports what the error carries rather than
// failing the execution, so the result shows the effect of the option.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output describes the step failure the handler caught.
type Output struct {
	Message     string `json:"message"`
	StackFrames int    `json:"stackFrames"`
}

// errCardDeclined is the failure the step body returns.
var errCardDeclined = errors.New("card declined")

// charge stands in for a payment call that the provider rejects.
func charge(card string) (string, error) {
	return "", fmt.Errorf("charge %s: %w", card, errCardDeclined)
}

func handler(ctx durable.Context, _ any) (Output, error) {
	_, err := durable.Step(ctx, "charge-card",
		func(durable.StepContext) (string, error) {
			return charge("4111-xxxx")
		},
		// A declined card is final, so the step fails on its first
		// attempt.
		durable.WithRetry(durable.NoRetry()),
	)

	var stepErr *durable.StepError
	if !errors.As(err, &stepErr) {
		return Output{}, fmt.Errorf("expected the charge-card step to fail, got %v", err)
	}
	return Output{Message: stepErr.Message, StackFrames: len(stepErr.StackTrace)}, nil
}

func main() { durable.Start(handler, durable.WithStackTraces(false)) }

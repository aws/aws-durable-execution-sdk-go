// Command step-error-determinism demonstrates that error handling is
// deterministic across replay. A step throws a custom error with no retry.
// Subsequent steps verify that the error is consistently reported — the
// execution succeeds, proving that error capture and replay are stable.
//
// Key insight: the SDK preserves the error type name and message in the
// checkpoint. On replay, the StepError wraps a reconstructed error that
// carries the original type and message. The presence and shape of the
// error are deterministic; only the string formatting (which includes the
// type prefix on replay) may differ.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// ValidationError is a custom business error.
type ValidationError struct {
	Code string
}

func (e *ValidationError) Error() string { return "business validation failed" }

// ErrorProps captures structural error properties for comparison.
type ErrorProps struct {
	HasError  bool `json:"hasError"`
	IsStepErr bool `json:"isStepErr"`
	Attempts  int  `json:"attempts"`
}

// Result reports whether error properties are stable across replay.
type Result struct {
	Deterministic bool       `json:"deterministic"`
	Before        ErrorProps `json:"before"`
	After         ErrorProps `json:"after"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Step that always fails with a custom error.
	var stepErr error
	_, err := durable.Step(ctx, "failing-step",
		func(_ durable.StepContext) (any, error) {
			return nil, &ValidationError{Code: "VALIDATION_ERROR"}
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		stepErr = err
	}

	// Capture structural error properties before replay boundary.
	before, err := durable.Step(ctx, "check-before-replay",
		func(_ durable.StepContext) (ErrorProps, error) {
			return captureProps(stepErr), nil
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		return Result{}, err
	}

	// Wait forces a checkpoint boundary.
	if err := durable.Wait(ctx, "replay-boundary", 1*time.Second); err != nil {
		return Result{}, err
	}

	// Capture error properties after replay.
	after, err := durable.Step(ctx, "check-after-replay",
		func(_ durable.StepContext) (ErrorProps, error) {
			return captureProps(stepErr), nil
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Deterministic: before == after,
		Before:        before,
		After:         after,
	}, nil
}

// captureProps extracts structural properties from a step error for
// determinism comparison. Uses errors.As to check type presence, not
// string formatting.
func captureProps(err error) ErrorProps {
	if err == nil {
		return ErrorProps{}
	}
	props := ErrorProps{HasError: true}
	var se *durable.StepError
	if errors.As(err, &se) {
		props.IsStepErr = true
		props.Attempts = se.Attempts
	}
	return props
}

func main() { durable.Start(handler) }

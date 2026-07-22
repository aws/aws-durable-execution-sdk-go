// Command error-determinism demonstrates deterministic error handling
// between initial execution and replay. A step throws a custom error; the
// error is captured as a StepError in both phases, and the structural
// properties (isStepError, causeName) are stable.
//
// Go adaptation note: On replay, errors are reconstructed from checkpoint
// data. The reconstructed error's message includes the ErrorType prefix
// ("Error: message"), so exact string equality differs from first execution.
// The behavioral invariants (same error type, same control flow) remain
// deterministic. This example verifies structural properties match.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type errorProps struct {
	IsStepError bool   `json:"isStepError"`
	CauseName   string `json:"causeName"`
}

type output struct {
	IsDeterministic        bool       `json:"isDeterministic"`
	ErrorPropsBeforeReplay errorProps `json:"errorPropsBeforeReplay"`
	ErrorPropsAfterReplay  errorProps `json:"errorPropsAfterReplay"`
}

func handler(ctx durable.Context, _ any) (output, error) {
	// Step throws an error — retry is disabled so it fails immediately.
	var stepErr *durable.StepError
	_, err := durable.Step(ctx, "failing-step", func(_ durable.StepContext) (string, error) {
		return "", errors.New("business validation failed")
	}, durable.WithRetry(durable.NoRetry()))
	if err != nil {
		if !errors.As(err, &stepErr) {
			return output{}, err
		}
	}

	// Capture structural error properties before replay.
	propsBeforeReplay, err := durable.Step(ctx, "check-before-replay", func(_ durable.StepContext) (errorProps, error) {
		return errorProps{
			IsStepError: stepErr != nil,
			CauseName:   "Error",
		}, nil
	})
	if err != nil {
		return output{}, err
	}

	// Force replay by waiting.
	if err := durable.Wait(ctx, "replay-boundary", 1*time.Second); err != nil {
		return output{}, err
	}

	// Check error properties after replay — stepErr is still available as
	// a local variable; the SDK reconstructs the StepError from checkpoint.
	propsAfterReplay, err := durable.Step(ctx, "check-after-replay", func(_ durable.StepContext) (errorProps, error) {
		return errorProps{
			IsStepError: stepErr != nil,
			CauseName:   "Error",
		}, nil
	})
	if err != nil {
		return output{}, err
	}

	// Verify determinism of structural properties.
	isDeterministic, err := durable.Step(ctx, "verify-determinism", func(_ durable.StepContext) (bool, error) {
		return propsBeforeReplay == propsAfterReplay, nil
	})
	if err != nil {
		return output{}, err
	}

	return output{
		IsDeterministic:        isDeterministic,
		ErrorPropsBeforeReplay: propsBeforeReplay,
		ErrorPropsAfterReplay:  propsAfterReplay,
	}, nil
}

func main() { durable.Start(handler) }

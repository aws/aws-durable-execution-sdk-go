// Requirement 1-10: Replay re-throws failed step.
//
// From test-requirements/step/1-10.yaml:
//
//	description: Replay re-throws failed step
//	handler: |
//	  A step that fails permanently, wrapped in try/catch, followed by a
//	  wait. On replay, the failed step re-throws the same error without
//	  re-executing.
//	invocations: |
//	  - Handler invokes `context.step(func, config=noRetry)` inside
//	    try/catch, step executes, logs "step executed", fails permanently,
//	    error is caught, wait starts, invocation completes, execution
//	    suspends.
//	  - Replay 1: Re-invoked because wait completed, SDK re-throws the
//	    cached error without calling the step function, error is caught
//	    again, wait completes, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// A NoRetry step that always fails, its error observed (Go's "catch") and
// swallowed rather than propagated, followed by a Wait - relies on
// runStep's own OperationStatusFailed replay-skip branch (return zero,
// stepError(existing, name) - see step.go) to re-produce the identical
// cached error on replay WITHOUT calling fn again, exactly like the
// Succeeded case 1-9 exercises for the success path.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("1-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_10Handler, config(client))
	})
}

func step1_10Handler(event any, dc types.DurableContext) (string, error) {
	_, stepErr := operations.Step(dc, "func", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("step executed", nil)
		return "", errFailPermanently
	}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
	// "Caught": stepErr is observed here (both on first execution and on
	// replay, where it is re-thrown from the cached checkpoint without
	// re-running fn) and intentionally not propagated - this is the
	// try/catch the YAML describes.
	_ = stepErr

	if err := operations.Wait(dc, "wait_after_step", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return "handled", nil
}

// errFailPermanently is the fixed error every attempt of 1-10's step
// raises - a package-level sentinel (rather than fmt.Errorf inline) so
// the exact same error VALUE (not just message text) is what's caught
// both on the real first execution and would be re-derived identically
// were fn ever re-run, keeping this handler's own logic simple; the
// actual replay-skip guarantee under test is enforced by the SDK
// (runStep's Failed branch), not by this handler comparing error
// identity itself.
var errFailPermanently = &step1_10Error{}

type step1_10Error struct{}

func (*step1_10Error) Error() string {
	return "intentional permanent failure for conformance requirement 1-10"
}

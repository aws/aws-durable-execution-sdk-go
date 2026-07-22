// Requirement 1-16: Retry specific exception (non-retryable fails).
//
// From test-requirements/step/1-16.yaml:
//
//	description: Retry specific exception (non-retryable fails)
//	handler: |
//	  A step that throws a custom TransientError. The retry strategy does
//	  not retry TransientError.
//	invocations: |
//	  - Handler invokes
//	    `context.step(func, config=StepConfig(retry_strategy=no_retry_on_transient_error))`,
//	    step throws TransientError, strategy checks error type, returns
//	    shouldRetry=false, step fails permanently without retry.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory:
//	  ...
//	  StepFailedDetails:
//	    Error:
//	      Payload:
//	        ErrorType: ${/.*TransientError$/}
//
// TransientError is a distinct named Go error type (not just
// errors.New(...)) specifically so ExpectedExecutionHistory's ErrorType
// regex (matching a type name ending in "TransientError") has a real,
// meaningful Go type to match against - see errors.go's OperationError
// for how the checkpointed ErrorType field is derived from the runtime
// error value in this SDK. The retry strategy inspects the error via
// errors.As and returns ShouldRetry: false specifically for
// *TransientError, demonstrating error-type-conditional retry decisions
// (the exact "retryStrategy checks error type" behavior the YAML
// describes) even though this particular strategy happens to always
// decline for this one error type - 1-15 is 1-16's mirror image, where
// the same error type IS retried.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TransientError models a transient, potentially-retryable failure -
// shared by 1-15 (retried) and 1-16 (not retried, by this requirement's
// own strategy) to demonstrate error-type-conditional retry decisions.
type TransientError struct {
	Message string
}

func (e *TransientError) Error() string { return e.Message }

func init() {
	Register("1-16", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_16Handler, config(client))
	})
}

// noRetryOnTransientError returns ShouldRetry: false for any
// *TransientError (and, defensively, for anything else too, since this
// requirement's step only ever raises TransientError) - the "checks
// error type" retry strategy the YAML describes, just always declining
// for this requirement's specific scenario.
func noRetryOnTransientError(err error, attempt int) types.RetryDecision {
	var transient *TransientError
	if errors.As(err, &transient) {
		return types.RetryDecision{ShouldRetry: false}
	}
	return types.RetryDecision{ShouldRetry: false}
}

func step1_16Handler(event any, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "func", func(sc types.StepContext) (string, error) {
		return "", &TransientError{Message: "intentional transient-typed failure for conformance requirement 1-16"}
	}, operations.WithStepRetryStrategy[string](noRetryOnTransientError))
}

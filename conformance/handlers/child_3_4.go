// Requirement 3-4: Child context error (step fails, execution fails).
//
// From test-requirements/child/3-4.yaml:
//
//	description: Child context error (step fails, execution fails)
//	handler: |
//	  A child context where the inner step throws an error with no retry.
//	  The error propagates as a ChildContextError and the execution fails.
//	invocations: |
//	  - Handler invokes a child context with a step that throws an error
//	    (no retry configured). SDK checkpoints ContextStarted, step starts
//	    and fails with ParentId pointing to child context Id, SDK
//	    checkpoints ContextFailed with error details, execution fails with
//	    ChildContextError.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory:
//	  ... StepFailed Error.Payload.{ErrorMessage,ErrorType}: '*'
//	  ... ContextFailed Error.Payload.{ErrorMessage,ErrorType}: '*'
//
// A NoRetry step (utils.Presets.NoRetry(), this SDK's genuine zero-option
// default per step.go - explicit here for clarity, matching step_1_10's
// same explicit style) that always fails. The step's error propagates
// unchanged out of the child fn closure; RunInChildContext's own error
// path (invoke.go: errors.Is(err, errSuspended) check, then
// checkpointErrorObject(err) into a CONTEXT/FAIL checkpoint) is what
// produces the ContextFailed event with a real ErrorObject - not
// anything this handler constructs directly - and RunInChildContext
// itself returns a *ChildContextFailedError, which this handler simply
// propagates as the whole execution's own error, causing durable.
// WithDurableExecution to mark the execution FAILED.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("3-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_4Handler, config(client))
	})
}

func child3_4Handler(event any, dc types.DurableContext) (*string, error) {
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return "", errors.New("intentional permanent failure for conformance requirement 3-4")
		}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
	})
	return nil, err
}

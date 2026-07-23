// Requirement 3-15: Child context error without step (error thrown
// directly in child body).
//
// From test-requirements/child/3-15.yaml:
//
//	description: Child context error without step (error thrown directly
//	  in child body)
//	handler: |
//	  A child context where the child function throws an error directly
//	  without calling any durable operation (no step/wait). The SDK
//	  checkpoints ContextStarted, then ContextFailed with the error. No
//	  StepStarted/StepFailed events.
//	invocations: |
//	  - Handler invokes a child context where the child function throws an
//	    error directly without calling any durable operation. SDK
//	    checkpoints ContextStarted, child throws error directly (no step),
//	    SDK checkpoints ContextFailed with error details, execution fails
//	    with ChildContextError.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory:
//	  - EventType: ContextStarted ...
//	  - EventType: ContextFailed ... Error.Payload.{ErrorMessage,ErrorType}: '*'
//	  - EventType: InvocationCompleted ...
//
// fn itself returns a plain Go error with NO nested operations.Step (or
// any other durable operation) call at all - RunInChildContext's own
// error path (invoke.go: checkpointErrorObject(err) straight into a
// CONTEXT/FAIL checkpoint, since fn's error here did not come from any
// nested checkpointed operation) is exactly what produces the
// ContextFailed-with-no-preceding-StepStarted/StepFailed shape this
// requirement's ExpectedExecutionHistory requires.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_15Handler, config(client))
	})
}

func child3_15Handler(event any, dc types.DurableContext) (*string, error) {
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return "", errors.New("intentional direct failure for conformance requirement 3-15 (no nested operation)")
	})
	return nil, err
}

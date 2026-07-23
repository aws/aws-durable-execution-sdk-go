// Requirement 8-19: Parallel with invalid max-concurrency raises a
// validation error.
//
// From test-requirements/parallel/8-19.yaml:
//
//	description: Parallel invoked with max-concurrency 0 raises a
//	  validation error and the execution fails
//	handler: |
//	  Handler invokes the parallel operation with two trivial branches
//	  but an invalid max-concurrency of 0. A positive max-concurrency (or
//	  none, for unlimited) is required, so the SDK raises a validation
//	  error before running any branch. The error is not caught, so the
//	  execution fails.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory:
//	  - EventType: InvocationCompleted
//	  - EventType: ExecutionFailed
//
// validateBatchConfig (batch.go) rejects maxConcurrency<=0 (this
// requirement's own "0" case) BEFORE Parallel ever claims a step ID or
// enqueues any checkpoint - called immediately after opts are applied, at
// the very top of Parallel, ahead of the c.NextStepID() call that would
// otherwise start the parent's own ContextStarted checkpoint. This
// matches the YAML's own "before running any branch" / "no branch
// contexts are created" expectation exactly: this requirement's
// ExpectedExecutionHistory has no ContextStarted (SubType Parallel) event
// at all, only InvocationCompleted followed directly by ExecutionFailed.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-19", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_19Handler, config(client))
	})
}

func parallel8_19Handler(event any, dc types.DurableContext) (*string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "unreached-0", nil },
		func(child types.DurableContext) (string, error) { return "unreached-1", nil },
	}

	_, err := operations.Parallel(dc, "bad-concurrency", branches, operations.WithParallelMaxConcurrency[string](0))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

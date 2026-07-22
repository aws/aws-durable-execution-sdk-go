// Requirement 3-5: Child context error caught (try/catch, execution
// succeeds).
//
// From test-requirements/child/3-5.yaml:
//
//	description: Child context error caught (try/catch, execution
//	  succeeds)
//	handler: |
//	  A child context where the inner step fails, but the error is caught
//	  by a try/catch in the parent handler. Execution continues and
//	  succeeds.
//	  The recovery step returns the input string.
//	invocations: |
//	  - Handler invokes a child context with a failing step inside,
//	    wrapped in try/catch. Child step fails (ContextStarted,
//	    StepStarted, StepFailed, ContextFailed), error is caught, handler
//	    continues with a subsequent recovery step that succeeds, execution
//	    completes with SUCCEEDED.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// Structurally identical to 3-4's failing child context, but the error
// returned from RunInChildContext is observed here ("caught") and
// intentionally not propagated - matching step_1_10's same try/catch
// idiom for a plain Step. A subsequent top-level (not nested in any
// child context) recovery step then runs and returns the input string,
// becoming the whole execution's own successful result.
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
	Register("3-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_5Handler, config(client))
	})
}

func child3_5Handler(event string, dc types.DurableContext) (string, error) {
	_, childErr := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return "", errors.New("intentional permanent failure for conformance requirement 3-5")
		}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
	})
	// "Caught": childErr is observed here and intentionally not
	// propagated, exactly like step_1_10's own try/catch idiom - the
	// actual replay-skip/error-reconstruction guarantee under test is
	// enforced by the SDK (RunInChildContext's Failed replay branch), not
	// by this handler comparing error identity itself.
	_ = childErr

	return operations.Step(dc, "recovery_step", func(sc types.StepContext) (string, error) {
		return event, nil
	})
}

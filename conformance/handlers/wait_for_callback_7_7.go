// Requirement 7-7: Wait-for-callback submitter retry exhaustion.
//
// From test-requirements/wait_for_callback/7-7.yaml:
//
//	description: Wait-for-callback whose submitter always throws; the
//	  configured retry policy is exhausted and the operation fails
//	  before the callback is ever completed
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name, with a retry policy on the submitter
//	  allowing up to 2 attempts (one retry) with a 1-second delay
//	  between attempts. The submitter throws on every attempt. After
//	  the retry budget is exhausted the submitter step fails
//	  permanently, the operation fails, and the handler does not catch
//	  it so the execution fails. The inner callback is created but is
//	  never completed by any external system.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    which throws (StepStarted/StepFailed, attempt 1, ParentId
//	    pointing to the operation Id), the invocation completes and
//	    execution suspends for the retry delay.
//	  - Replay 1: Submitter retried (attempt 2) and throws again
//	    (StepStarted/StepFailed). The retry budget is exhausted, SDK
//	    checkpoints ContextFailed, and execution fails.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	# No CallbackActions - deliberately: the submitter never succeeds,
//	# so the inner callback (created but never externally completed) is
//	# never reached by this scenario at all.
//
// # Real SDK gap found and fixed this session
//
// Before this session, operations.WaitForCallback's internal Step call
// for the submitter accepted no StepOption at all - there was no way to
// configure retry on the submitter specifically (confirmed by reading
// callback.go's WaitForCallback body directly before making any change:
// `Step(child, id+"-submit", func(sc types.StepContext) (struct{},
// error) {...})`, no opts). This is a genuinely distinct gap from the
// timeout/heartbeat wiring gap 7-5/7-12/7-13 exercise - it is about the
// submitter STEP's own retry policy, not the CALLBACK's timeout. Fixed
// by adding operations.WithWaitForCallbackSubmitterRetryStrategy (see
// its own doc in callback.go), which is passed straight through to the
// internal Step call via operations.WithStepRetryStrategy. Verified via
// a real deployment: the fixed 7-7 handler below now produces the exact
// expected StepStarted/StepFailed (attempt 1) -> InvocationCompleted
// (suspends for the 1s retry delay) -> StepStarted/StepFailed (attempt
// 2) -> ContextFailed -> ExecutionFailed sequence.
//
// utils.Presets.FixedDelay(1s, 2) matches this requirement's own
// "up to 2 attempts... with a 1-second delay between attempts" policy
// exactly: it retries while attempt < maxAttempts (i.e. once, after
// attempt 1), then gives up once the 2nd attempt also fails.
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
	Register("7-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_7Handler, config(client))
	})
}

func waitForCallback7_7Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return errors.New("intentional submitter failure for conformance requirement 7-7")
	}, operations.WithWaitForCallbackSubmitterRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 2)))
}

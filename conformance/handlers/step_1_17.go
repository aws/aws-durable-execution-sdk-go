// Requirement 1-17: Step with AtMostOncePerRetry semantics (interrupted,
// no retry).
//
// From test-requirements/step/1-17.yaml:
//
//	description: Step with AtMostOncePerRetry semantics (interrupted, no
//	  retry)
//	handler: |
//	  A step configured with AtMostOncePerRetry semantics and no retry
//	  strategy. The step causes a Lambda runtime crash (process.exit).
//	  The step prints the input string to raw stdout before crashing.
//	  Log validation uses the raw standard output of the SDK (not the
//	  context logger).
//	invocations: |
//	  - SDK checkpoints StepStarted, step begins execution, prints input to
//	    stdout, Lambda crashes.
//	  - Replay 1: Re-invoked because Lambda crashed (Runtime.ExitError), SDK
//	    sees StepStarted with no completion, raises StepInterruptedError, no
//	    retry configured, SDK checkpoints StepFailed (permanent), execution
//	    fails.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory:
//	  Name: at_most_once_flaky_step
//	  ... InvocationCompleted with Error.ErrorType: Runtime.ExitError
//	  ... StepFailed with RetryDetails.CurrentAttempt: 1
//	ExpectedLogs:
//	  - pattern: ${INPUT_1}
//	    count: 1
//
// This is a REAL Lambda process crash, not a simulated/faked one: the
// step body prints event to raw os.Stdout (fmt.Println, NOT
// sc.Logger(), since ExpectedLogs explicitly validates raw stdout, not
// the structured context logger) and then calls os.Exit(1) - killing
// this entire container mid-invocation. The Lambda Runtime API's own
// getNextInvocation loop in main.go never gets to post a response for
// this invocation at all; the Lambda service itself detects the process
// exited without responding and reports Runtime.ExitError on the
// execution history (InvocationCompletedDetails.Error.ErrorType) - not
// something this handler code produces or controls directly, but the
// genuine, documented real-Lambda consequence of a step crashing the
// process, exactly as the YAML's invocations describes.
//
// operations.WithStepSemantics[T](types.StepSemanticsAtMostOncePerRetry)
// is the real, exported, currently-existing way to select this
// semantics (confirmed directly from step.go/types.go - see this
// package's other AtMostOncePerRetry file, step_1_18.go, for the same
// verification note) - runStep's own OperationStatusStarted branch (see
// step.go) is what turns "checkpointed START, never reached a terminal
// state" into a synthesized interruption error fed through the retry
// strategy on the NEXT invocation (the replay after the crash).
//
// # Update: now passes WithStepRetryStrategy(utils.Presets.NoRetry())
// # explicitly - Step's own zero-option default changed
//
// This requirement's own YAML explicitly requires "no retry configured"
// to mean the interruption error is checkpointed as a PERMANENT
// StepFailed (RetryDetails.CurrentAttempt: 1, ExecutionStatus: FAILED) -
// that was true of Step's own zero-option default (utils.Presets.
// NoRetry()) at the time this handler was first written, but is no
// longer true after a later fix to that default (see pkg/durable/
// operations/step.go's own doc comment: the zero-option fallback now
// matches the JS reference SDK's actual runtime behavior,
// utils.Presets.Default() - 6 attempts, not zero). This handler's own
// documented intent ("no retry configured" -> permanent failure) is
// unrelated to that fix - it specifically needs NoRetry, not whatever
// Step's own default happens to be - so it now requests it explicitly,
// mirroring step_1_10.go's own identical, pre-existing pattern.
package handlers

import (
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("1-17", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_17Handler, config(client))
	})
}

func step1_17Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "at_most_once_flaky_step", func(sc types.StepContext) (string, error) {
		// Raw stdout, deliberately NOT sc.Logger() - ExpectedLogs
		// validates the SDK's raw standard output for this requirement,
		// not the structured context logger (see 1-7's handler for the
		// contrasting case that DOES use sc.Logger()).
		fmt.Println(event)
		os.Exit(1)
		return "", nil // unreachable
	},
		operations.WithStepSemantics[string](types.StepSemanticsAtMostOncePerRetry),
		operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()),
	)
}

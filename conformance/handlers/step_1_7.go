// Requirement 1-7: Step with context logger.
//
// From test-requirements/step/1-7.yaml:
//
//	description: Step with context logger
//	optional: true
//	handler: |
//	  A step that uses `step_context.logger` to emit log entries during
//	  execution. Validates that the SDK's step context logger correctly
//	  writes to CloudWatch.
//	invocations: |
//	  - Handler invokes `context.step(greet(event))`, function calls
//	    `step_context.logger.info("Greeting step started for: <input>")`,
//	    then
//	    `step_context.logger.info("Greeting step completed with: Hello, <input>!")`,
//	    returns the greeting result.
//	ExpectedLogs:
//	  - pattern: 'Greeting step started for: ${INPUT_1}'
//	    count: 1
//	  - pattern: 'Greeting step completed with: Hello, ${INPUT_1}!'
//	    count: 1
//	  - pattern: \[ERROR\]
//	    match: regex
//	    count: 0
//
// Uses types.StepContext.Logger() (sc.Logger() below) - the step-scoped
// logger handed to the step body's fn - rather than the raw log package,
// exercising the SDK's own real logging surface end to end. The log
// message text is built to match ExpectedLogs' patterns EXACTLY
// (including the literal "Greeting step started for: "/"Greeting step
// completed with: Hello, ...!" prefixes), since the conformance
// validator matches these as literal substrings against the function's
// CloudWatch log output. utils.DefaultLogger's Info implementation
// writes structured "LEVEL message key=value ..." lines (see
// utils/logger.go) - the message text itself is passed through verbatim
// as msg, so this satisfies the exact "Greeting step started for: X"
// substring the pattern checks for regardless of what additional
// structured fields/prefix the logger emits around it.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("1-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_7Handler, config(client))
	})
}

func step1_7Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "greet", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("Greeting step started for: "+event, nil)
		result := "Hello, " + event + "!"
		sc.Logger().Info("Greeting step completed with: "+result, nil)
		return result, nil
	})
}

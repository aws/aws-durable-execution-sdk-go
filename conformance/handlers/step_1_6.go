// Requirement 1-6: Custom serdes (per-step).
//
// From test-requirements/step/1-6.yaml:
//
//	description: Custom serdes (per-step)
//	handler: |
//	  A step configured with a custom serdes that transforms all string
//	  values to uppercase on serialization.
//	invocations: |
//	  - Handler invokes `context.step(return_input, serdes=uppercase_serdes)`,
//	    step returns the input string, custom serdes serializes the result
//	    as uppercase.
//	Input: hello world
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: HELLO WORLD
//
// uppercaseSerdes implements types.Serdes directly (same shape as
// utils.JSONSerdes) and is passed via operations.WithStepSerdes - the
// SDK's real, exported per-step serdes override mechanism (see step.go's
// stepConfig.serdes / WithStepSerdes). Serialize uppercases the string
// value and writes it as-is (no JSON-quoting) as this serdes's own raw
// wire format, and Deserialize reads that same raw string back directly
// - this mirrors exactly what a "transforms all string values to
// uppercase on serialization" serdes means: the transformation happens
// in Serialize, not in the step body, and a CUSTOM serdes is free to
// define its own wire format entirely (it is not obligated to be
// JSON, unlike the SDK's own DefaultSerdes) - confirmed as a genuine,
// real bug fix this session via requirement 3-14 (Child context with
// custom serdes), which - unlike 1-6's own ExpectedExecutionHistory,
// which never actually asserts StepSucceededDetails.Result.Payload's
// exact bytes - DOES assert ContextSucceededDetails.Result.Payload
// against the UNQUOTED literal string "HELLO CHILD" (see
// test-requirements/child/3-14.yaml). The conformance harness's own
// event-history matcher (history.py's _match_value) does a plain
// literal Python string comparison for any non-placeholder,
// non-wildcard expected string - it does NOT json.loads() the actual
// Payload before comparing against an ExpectedExecutionHistory field -
// so this serdes's PRIOR behavior of json.Marshal-wrapping the
// uppercased string (producing the wire bytes '"HELLO CHILD"', with
// embedded literal quote characters) genuinely mismatched the expected
// unquoted "HELLO CHILD" once a real requirement (3-14) finally
// asserted that Payload field directly - 1-6 itself never caught this
// because its own YAML happens to only assert an empty
// StepSucceededDetails: {} (no Payload key at all). The top-level
// ExpectedResult.Result comparison (_extract_execution_output) is
// unaffected either way: it already falls back to the raw string when
// json.loads() fails, which happens for both the old (quoted) and new
// (unquoted) wire formats' worth of DIFFERENT reasons - a quoted
// "HELLO WORLD" string *would* json.loads() successfully (to the Python
// str HELLO WORLD, matching), while an unquoted HELLO WORLD does NOT
// json.loads() (invalid JSON) and falls back to the same raw string -
// both paths were already coincidentally producing the same top-level
// comparison result before this fix; only the per-event Payload
// literal-byte comparison actually distinguishes them, which is what
// 3-14 exposed.
package handlers

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// uppercaseSerdes is a types.Serdes that uppercases string values on
// serialization - the exact "custom serdes" scenario requirement 1-6
// describes.
type uppercaseSerdes struct{}

func (uppercaseSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("uppercaseSerdes: expected string, got %T", value)
	}
	return strings.ToUpper(s), nil
}

func (uppercaseSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	return pointer, nil
}

func init() {
	Register("1-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_6Handler, config(client))
	})
}

func step1_6Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "return_input", func(sc types.StepContext) (string, error) {
		return event, nil
	}, operations.WithStepSerdes[string](uppercaseSerdes{}))
}

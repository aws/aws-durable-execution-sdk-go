// Requirement 6-11: Wait-for-condition with custom state serdes.
//
// From test-requirements/wait_for_condition/6-11.yaml:
//
//	description: Wait-for-condition configured with a custom
//	  serializer/deserializer for its checkpointed state, exercising the
//	  serdes round-trip across suspensions
//	handler: |
//	  Handler runs a single wait_for_condition operation configured with
//	  a custom serdes for the state. The state is a string that the
//	  custom serializer encodes when checkpointing and the custom
//	  deserializer decodes when resuming. The check function appends a
//	  character to the state on each invocation; the wait strategy
//	  continues until the state length reaches 2 and then stops. Because
//	  the state survives suspension via the custom serdes, the final
//	  decoded state is "xx".
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check produces state "x", continue, the custom serializer
//	    encodes the state into the StepSucceeded checkpoint, invocation
//	    completes, execution suspends.
//	  - Replay 1: The custom deserializer decodes the checkpointed state
//	    back to "x"; the second check produces "xx". The wait strategy
//	    stops, SDK checkpoints the terminal StepSucceeded with the
//	    encoded final state, execution succeeds returning "xx".
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: xx
//
// Uses operations.WithConditionSerdes[string] with a small custom
// types.Serdes (reversedStringSerdes) that checkpoints the state
// reversed - a deliberately observable, non-default-JSON-equivalent
// transformation (unlike a screaming-snake-case object rekey, which
// would be meaningless for a bare string state), so a silently-ignored
// WithConditionSerdes option would produce a DIFFERENT (non-reversed)
// wire payload than what actually gets checkpointed if the option is
// genuinely wired through - the same "prove it's actually in the loop"
// principle examples/custom-config-go's own screamingSnakeCaseSerdes
// test applies to Step's WithStepSerdes. The state itself is still the
// plain Go string "x"/"xx" at every checkfn/return boundary; only its
// on-the-wire checkpointed representation is reversed. Also configures
// the same explicit, generously-bounded FixedDelay retry strategy 6-1
// uses (via WithConditionRetryStrategy, alongside WithConditionSerdes -
// both ConditionOption[string] values compose freely) - see that file's
// own doc for why the operation's zero-option default (NoRetry) cannot
// drive this scenario's second poll.
package handlers

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// reversedStringSerdes is a minimal custom types.Serdes for requirement
// 6-11: it JSON-encodes the state as usual but with the string's
// characters reversed, and reverses them back on decode - a simple,
// self-contained, and OBSERVABLE (see this file's top-level doc)
// transformation requiring no extra infrastructure to demonstrate the
// WithConditionSerdes round trip.
type reversedStringSerdes struct{}

func (reversedStringSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("reversedStringSerdes: value for entity %q is not a string (got %T)", entityID, value)
	}
	return fmt.Sprintf("%q", reverseString(s)), nil
}

func (reversedStringSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var reversed string
	if _, err := fmt.Sscanf(pointer, "%q", &reversed); err != nil {
		return nil, fmt.Errorf("reversedStringSerdes: checkpointed value for entity %q is not a quoted string: %w", entityID, err)
	}
	return reverseString(reversed), nil
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func init() {
	Register("6-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_11Handler, config(client))
	})
}

func waitForCondition6_11Handler(event any, dc types.DurableContext) (string, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state string) (operations.ConditionResult[string], error) {
		next := state + "x"
		return operations.ConditionResult[string]{State: next, ConditionMet: len(next) >= 2}, nil
	}, "",
		operations.WithConditionSerdes[string](reversedStringSerdes{}),
		operations.WithConditionRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
}

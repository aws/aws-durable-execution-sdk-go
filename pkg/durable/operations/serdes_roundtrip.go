package operations

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// roundTripSerialized returns the serdes ROUND-TRIP of an
// already-serialized success payload - Deserialize(serialized) converted
// to T - which is the value every operation's FRESH-execution success
// path must hand back to the caller, matching the JS reference SDK's
// own explicitly documented semantics (see
// run-in-child-context-handler.ts: "the value handed back ... has
// always passed through the serdes round-trip, regardless of payload
// size"; step-handler.ts, wait-for-condition-handler.ts, and the map/
// parallel item paths all do the same
// safeSerialize-then-safeDeserialize before returning).
//
// This is NOT redundant for a custom serdes: a serdes whose Serialize
// transforms the value (the conformance suite's own requirements 1-6 and
// 3-14 use an uppercasing one) produces a DIFFERENT value after the
// round-trip than the raw in-memory result - and, critically, it is the
// round-tripped value that every LATER replay of this same operation
// will deserialize from the checkpoint and return. Returning the raw
// in-memory value on the fresh execution (this SDK's original,
// incorrect behavior - found via conformance failures 1-6/3-14, which
// an earlier investigation in this repo wrongly wrote off as suite
// bugs) makes the same operation return two DIFFERENT values depending
// on whether the current invocation executed it or replayed it,
// breaking determinism for any transforming serdes.
//
// The any→T conversion falls back to a JSON re-marshal/unmarshal for
// serdes implementations (like the default JSON one) that return an
// untyped any (typically map[string]any) rather than the concrete T -
// the same fallback deserializeStepResult/deserializeContextResult
// already use on their replay paths.
func roundTripSerialized[T any](serdes types.Serdes, serialized, entityID, name, executionARN string) (T, error) {
	var zero T
	val, err := serdes.Deserialize(serialized, entityID, executionARN)
	if err != nil {
		return zero, newSerdesError("deserialize", entityID, name, err)
	}
	typed, ok := val.(T)
	if ok {
		return typed, nil
	}
	b, err := json.Marshal(val)
	if err != nil {
		return zero, newSerdesError("deserialize", entityID, name, fmt.Errorf("converting round-tripped result to %T: %w", zero, err))
	}
	if err := json.Unmarshal(b, &typed); err != nil {
		return zero, newSerdesError("deserialize", entityID, name, fmt.Errorf("converting round-tripped result to %T: %w", zero, err))
	}
	return typed, nil
}

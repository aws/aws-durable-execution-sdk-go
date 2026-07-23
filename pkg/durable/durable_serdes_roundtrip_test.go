package durable_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// These tests pin the serdes ROUND-TRIP contract for every operation's
// fresh-execution success path: the value handed back to the caller must
// be Deserialize(Serialize(result)) - NOT the raw in-memory result -
// matching the JS reference SDK's own explicitly documented behavior
// (run-in-child-context-handler.ts: the returned value "has always
// passed through the serdes round-trip, regardless of payload size";
// step-handler.ts and wait-for-condition-handler.ts both return
// safeDeserialize(serdes, serializedResult)).
//
// Found via conformance requirements 1-6 ("Custom serdes (per-step)",
// expects Result: HELLO WORLD from an uppercasing serdes on input
// "hello world" in a SINGLE invocation - i.e. the fresh path itself
// must return the transformed value) and 3-14 (same for a child
// context). An earlier investigation in this repo wrongly concluded
// these two were conformance-suite bugs ("no reference SDK round-trips")
// - reading the JS SDK's actual handler source disproved that: it
// round-trips on every path. A transforming serdes makes the difference
// observable; it also matters for determinism, since every LATER replay
// of the same operation returns the checkpoint's deserialized value -
// without the fresh-path round-trip, the same operation would return
// two DIFFERENT values across invocations.

// uppercasingSerdes serializes a string by uppercasing it (raw wire
// form, not JSON), and deserializes as identity - the exact shape of
// conformance 1-6/3-14's own custom serdes.
type uppercasingSerdes struct{}

func (uppercasingSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("uppercasingSerdes: expected string, got %T", value)
	}
	return strings.ToUpper(s), nil
}

func (uppercasingSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	return pointer, nil
}

func TestStep_FreshPathReturnsSerdesRoundTrip(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (string, error) {
		return operations.Step(dc, "return_input", func(sc types.StepContext) (string, error) {
			return "hello world", nil
		}, operations.WithStepSerdes[string](uppercasingSerdes{}))
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-step-roundtrip", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "x"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}
	// The handler's own top-level result is the step's return value,
	// JSON-encoded by the default top-level serdes: the round-tripped
	// "HELLO WORLD", not the raw in-memory "hello world".
	if out.ResultPayload == nil || *out.ResultPayload != `"HELLO WORLD"` {
		got := "<nil>"
		if out.ResultPayload != nil {
			got = *out.ResultPayload
		}
		t.Errorf("expected top-level result %q (round-tripped), got %q", `"HELLO WORLD"`, got)
	}
}

func TestRunInChildContext_FreshPathReturnsSerdesRoundTrip(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (string, error) {
		return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
			return "hello child", nil
		}, operations.WithChildSerdes[string](uppercasingSerdes{}))
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-child-roundtrip", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "x"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}
	if out.ResultPayload == nil || *out.ResultPayload != `"HELLO CHILD"` {
		got := "<nil>"
		if out.ResultPayload != nil {
			got = *out.ResultPayload
		}
		t.Errorf("expected top-level result %q (round-tripped), got %q", `"HELLO CHILD"`, got)
	}
}

func TestMapItems_FreshPathReturnsSerdesRoundTrip(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		batch, err := operations.Map(dc, "items", []string{"a", "b"},
			func(child types.DurableContext, item string, index int) (string, error) {
				return item + item, nil
			},
			operations.WithMapSerdes[string, string](uppercasingSerdes{}),
			operations.WithMapMaxConcurrency[string, string](1),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			return nil, fmt.Errorf("unexpected failures: %v", batch.GetErrors())
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-map-roundtrip", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "x"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}
	if out.ResultPayload == nil || *out.ResultPayload != `["AA","BB"]` {
		got := "<nil>"
		if out.ResultPayload != nil {
			got = *out.ResultPayload
		}
		t.Errorf("expected item results round-tripped to uppercase %q, got %q", `["AA","BB"]`, got)
	}
}

func TestWaitForCondition_FreshPathReturnsSerdesRoundTrip(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) (string, error) {
		return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state string) (operations.ConditionResult[string], error) {
			return operations.ConditionResult[string]{State: "ready now", ConditionMet: true}, nil
		}, "initial", operations.WithConditionSerdes[string](uppercasingSerdes{}))
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-cond-roundtrip", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "x"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}
	if out.ResultPayload == nil || *out.ResultPayload != `"READY NOW"` {
		got := "<nil>"
		if out.ResultPayload != nil {
			got = *out.ResultPayload
		}
		t.Errorf("expected final state round-tripped to %q, got %q", `"READY NOW"`, got)
	}
}

// TestStep_FreshAndReplayReturnSameValue pins the determinism property
// the round-trip exists to guarantee: the SAME operation returns the
// SAME value whether the current invocation executed it (fresh path) or
// replayed it from its checkpoint - with a transforming serdes, both
// must be the transformed value.
func TestStep_FreshAndReplayReturnSameValue(t *testing.T) {
	client := newFakeClient()

	var values []string
	handler := func(event orderEvent, dc types.DurableContext) (string, error) {
		v, err := operations.Step(dc, "transform", func(sc types.StepContext) (string, error) {
			return "hello world", nil
		}, operations.WithStepSerdes[string](uppercasingSerdes{}))
		if err != nil {
			return "", err
		}
		values = append(values, v)
		return v, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "x"})

	// First invocation: the step runs fresh.
	out1, err := entry(context.Background(), newInvocationInput("exec-same-value", "arn:test:1", "token-0", eventPayload))
	if err != nil {
		t.Fatalf("unexpected transport-level error on invocation 1: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded on invocation 1, got status=%s", out1.Status)
	}

	// Second invocation, seeded with the checkpointed operations: the
	// step replay-skips and returns the checkpoint's deserialized value.
	var extraOps []types.Operation
	for _, op := range client.snapshot() {
		extraOps = append(extraOps, op)
	}
	out2, err := entry(context.Background(), newInvocationInput("exec-same-value", "arn:test:1", "token-1", eventPayload, extraOps...))
	if err != nil {
		t.Fatalf("unexpected transport-level error on invocation 2: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded on invocation 2, got status=%s", out2.Status)
	}

	if len(values) != 2 {
		t.Fatalf("expected the handler to observe the step value twice (fresh + replay), got %d", len(values))
	}
	if values[0] != "HELLO WORLD" {
		t.Errorf("fresh path returned %q, want the round-tripped %q", values[0], "HELLO WORLD")
	}
	if values[1] != "HELLO WORLD" {
		t.Errorf("replay path returned %q, want %q", values[1], "HELLO WORLD")
	}
	if values[0] != values[1] {
		t.Errorf("determinism violation: fresh=%q replay=%q must be identical", values[0], values[1])
	}
}

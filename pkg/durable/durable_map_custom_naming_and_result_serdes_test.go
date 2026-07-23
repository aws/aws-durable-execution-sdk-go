package durable_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// TestMap_WithMapItemNamer verifies the real, additive feature
// operations.WithMapItemNamer (conformance requirement 9-13, "Map with a
// custom item namer") - confirmed against the JS reference SDK's own
// real, public MapConfig.itemNamer field (types/batch.ts) and its own
// mechanism (map-handler.ts: `config?.itemNamer ? config.itemNamer(item,
// index) : undefined`, feeding directly into each iteration's own
// checkpoint Name). Mirrors 9-13.yaml exactly: items [1, 2], a namer
// deriving "item-1"/"item-2" from the item value, max-concurrency 1.
func TestMap_WithMapItemNamer(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]int, error) {
		batch, err := operations.Map(dc, "named-items", []int{1, 2},
			func(child types.DurableContext, item int, index int) (int, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (int, error) {
					return item * 10, nil
				})
			},
			operations.WithMapMaxConcurrency[int, int](1),
			operations.WithMapItemNamer[int, int](func(item int, index int) string {
				return fmt.Sprintf("item-%d", item)
			}),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			t.Errorf("expected no failures, got %v", batch.GetErrors())
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-item-namer", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "namer-test"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}

	snapshot := client.snapshot()
	var iterationNames []string
	for _, op := range snapshot {
		if op.Type == types.OperationTypeContext && op.SubType == "MapIteration" {
			iterationNames = append(iterationNames, op.Name)
		}
	}

	wantNames := map[string]bool{"item-1": false, "item-2": false}
	for _, name := range iterationNames {
		if _, ok := wantNames[name]; !ok {
			t.Errorf("unexpected MapIteration name %q, want one of item-1/item-2", name)
		}
		wantNames[name] = true
	}
	for name, seen := range wantNames {
		if !seen {
			t.Errorf("expected a MapIteration checkpointed with the custom name %q, never saw it (saw: %v)", name, iterationNames)
		}
	}
}

// opSerdes is the test-suite's own custom, non-identity, whole-result
// serdes - directly mirroring the conformance suite's own canonical
// operation-level-serdes example (test-requirements/map/9-19.yaml,
// 9-20.yaml: "emits the deterministic string 'OPSERDE:' followed by the
// comma-joined results", "deserializes by reversing that encoding").
// Deliberately encodes/decodes ONLY the ordered value list - no
// CompletionReason or per-item error detail survives the round-trip,
// exactly like the real conformance requirements' own scenario (both
// requirements only ever assert Result, never completionReason, for a
// batch that always fully succeeds).
type opSerdes struct{}

func (opSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	batch, ok := value.(operations.BatchResult[string])
	if !ok {
		return "", fmt.Errorf("opSerdes.Serialize: expected operations.BatchResult[string], got %T", value)
	}
	return "OPSERDE:" + strings.Join(batch.GetResults(), ","), nil
}

func (opSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	trimmed := strings.TrimPrefix(pointer, "OPSERDE:")
	if trimmed == "" {
		return []string{}, nil
	}
	return strings.Split(trimmed, ","), nil
}

// TestMap_WithMapResultSerdes_Serialize verifies the real, additive
// feature operations.WithMapResultSerdes (conformance requirement 9-19,
// "Map with an operation-level (whole-result) serdes") - confirmed
// against the JS reference SDK's own real, public, DISTINCT
// MapConfig.serdes field (types/batch.ts: "Serialization/deserialization
// configuration for parent context", separate from itemSerdes) and its
// own mechanism (map-handler.ts forwards config.serdes into
// executeConcurrently -> runInChildContext's own options.serdes - the
// same generic per-context serdes hook RunInChildContext/WithChildSerdes
// already implements one level down). Mirrors 9-19.yaml exactly: the
// outer Map context's own ContextSucceeded payload must be the custom
// serdes's own literal "OPSERDE:X,Y" output, not a JSON-wrapped envelope
// around it.
func TestMap_WithMapResultSerdes_Serialize(t *testing.T) {
	client := newFakeClient()

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		batch, err := operations.Map(dc, "op-serde", []string{"x", "y"},
			func(child types.DurableContext, item string, index int) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return strings.ToUpper(item), nil
				})
			},
			operations.WithMapMaxConcurrency[string, string](1),
			operations.WithMapResultSerdes[string, string](opSerdes{}),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			t.Errorf("expected no failures, got %v", batch.GetErrors())
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-op-serde", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "op-serde-test"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected transport-level error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s", out.Status)
	}

	snapshot := client.snapshot()
	var mapOp *types.Operation
	for _, op := range snapshot {
		o := op
		if op.Name == "op-serde" && op.Type == types.OperationTypeContext {
			mapOp = &o
		}
	}
	if mapOp == nil {
		t.Fatal("expected the outer Map context operation 'op-serde' to be checkpointed")
	}
	if mapOp.ContextDetails == nil || mapOp.ContextDetails.Result == nil {
		t.Fatal("expected the outer Map context's ContextSucceeded to carry a Result payload")
	}
	if got, want := *mapOp.ContextDetails.Result, "OPSERDE:X,Y"; got != want {
		t.Errorf("expected the outer Map context's checkpointed payload to be the custom serdes's own literal output %q, got %q", want, got)
	}
}

// TestMap_WithMapResultSerdes_DeserializeOnReplay is the deserialize half
// (conformance requirement 9-20, "Map with an operation-level serdes -
// deserialize on replay"): after the Map context is already checkpointed
// SUCCEEDED with a custom-serdes payload, a LATER invocation that replays
// past it (simulating the real backend re-invoking after an intervening
// suspension resolves) must reconstruct the BatchResult by calling the
// custom serdes's own Deserialize on the checkpointed payload - proven
// here by asserting the correct post-replay result, which only
// round-trips correctly if Deserialize genuinely ran.
//
// Uses a failing Step with a non-zero retry delay (rather than Wait) to
// force genuine suspension, mirroring
// TestStep_RetryDelaySuspendsThenResumes's own established pattern in
// this same test package: fakeClient auto-resolves a Wait/START action
// synchronously (see durable_wait_test.go's own
// TestWait_SuspendsThenResumes doc for why that test can't observe true
// suspension either), but does NOT auto-resolve a RETRY action, so a
// Step retry after the Map genuinely suspends the whole invocation here.
func TestMap_WithMapResultSerdes_DeserializeOnReplay(t *testing.T) {
	client := newFakeClient()
	mapBodyExecutions := 0
	retryAttempts := 0

	handler := func(event orderEvent, dc types.DurableContext) ([]string, error) {
		batch, err := operations.Map(dc, "op-serde-replay", []string{"x", "y"},
			func(child types.DurableContext, item string, index int) (string, error) {
				mapBodyExecutions++
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return strings.ToUpper(item), nil
				})
			},
			operations.WithMapMaxConcurrency[string, string](1),
			operations.WithMapResultSerdes[string, string](opSerdes{}),
		)
		if err != nil {
			return nil, err
		}
		if batch.HasFailure() {
			t.Errorf("expected no failures, got %v", batch.GetErrors())
		}
		// A failing Step with a non-zero retry delay after the map forces
		// a genuine suspend/resume, exactly like 9-20.yaml's own
		// intervening Wait, without relying on fakeClient's own
		// synchronous Wait auto-resolution (see this test's own doc).
		if _, err := operations.Step(dc, "pause", func(sc types.StepContext) (string, error) {
			retryAttempts++
			if sc.Attempt() < 2 {
				return "", fmt.Errorf("forced suspend, attempt %d", sc.Attempt())
			}
			return "resumed", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5))); err != nil {
			return nil, err
		}
		return batch.GetResults(), nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "op-serde-replay-test"})

	firstInput := newInvocationInput("exec-op-serde-replay-1", "arn:test:1", "token-0", eventPayload)
	out, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("unexpected error on first invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING (suspended for the retry delay), got status=%s error=%v", out.Status, out.Error)
	}
	if mapBodyExecutions != 2 {
		t.Fatalf("expected exactly 2 map item executions on the first invocation, got %d", mapBodyExecutions)
	}

	// Second invocation: simulates the real backend re-invoking once the
	// retry delay elapses, seeded with the checkpointed operations from
	// the first invocation (exactly like
	// TestStep_RetryDelaySuspendsThenResumes's own second-invocation
	// setup). The Map context is already checkpointed SUCCEEDED with the
	// custom serdes's own payload - Map's own replay-skip must
	// deserialize it through opSerdes.Deserialize, not the default
	// internal JSON wire format, to reconstruct the correct result.
	var extraOps []types.Operation
	for _, op := range client.snapshot() {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-op-serde-replay-1", "arn:test:1", "token-1", eventPayload, extraOps...)
	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("unexpected error on second invocation: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded on the second (replay) invocation, got status=%s error=%v", out2.Status, out2.Error)
	}
	// mapBodyExecutions must NOT have grown - the outer Map context's own
	// replay-skip (via the custom serdes's Deserialize) must short-circuit
	// re-running the item loop entirely, exactly like the default-serdes
	// case already does.
	if mapBodyExecutions != 2 {
		t.Fatalf("expected the map item loop to NOT re-run on replay (replay-skip via custom Deserialize), but mapBodyExecutions grew to %d", mapBodyExecutions)
	}
}

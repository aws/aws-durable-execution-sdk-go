// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, asserting on the actual checkpointed operations (via the
// event-history golden-file pattern), not just the final result.
//
// TestHandler_ChecksSnapshotUsesCustomSerdes additionally asserts on the
// RAW checkpointed payload string for the "snapshot-inventory" step,
// confirming it is genuinely SCREAMING_SNAKE_CASE JSON - i.e. that
// operations.WithStepSerdes(screamingSnakeCaseSerdes{}) actually ran and
// changed the wire representation, not just that the step's
// deserialized Go-side result still round-trips correctly (which could
// pass even if WithStepSerdes were silently ignored, since the default
// JSON Serdes would ALSO successfully round-trip this same struct - the
// custom Serdes's key-casing side effect is the only observable proof
// it's genuinely in the loop).
package main

import (
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_SnapshotsInventory(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(InventorySnapshotEvent{WarehouseID: "warehouse-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[InventorySnapshotResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.WarehouseID != "warehouse-1" {
		t.Fatalf("expected WarehouseID to round-trip, got %q", out.WarehouseID)
	}
	if out.ItemCount != 42 {
		t.Fatalf("expected ItemCount 42, got %d", out.ItemCount)
	}

	snapshotStep, ok := result.GetOperation("snapshot-inventory")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'snapshot-inventory'")
	}
	if snapshotStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected snapshot-inventory step SUCCEEDED, got %s", snapshotStep.GetStatus())
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/custom-config-go/... -run TestHandler_SnapshotsInventory
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_SnapshotsInventory.history.json")
}

// TestHandler_ChecksSnapshotUsesCustomSerdes confirms the custom
// screamingSnakeCaseSerdes genuinely ran for the "snapshot-inventory"
// step's checkpointed result - see this file's top-level doc for why
// checking the RAW wire payload, not just the deserialized Go value, is
// the only way to prove this (a silently-ignored WithStepSerdes option
// would still produce a correctly round-tripping Go value via the
// default JSON Serdes, making that alone insufficient proof).
func TestHandler_ChecksSnapshotUsesCustomSerdes(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(InventorySnapshotEvent{WarehouseID: "warehouse-2"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	snapshotStep, ok := result.GetOperation("snapshot-inventory")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'snapshot-inventory'")
	}
	details := snapshotStep.GetStepDetails()
	if details == nil || details.Result == nil {
		t.Fatal("expected the snapshot-inventory step to have a recorded checkpointed result")
	}
	raw := *details.Result

	// screamingSnakeCaseSerdes rekeys inventorySnapshot's own
	// snake_case json tags ("item_count", "snapshotted_at") to
	// SCREAMING_SNAKE_CASE on the wire - if this string instead
	// contained the ORIGINAL lowercase keys, WithStepSerdes's custom
	// Serdes was silently not applied.
	if !strings.Contains(raw, `"ITEM_COUNT"`) {
		t.Fatalf("expected the checkpointed payload to contain the rekeyed field \"ITEM_COUNT\" (proving screamingSnakeCaseSerdes ran), got %q", raw)
	}
	if !strings.Contains(raw, `"SNAPSHOTTED_AT"`) {
		t.Fatalf("expected the checkpointed payload to contain the rekeyed field \"SNAPSHOTTED_AT\" (proving screamingSnakeCaseSerdes ran), got %q", raw)
	}
	if strings.Contains(raw, `"item_count"`) || strings.Contains(raw, `"snapshotted_at"`) {
		t.Fatalf("expected the checkpointed payload to NOT contain the original lowercase keys (screamingSnakeCaseSerdes should have rekeyed them), got %q", raw)
	}

	// And, despite the rekeyed wire format, this handler's own returned
	// result (asserted in TestHandler_SnapshotsInventory above) still
	// comes out correct (ItemCount: 42) - proving the round trip
	// (Serialize on write, Deserialize on read, both through the SAME
	// screamingSnakeCaseSerdes, exactly as operations.Step's cfg.serdes
	// field is used on both paths) genuinely works end-to-end, not just
	// that the wire bytes look different.
}

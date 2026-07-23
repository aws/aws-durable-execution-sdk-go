// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// InventorySnapshotEvent is this example's input shape.
type InventorySnapshotEvent struct {
	WarehouseID string `json:"warehouseId"`
}

// InventorySnapshotResult is this example's output shape (this is the
// HANDLER's own return value, serialized by Lambda's normal JSON
// marshaling via durable.WithDurableExecution's outcomeToResponse - NOT
// by this example's custom Serdes, which only applies to the
// "snapshot-inventory" STEP's own CHECKPOINTED result - see handler's
// doc for why those are two different serialization boundaries).
type InventorySnapshotResult struct {
	WarehouseID string `json:"warehouseId"`
	ItemCount   int    `json:"itemCount"`
}

// inventorySnapshot is the STEP's own result type - what
// screamingSnakeCaseSerdes actually (de)serializes. A distinct type from
// InventorySnapshotResult specifically so this example demonstrates a
// custom Serdes's effect on ONE step's checkpointed payload without
// also changing the handler's overall JSON output shape (a separate,
// unrelated serialization boundary - see this file's top-level doc).
type inventorySnapshot struct {
	ItemCount     int    `json:"item_count"`
	SnapshottedAt string `json:"snapshotted_at"`
}

// screamingSnakeCaseSerdes is a custom types.Serdes implementation
// demonstrating the interface's extensibility point (see types.go's
// Serdes doc: "Implement this interface to support alternate storage
// strategies"). Rather than a storage-offload strategy (this SDK's
// confirmed real use case for a custom Serdes per
// docs/remaining-work.md §6 task 16's research - see that task's writeup
// for why an automatic large-payload-offload Serdes, like the official
// guide's documented FileSystem SerDes, is a separate, larger,
// infrastructure-dependent feature this example does not attempt to
// build), this is a deliberately simple, self-contained demonstration:
// it re-keys every top-level JSON field from the default JSON Serdes'
// camelCase/whatever-the-struct-tag-says convention to
// SCREAMING_SNAKE_CASE on the wire, and reverses the transformation on
// deserialize - a realistic reason a caller might reach for a custom
// Serdes (matching an external system's own naming convention for
// checkpointed data, e.g. a legacy analytics pipeline that expects
// SCREAMING_SNAKE_CASE keys) without requiring any additional
// infrastructure (S3, EFS, etc.) to demonstrate.
type screamingSnakeCaseSerdes struct{}

func (screamingSnakeCaseSerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: marshaling value for entity %q: %w", entityID, err)
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(b, &asMap); err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: value for entity %q did not marshal to a JSON object: %w", entityID, err)
	}
	rekeyed := make(map[string]json.RawMessage, len(asMap))
	for k, v := range asMap {
		rekeyed[toScreamingSnakeCase(k)] = v
	}
	out, err := json.Marshal(rekeyed)
	if err != nil {
		return "", fmt.Errorf("screamingSnakeCaseSerdes: re-marshaling rekeyed value for entity %q: %w", entityID, err)
	}
	return string(out), nil
}

func (screamingSnakeCaseSerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(pointer), &asMap); err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: checkpointed value for entity %q is not a JSON object: %w", entityID, err)
	}
	rekeyed := make(map[string]json.RawMessage, len(asMap))
	for k, v := range asMap {
		rekeyed[fromScreamingSnakeCase(k)] = v
	}
	out, err := json.Marshal(rekeyed)
	if err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: re-marshaling un-rekeyed value for entity %q: %w", entityID, err)
	}
	var v any
	if err := json.Unmarshal(out, &v); err != nil {
		return nil, fmt.Errorf("screamingSnakeCaseSerdes: unmarshaling un-rekeyed value for entity %q: %w", entityID, err)
	}
	return v, nil
}

// toScreamingSnakeCase converts a snake_case (this example's own struct
// tags, e.g. "item_count") or camelCase key to SCREAMING_SNAKE_CASE
// (e.g. "ITEM_COUNT"). Only snake_case input is actually exercised by
// this example's own inventorySnapshot struct tags, but camelCase is
// handled too so this helper is a genuinely reusable "make this
// screaming" transform, not one narrowly special-cased to this
// example's own field names.
func toScreamingSnakeCase(key string) string {
	var b strings.Builder
	for i, r := range key {
		if r >= 'A' && r <= 'Z' && i > 0 {
			b.WriteByte('_')
		}
		if r == '_' {
			b.WriteByte('_')
			continue
		}
		b.WriteRune(toUpperASCII(r))
	}
	return b.String()
}

// fromScreamingSnakeCase converts a SCREAMING_SNAKE_CASE key (e.g.
// "ITEM_COUNT") back to snake_case (e.g. "item_count") - the exact
// inverse of toScreamingSnakeCase for the underscore-separated keys this
// example's own struct tags use, so a value serialized then deserialized
// through screamingSnakeCaseSerdes round-trips into the same Go struct
// shape unmarshal.Unmarshal expects (matching inventorySnapshot's own
// snake_case json tags).
func fromScreamingSnakeCase(key string) string {
	return strings.ToLower(key)
}

func toUpperASCII(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 'a' + 'A'
	}
	return r
}

// handler demonstrates durable.Config customization
// (docs/remaining-work.md §10 task 23's "custom-config" catalog entry):
//
//  1. A non-default CheckpointStrategy (main.go's Config literal) -
//     shown for API-surface completeness, NOT because it changes this
//     example's observable behavior. durable.go's own doc is explicit
//     that CheckpointStrategy is "accepted ... for API compatibility
//     with the design surface but not yet wired to distinct runtime
//     behavior" - this example's doc comments repeat that honestly
//     rather than implying batching actually happens differently today.
//  2. A custom LoggerConfig (main.go's Config literal) - ModeAware: true
//     set explicitly (see types.LoggerConfig.ModeAware's own doc on the
//     Go-specific default nuance this sidesteps).
//  3. A custom types.Serdes (screamingSnakeCaseSerdes, this file) applied
//     to ONE step's checkpointed result via operations.WithStepSerdes -
//     no existing example in this repo demonstrates a custom Serdes
//     (confirmed by grepping for WithStepSerdes/types.Serdes across
//     examples/ before writing this).
func handler(event InventorySnapshotEvent, dc types.DurableContext) (InventorySnapshotResult, error) {
	dc.Logger().Info("handler started", map[string]any{"warehouseId": event.WarehouseID})

	snapshot, err := operations.Step(dc, "snapshot-inventory",
		func(sc types.StepContext) (inventorySnapshot, error) {
			// In production this would query a real inventory system.
			sc.Logger().Info("taking inventory snapshot", map[string]any{"warehouseId": event.WarehouseID})
			return inventorySnapshot{ItemCount: 42, SnapshottedAt: "2026-01-01T00:00:00Z"}, nil
		},
		operations.WithStepSerdes[inventorySnapshot](screamingSnakeCaseSerdes{}),
	)
	if err != nil {
		return InventorySnapshotResult{}, fmt.Errorf("warehouse %s: snapshotting inventory: %w", event.WarehouseID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"itemCount": snapshot.ItemCount})
	return InventorySnapshotResult{WarehouseID: event.WarehouseID, ItemCount: snapshot.ItemCount}, nil
}

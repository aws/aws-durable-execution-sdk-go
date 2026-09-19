// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
	"github.com/aws/aws-durable-execution-sdk-go/insight"
)

// capturingExporter records every insight record it receives.
type capturingExporter struct {
	mu      sync.Mutex
	records []insight.Record
}

func (e *capturingExporter) Export(_ context.Context, record insight.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)
	return nil
}

func (e *capturingExporter) Flush(_ context.Context) error { return nil }

func (e *capturingExporter) snapshot() []insight.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]insight.Record{}, e.records...)
}

func TestHandler(t *testing.T) {
	exp := &capturingExporter{}
	plugin := newInsightPlugin(exp)

	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(plugin.Plugin()))
	result := runner.RunUntilComplete(t, map[string]any{"orderId": "ord-42"})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	if output.OrderID != "ord-42" {
		t.Errorf("expected orderId %q, got %q", "ord-42", output.OrderID)
	}
	if output.Message != "processed order ord-42" {
		t.Errorf("expected message %q, got %q", "processed order ord-42", output.Message)
	}

	// --- Insight plugin assertions: prove the plugin emitted records ---

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 exported record, got %d", len(records))
	}
	rec := records[0]

	if len(rec.Operations) != 1 {
		t.Fatalf("expected exactly 1 operation record, got %d", len(rec.Operations))
	}
	op := rec.Operations[0]

	if op.Name != "process-order" {
		t.Errorf("expected operation name %q, got %q", "process-order", op.Name)
	}

	if op.Status == "" {
		t.Error("expected operation to carry a status, got empty string")
	}

	if op.DurationMs == nil || *op.DurationMs < 0 {
		t.Errorf("expected operation to carry non-negative timing (DurationMs), got %v", op.DurationMs)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

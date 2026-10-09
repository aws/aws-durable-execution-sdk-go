// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// sampleOrder is the order in examples/event.json.
var sampleOrder = Order{
	OrderID: "ORD-12345",
	Items: []LineItem{
		{SKU: "WIDGET-A", Quantity: 2, Price: 29.99},
		{SKU: "GADGET-B", Quantity: 1, Price: 49.99},
	},
}

// itemPayloads returns the checkpointed result of every succeeded Map
// item, sorted, because concurrent items checkpoint in scheduling order.
func itemPayloads(result *durabletest.TestResult) []string {
	var payloads []string
	for _, op := range result.OperationsByType("CONTEXT") {
		if op.SubType == "MapIteration" && op.Status == "SUCCEEDED" && op.ContextDetails != nil {
			payloads = append(payloads, op.ContextDetails.Result)
		}
	}
	slices.Sort(payloads)
	return payloads
}

// aggregateRecord decodes the Map operation's checkpointed result, which
// resultSerdes wrote, without going through resultSerdes.
func aggregateRecord(t *testing.T, result *durabletest.TestResult) batchRecord {
	t.Helper()
	op := result.Operation("price-lines")
	if op == nil || op.ContextDetails == nil {
		t.Fatalf("no checkpointed result for Map price-lines: %+v", op)
	}
	zipped, err := base64.StdEncoding.DecodeString(op.ContextDetails.Result)
	if err != nil {
		t.Fatalf("aggregate is not base64 (resultSerdes not used?): %v\npayload: %s", err, op.ContextDetails.Result)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		t.Fatalf("aggregate is not gzip: %v", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read aggregate: %v", err)
	}
	var rec batchRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("decode aggregate: %v", err)
	}
	return rec
}

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(sampleOrder)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	// The output is built on the second invocation, from the aggregate
	// that resultSerdes decoded when that invocation replayed the Map.
	if len(result.Invocations) < 2 {
		t.Fatalf("expected the wait to start a second invocation, got %d invocation(s)", len(result.Invocations))
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := Output{
		OrderID: "ORD-12345",
		Lines: []Line{
			{SKU: "WIDGET-A", Quantity: 2, TotalCents: 5998},
			{SKU: "GADGET-B", Quantity: 1, TotalCents: 4999},
		},
		Rejected:   []string{},
		TotalCents: 10997,
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("output = %+v, want %+v", out, want)
	}

	// Each item's checkpoint holds the text lineSerdes wrote. The default
	// serdes would have written a JSON object.
	if got, wantPayloads := itemPayloads(result), []string{"GADGET-B|1|4999", "WIDGET-A|2|5998"}; !slices.Equal(got, wantPayloads) {
		t.Errorf("item payloads = %q, want %q", got, wantPayloads)
	}

	// The Map's own checkpoint holds the compressed record resultSerdes
	// wrote, not the SDK's default aggregate.
	rec := aggregateRecord(t, result)
	if rec.Reason != durable.CompletionAllCompleted || len(rec.Items) != 2 {
		t.Errorf("aggregate record = %+v, want 2 items completed with ALL_COMPLETED", rec)
	}

	// Map items checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}

// TestHandlerRejectedLine prices an order with an invalid line. The
// failure travels through resultSerdes as a message and is reported after
// replay.
func TestHandlerRejectedLine(t *testing.T) {
	order := Order{
		OrderID: "ORD-67890",
		Items: []LineItem{
			{SKU: "WIDGET-A", Quantity: 3, Price: 29.99},
			{SKU: "GADGET-B", Quantity: 0, Price: 49.99},
		},
	}
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(order)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if want := []Line{{SKU: "WIDGET-A", Quantity: 3, TotalCents: 8997}}; !reflect.DeepEqual(out.Lines, want) {
		t.Errorf("lines = %+v, want %+v", out.Lines, want)
	}
	if len(out.Rejected) != 1 || !strings.Contains(out.Rejected[0], "line 1 (GADGET-B): quantity must be positive, got 0") {
		t.Errorf("rejected = %q, want the GADGET-B quantity error", out.Rejected)
	}
	if out.TotalCents != 8997 {
		t.Errorf("total = %d, want 8997", out.TotalCents)
	}

	// The record stores the failure by message, so it can be decoded.
	rec := aggregateRecord(t, result)
	if len(rec.Items) != 2 || rec.Items[1].Status != durable.BatchItemFailed || rec.Items[1].Err == "" {
		t.Errorf("aggregate record = %+v, want item 1 failed with its message", rec)
	}

	// Map items checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignatureFile(t, result, extest.Unordered, "testdata/signature.rejected.golden")
}

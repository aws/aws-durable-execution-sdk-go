// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(Input{OrderID: "ORD-12345"})
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
	if out.Message != "confirmed order ORD-12345" {
		t.Errorf("unexpected output %+v", out)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// logRecords decodes the JSON log lines in buf.
func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, r)
	}
	return records
}

// TestEntryPointPassesPayloadThrough checks the middleware on its own: the
// payload reaches the wrapped function byte for byte, its response comes
// back unchanged, and one record is logged per invocation.
func TestEntryPointPassesPayloadThrough(t *testing.T) {
	payload := []byte(`{"DurableExecutionArn":"arn","CheckpointToken":"token"}`)
	response := []byte(`{"Status":"SUCCEEDED","Result":"{}"}`)

	var got []byte
	var buf bytes.Buffer
	ep := timedEntryPoint{
		next: func(_ context.Context, p []byte) ([]byte, error) {
			got = p
			return response, nil
		},
		logger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}

	out, err := ep.Invoke(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("wrapped function got payload %s, want %s", got, payload)
	}
	if !bytes.Equal(out, response) {
		t.Errorf("entry point returned %s, want %s", out, response)
	}

	records := logRecords(t, &buf)
	if len(records) != 1 || records[0]["msg"] != "invocation finished" {
		t.Fatalf("expected one \"invocation finished\" record, got %v", records)
	}
	if records[0]["payloadBytes"] != float64(len(payload)) || records[0]["responseBytes"] != float64(len(response)) {
		t.Errorf("unexpected sizes in %v", records[0])
	}
}

// TestEntryPointRejectsPlainEvent sends the example event as it is, not
// wrapped in a durable invocation payload. The function durable.Wrap
// returned rejects it, which is what a function deployed without
// DurableConfig would see, and the middleware logs the failure.
func TestEntryPointRejectsPlainEvent(t *testing.T) {
	event, err := os.ReadFile("../event.json")
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	_, err = newEntryPoint(slog.New(slog.NewJSONHandler(&buf, nil))).Invoke(context.Background(), event)
	if err == nil || !strings.Contains(err.Error(), "DurableConfig") {
		t.Fatalf("expected the missing durable payload to be reported, got %v", err)
	}

	records := logRecords(t, &buf)
	if len(records) != 1 || records[0]["msg"] != "invocation failed" {
		t.Fatalf("expected one \"invocation failed\" record, got %v", records)
	}
}

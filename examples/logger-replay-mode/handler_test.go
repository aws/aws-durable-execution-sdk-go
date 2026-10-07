// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// lockedBuffer is a goroutine-safe buffer for the handler's output.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// run runs the handler to completion with its log records written as JSON
// to a buffer, and returns the result and the decoded records.
func run(t *testing.T, in Input, opts ...durable.HandlerOption) (*durabletest.TestResult, []map[string]any) {
	t.Helper()
	var out lockedBuffer
	opts = append([]durable.HandlerOption{durable.WithLogHandler(slog.NewJSONHandler(&out, nil))}, opts...)
	runner := durabletest.NewLocalRunner(handler, opts...)
	result, err := runner.RunUntilComplete(in)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output != "done" {
		t.Errorf("expected %q, got %q", "done", output)
	}

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		records = append(records, rec)
	}
	return result, records
}

// lines renders each record as its message, followed by " (replay)" when
// the record carries replay=true.
func lines(records []map[string]any) []string {
	var got []string
	for _, rec := range records {
		line, _ := rec["msg"].(string)
		if rec["replay"] == true {
			line += " (replay)"
		}
		got = append(got, line)
	}
	return got
}

// suppressed is the output when replayed records are dropped: each line
// once, none marked as replayed.
var suppressed = []string{"before wait", "inside step", "after step"}

// emitted is the output when replayed records are written: the second
// invocation writes the line before the wait again, marked as replayed.
var emitted = []string{"before wait", "before wait (replay)", "inside step", "after step"}

// TestHandler runs with neither setting, so replayed records are
// suppressed.
func TestHandler(t *testing.T) {
	result, records := run(t, Input{})
	if got := lines(records); !slices.Equal(got, suppressed) {
		t.Errorf("log lines = %q, want %q", got, suppressed)
	}
	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerEmitFromEvent turns emission on for one execution with
// ConfigureLogging.
func TestHandlerEmitFromEvent(t *testing.T) {
	result, records := run(t, Input{ReplayLogs: "emit"})
	if got := lines(records); !slices.Equal(got, emitted) {
		t.Errorf("log lines = %q, want %q", got, emitted)
	}
	// The log mode changes no operation, so the signature is the default
	// scenario's.
	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerEmitFunctionWide turns emission on for the whole function
// with WithReplayLogMode, as REPLAY_LOGS=emit does for the deployed
// function. An event without replayLogs keeps that mode.
func TestHandlerEmitFunctionWide(t *testing.T) {
	result, records := run(t, Input{}, durable.WithReplayLogMode(parseReplayLogMode("emit")))
	if got := lines(records); !slices.Equal(got, emitted) {
		t.Errorf("log lines = %q, want %q", got, emitted)
	}
	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerEventOverridesFunctionWide suppresses replayed records for
// one execution of a function that emits them.
func TestHandlerEventOverridesFunctionWide(t *testing.T) {
	result, records := run(t, Input{ReplayLogs: "suppress"}, durable.WithReplayLogMode(parseReplayLogMode("emit")))
	if got := lines(records); !slices.Equal(got, suppressed) {
		t.Errorf("log lines = %q, want %q", got, suppressed)
	}
	extest.AssertSignature(t, result, extest.Ordered)
}

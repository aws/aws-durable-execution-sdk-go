// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
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

func TestHandler(t *testing.T) {
	var out lockedBuffer
	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(newHandler(&out)))
	result := runner.RunUntilComplete(t, nil)

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

	extest.AssertSignature(t, result, extest.Ordered)

	// Each line is one JSON record with the handler's own field names and
	// the SDK's identifiers as structured attributes. Lines before the
	// wait appear once: the second invocation replays them suppressed.
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		records = append(records, rec)
	}

	wantMessages := []string{
		"=== Logger Level Demo Starting ===",
		"Debug message: Detailed debugging information",
		"Info message: General information about execution",
		"Warning message: Something might need attention",
		"Error message: Something went wrong (simulated)",
		"Before wait operation",
		"Info log from child context",
		"Debug log from child context",
		"Warning log from child context",
		"Error from child context with error value",
		"After wait operation - logger still works",
		"message",
		"message",
		"Errors in context",
	}
	var gotMessages []string
	for _, rec := range records {
		msg, _ := rec["message"].(string)
		gotMessages = append(gotMessages, msg)
	}
	if strings.Join(gotMessages, "\n") != strings.Join(wantMessages, "\n") {
		t.Fatalf("messages =\n%s\nwant\n%s", strings.Join(gotMessages, "\n"), strings.Join(wantMessages, "\n"))
	}

	for i, rec := range records {
		if rec["service"] != serviceName {
			t.Errorf("record %d: service = %v, want %s", i, rec["service"], serviceName)
		}
		if _, ok := rec["execution_arn"]; !ok {
			t.Errorf("record %d: missing execution_arn: %v", i, rec)
		}
		if _, ok := rec["request_id"]; !ok {
			t.Errorf("record %d: missing request_id: %v", i, rec)
		}
		for _, absent := range []string{"msg", "time", "requestId", "executionArn"} {
			if _, ok := rec[absent]; ok {
				t.Errorf("record %d: key %q must be renamed by the handler: %v", i, absent, rec)
			}
		}
	}

	levels := []string{"INFO", "DEBUG", "INFO", "WARN", "ERROR"}
	for i, want := range levels {
		if records[i]["level"] != want {
			t.Errorf("record %d: level = %v, want %s", i, records[i]["level"], want)
		}
	}

	// The step records carry the operation attributes; the handler-body
	// records do not.
	for _, i := range []int{11, 12} {
		rec := records[i]
		if rec["operation_name"] != "direct-object-step" || rec["attempt"] != float64(1) {
			t.Errorf("step record %d: operation_name/attempt = %v/%v, want direct-object-step/1", i, rec["operation_name"], rec["attempt"])
		}
		if _, ok := rec["operation_id"]; !ok {
			t.Errorf("step record %d: missing operation_id: %v", i, rec)
		}
	}
	// The child-context records carry the child operation's ID and name
	// but no attempt.
	for i := 6; i <= 9; i++ {
		rec := records[i]
		if rec["operation_name"] != "child-context" {
			t.Errorf("child record %d: operation_name = %v, want child-context", i, rec["operation_name"])
		}
		if _, ok := rec["operation_id"]; !ok {
			t.Errorf("child record %d: missing operation_id: %v", i, rec)
		}
		if _, ok := rec["attempt"]; ok {
			t.Errorf("child record %d: must not carry attempt: %v", i, rec)
		}
	}
	if _, ok := records[0]["operation_id"]; ok {
		t.Errorf("handler-body record must not carry operation_id: %v", records[0])
	}
	if records[11]["stepData"] != "value" || records[11]["num"] != float64(42) {
		t.Errorf("step record attributes = %v, want stepData=value num=42", records[11])
	}
	// An error value passes through the handler as the attribute it was
	// given; the default handler's errorType expansion is specific to the
	// default handler.
	if got, _ := records[12]["err"].(string); got != "step context direct error" {
		t.Errorf("step error record err = %v, want the error text", records[12]["err"])
	}
}

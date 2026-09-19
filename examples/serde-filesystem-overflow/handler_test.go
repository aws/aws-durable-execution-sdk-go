// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// envelope is the checkpoint envelope the filesystem serdes writes. In
// overflow mode exactly one of the two fields is set: Data holds the value's
// JSON when it fits inline, File names the overflow file when it does not.
// Both fields are optional here because the test asserts each one's
// absence as well as the other's presence.
type envelope struct {
	Data *string `json:"data"`
	File string  `json:"file"`
}

// envelopeOf decodes the checkpointed result of the named step.
func envelopeOf(t *testing.T, result *durabletest.TestResult, name string) envelope {
	t.Helper()
	step := result.Operation(name)
	if step == nil || step.StepDetails == nil {
		t.Fatalf("%s step not found", name)
	}
	if step.Status != "SUCCEEDED" {
		t.Fatalf("%s status = %s", name, step.Status)
	}
	var env envelope
	if err := json.Unmarshal([]byte(step.StepDetails.Result), &env); err != nil {
		t.Fatalf("decode %s envelope %q: %v", name, step.StepDetails.Result, err)
	}
	return env
}

func TestHandler(t *testing.T) {
	serdesDir := t.TempDir()
	t.Setenv("SERDES_BASE_PATH", serdesDir)

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	// One invocation: the handler never suspends, so every serdes round
	// trip happens in the invocation that wrote the file. This is what
	// makes the local base path in basePath sufficient for the example.
	if got := len(result.Invocations); got != 1 {
		t.Fatalf("expected 1 invocation, got %d", got)
	}
	// small-record + large-document + combine.
	if len(result.Operations) != 3 {
		t.Fatalf("expected 3 operations, got %d", len(result.Operations))
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.SmallOrderID != "ORD-42" || out.LargeLength != largeSize {
		t.Fatalf("unexpected output %+v", out)
	}

	// The distinction that defines overflow mode: each envelope carries
	// exactly one of data or file, chosen by the value's size.

	// Small value: inline JSON, no file written.
	small := envelopeOf(t, result, "small-record")
	if small.File != "" {
		t.Errorf("small-record was written to a file %q; want inline", small.File)
	}
	if small.Data == nil {
		t.Fatal("small-record envelope has no inline data")
	}
	var rec Record
	if err := json.Unmarshal([]byte(*small.Data), &rec); err != nil {
		t.Fatalf("decode inline data %q: %v", *small.Data, err)
	}
	if rec.OrderID != "ORD-42" || rec.Total != 19.99 {
		t.Errorf("inline record = %+v, want ORD-42 at 19.99", rec)
	}

	// Large value: a file under the base path, nothing inline.
	large := envelopeOf(t, result, "large-document")
	if large.Data != nil {
		t.Errorf("large-document was stored inline (%d bytes); want a file", len(*large.Data))
	}
	if !strings.HasPrefix(large.File, serdesDir) {
		t.Fatalf("file %q not under base path %q", large.File, serdesDir)
	}
	contents, err := os.ReadFile(large.File)
	if err != nil {
		t.Fatalf("overflow file: %v", err)
	}
	var doc string
	if err := json.Unmarshal(contents, &doc); err != nil {
		t.Fatalf("decode overflow file: %v", err)
	}
	if len(doc) != largeSize {
		t.Errorf("overflow file holds %d characters, want %d", len(doc), largeSize)
	}

	// The serdes mode changes where payloads are stored, not which
	// operations run.
	extest.AssertSignature(t, result, extest.Ordered)
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// envelope is the checkpoint envelope the filesystem serdes writes. In
// Always mode File is always set and Data is never set; Data is decoded
// here so the test can assert its absence.
type envelope struct {
	Data    *string        `json:"data"`
	File    string         `json:"file"`
	Preview map[string]any `json:"preview"`
}

// envelopeOf decodes the checkpointed result of the named step.
func envelopeOf(t *testing.T, result *durabletest.TestResult, name string) (envelope, *durabletest.TestOperation) {
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
	return env, step
}

// hexDir matches the directory name of the hashed layout: the first 16
// bytes of the SHA-256 digest of the execution ARN, hex encoded.
var hexDir = regexp.MustCompile(`^[0-9a-f]{32}$`)

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
	// generate-report + summarize-report.
	if len(result.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(result.Operations))
	}

	// The rehydrated report round-tripped through the file: the body
	// length survives, so deserialize read the file back.
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	wantBody := len(strings.Repeat("REPORT-", bodyRepeat))
	if out.ID != "RPT-001" || out.Status != "generated" || out.BodyLength != wantBody {
		t.Fatalf("unexpected output %+v", out)
	}

	env, step := envelopeOf(t, result, "generate-report")
	raw := step.StepDetails.Result

	// The checkpoint holds a file reference and a preview, never the body.
	if env.Data != nil {
		t.Errorf("generate-report was stored inline; want a file: %s", raw)
	}
	if env.File == "" {
		t.Fatalf("generate-report envelope has no file: %s", raw)
	}
	if strings.Contains(raw, "REPORT-REPORT-") {
		t.Errorf("envelope contains the report body: %s", raw)
	}
	if !strings.HasPrefix(env.File, serdesDir) {
		t.Fatalf("file %q not under base path %q", env.File, serdesDir)
	}

	// Hashed layout: <basePath>/<32 hex chars>/<operationID>.json, where
	// the operation ID is the positional ID of the step ("1" for the first
	// top-level operation). The readable default layout would put a
	// percent-encoded ARN segment in the directory name instead, which is
	// not 32 hex characters.
	if dir := filepath.Base(filepath.Dir(env.File)); !hexDir.MatchString(dir) {
		t.Errorf("directory %q is not the hashed layout", dir)
	}
	if got := filepath.Base(env.File); got != "1.json" {
		t.Errorf("file name = %q, want %q", got, "1.json")
	}

	// PreviewExcludeAll with id and status included: both are present.
	p := env.Preview
	if p["id"] != "RPT-001" || p["status"] != "generated" {
		t.Errorf("preview should include id and status: %s", raw)
	}
	// ownerEmail is masked: present, redacted, and the real value never
	// appears in the envelope.
	if p["ownerEmail"] != "***" {
		t.Errorf("ownerEmail should be masked: %s", raw)
	}
	if strings.Contains(raw, "owner@example.com") {
		t.Errorf("envelope leaks the masked value: %s", raw)
	}
	// The body is not an included field, so PreviewExcludeAll keeps it out.
	if _, ok := p["body"]; ok {
		t.Errorf("body must not be in the preview: %s", raw)
	}

	// The file holds the full value, including the masked field and the
	// body. Masking in the preview does not redact the file.
	contents, err := os.ReadFile(env.File)
	if err != nil {
		t.Fatal(err)
	}
	var stored Report
	if err := json.Unmarshal(contents, &stored); err != nil {
		t.Fatalf("decode offloaded file: %v", err)
	}
	if stored.OwnerEmail != "owner@example.com" || len(stored.Body) != wantBody {
		t.Errorf("offloaded file should hold the full report, got id=%s owner=%s bodyLength=%d",
			stored.ID, stored.OwnerEmail, len(stored.Body))
	}

	// Always mode offloads every value, so the small summary is written
	// to a file as well. Overflow mode would store it inline.
	summary, _ := envelopeOf(t, result, "summarize-report")
	if summary.Data != nil || summary.File == "" {
		t.Errorf("summarize-report should be offloaded in Always mode: %+v", summary)
	}

	// The serdes changes where payloads are stored, not which operations
	// run.
	extest.AssertSignature(t, result, extest.Ordered)
}

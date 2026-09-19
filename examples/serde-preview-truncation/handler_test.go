// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// envelope is the checkpoint envelope the filesystem serdes writes when the
// value is offloaded: the file reference plus the inline preview.
type envelope struct {
	File    string         `json:"file"`
	Preview map[string]any `json:"preview"`
}

func TestHandler(t *testing.T) {
	serdesDir := t.TempDir()
	t.Setenv("SERDES_BASE_PATH", serdesDir)

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	// build-record + read-record, in one invocation: the serdes round trip
	// needs no replay.
	if len(result.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(result.Operations))
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.ID != "acct-123" || out.Tier != "gold" || out.NotesLength != 500 {
		t.Fatalf("unexpected output %+v", out)
	}

	step := result.Operation("build-record")
	if step == nil || step.StepDetails == nil {
		t.Fatal("build-record step not found")
	}
	if step.Status != "SUCCEEDED" {
		t.Fatalf("build-record status = %s", step.Status)
	}

	var env envelope
	if err := json.Unmarshal([]byte(step.StepDetails.Result), &env); err != nil {
		t.Fatalf("decode envelope %q: %v", step.StepDetails.Result, err)
	}
	if !strings.HasPrefix(env.File, serdesDir) {
		t.Fatalf("file %q not under base path %q", env.File, serdesDir)
	}
	if _, err := os.Stat(env.File); err != nil {
		t.Fatalf("offloaded file: %v", err)
	}

	previewJSON, err := json.Marshal(env.Preview)
	if err != nil {
		t.Fatal(err)
	}

	// The excluded secret is never present, even under PreviewIncludeAll.
	if _, ok := env.Preview["internalSecret"]; ok {
		t.Errorf("preview contains excluded field: %s", previewJSON)
	}
	if strings.Contains(string(previewJSON), "do-not-log-me") {
		t.Errorf("preview leaks excluded value: %s", previewJSON)
	}

	// The small leading fields fit; notes and history did not.
	if env.Preview["id"] != "acct-123" || env.Preview["region"] != "us-west-2" || env.Preview["tier"] != "gold" {
		t.Errorf("preview missing leading fields: %s", previewJSON)
	}
	if _, ok := env.Preview["notes"]; ok {
		t.Errorf("preview should have dropped notes: %s", previewJSON)
	}
	if _, ok := env.Preview["history"]; ok {
		t.Errorf("preview should have dropped history: %s", previewJSON)
	}

	// Truncation is visible, and the cap holds on the encoded preview.
	if env.Preview[durable.PreviewTruncatedKey] != true {
		t.Errorf("preview lacks truncation marker: %s", previewJSON)
	}
	if len(previewJSON) > previewMaxBytes {
		t.Errorf("preview is %d bytes, cap is %d", len(previewJSON), previewMaxBytes)
	}

	// Excluding a field from the preview does not remove it from the file.
	contents, err := os.ReadFile(env.File)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "do-not-log-me") {
		t.Error("offloaded file should hold the full value, including the excluded field")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

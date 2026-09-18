// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// envelope is the checkpoint envelope the filesystem serdes writes when the
// value is offloaded: the file reference plus the inline preview.
type envelope struct {
	File    string `json:"file"`
	Preview struct {
		ID       any `json:"id"`
		Email    any `json:"email"`
		Status   any `json:"status"`
		AuditLog any `json:"auditLog"`
		Customer struct {
			ID    any `json:"id"`
			Email any `json:"email"`
			SSN   any `json:"ssn"`
		} `json:"customer"`
	} `json:"preview"`
}

func TestHandler(t *testing.T) {
	serdesDir := t.TempDir()
	t.Setenv("SERDES_BASE_PATH", serdesDir)

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	// build-profile + read-profile, in one invocation: the serdes round
	// trip needs no replay.
	if len(result.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(result.Operations))
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.ID != "cust-9" || out.Email != "decoy@example.com" ||
		out.CustomerEmail != "person@example.com" || out.AuditLength != 2000 {
		t.Fatalf("unexpected output %+v", out)
	}

	step := result.Operation("build-profile")
	if step == nil || step.StepDetails == nil {
		t.Fatal("build-profile step not found")
	}
	if step.Status != "SUCCEEDED" {
		t.Fatalf("build-profile status = %s", step.Status)
	}

	raw := step.StepDetails.Result
	var env envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode envelope %q: %v", raw, err)
	}
	if !strings.HasPrefix(env.File, serdesDir) {
		t.Fatalf("file %q not under base path %q", env.File, serdesDir)
	}

	p := env.Preview

	// id matches anywhere (the default), so it appears at the root and
	// inside customer.
	if p.ID != "cust-9" || p.Customer.ID != "cust-9" {
		t.Errorf("id should appear at both depths: %s", raw)
	}

	// customer.email is a path selector: the nested field is included and
	// the root field with the same name is not. Under anywhere matching a
	// dotted selector matches nothing, so this assertion also fails if the
	// match mode is dropped.
	if p.Customer.Email != "person@example.com" {
		t.Errorf("customer.email should be included: %s", raw)
	}
	if p.Email != nil || strings.Contains(raw, "decoy@example.com") {
		t.Errorf("root email must not be included by the path selector: %s", raw)
	}

	// ssn is masked: present, redacted, and the real value never appears
	// in the envelope.
	if p.Customer.SSN != "***" {
		t.Errorf("ssn should be masked: %s", raw)
	}
	if strings.Contains(raw, "123-45-6789") {
		t.Errorf("envelope leaks the masked value: %s", raw)
	}

	// PreviewExcludeAll keeps everything else out.
	if p.Status != nil || p.AuditLog != nil {
		t.Errorf("status and auditLog must not be in the preview: %s", raw)
	}

	// Masking in the preview does not redact the offloaded file.
	contents, err := os.ReadFile(env.File)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "123-45-6789") {
		t.Error("offloaded file should hold the full value, including the masked field")
	}
}

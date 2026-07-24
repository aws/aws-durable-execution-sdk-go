// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%+v)", result.Status, result.Error)
	}

	out, err := durabletest.ResultAs[runIfResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Branch != "review" {
		t.Errorf("branch = %q, want review", out.Branch)
	}
	if out.Succeeded != 2 || out.Failed != 0 || out.Skipped != 2 || out.Total != 4 {
		t.Errorf("counts = %+v, want succeeded=2 failed=0 skipped=2 total=4", out)
	}
	if out.Reason != "ALL_COMPLETED" {
		t.Errorf("reason = %q, want ALL_COMPLETED", out.Reason)
	}
	want := map[string]string{
		"classify": "SUCCEEDED",
		"review":   "SUCCEEDED",
		"publish":  "SKIPPED",
		"block":    "SKIPPED",
	}
	for name, ws := range want {
		if out.Statuses[name] != ws {
			t.Errorf("status[%s] = %q, want %q", name, out.Statuses[name], ws)
		}
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

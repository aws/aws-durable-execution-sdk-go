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

	out, err := durabletest.ResultAs[diamondResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Merge != 31 {
		t.Errorf("merge = %d, want 31", out.Merge)
	}
	if out.Succeeded != 4 || out.Failed != 0 || out.Skipped != 0 || out.Total != 4 {
		t.Errorf("counts = %+v, want succeeded=4 failed=0 skipped=0 total=4", out)
	}
	if out.Reason != "ALL_COMPLETED" {
		t.Errorf("reason = %q, want ALL_COMPLETED", out.Reason)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

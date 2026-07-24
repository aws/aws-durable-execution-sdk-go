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
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !output.Success {
		t.Error("expected Success=true")
	}
	// 6 chunks × 50KB each = 300KB
	expectedSize := 6 * 50 * 1024
	if output.TotalSize != expectedSize {
		t.Errorf("expected TotalSize=%d, got %d", expectedSize, output.TotalSize)
	}
	if output.ChunkCount != 6 {
		t.Errorf("expected ChunkCount=6, got %d", output.ChunkCount)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

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
	result := runner.RunUntilComplete(t, Input{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// The step should succeed locally (no real Lambda timeout in local runner),
	// so the handler reports success from the step result.
	if output.Status != "succeeded" {
		t.Errorf("expected status %q, got %q", "succeeded", output.Status)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

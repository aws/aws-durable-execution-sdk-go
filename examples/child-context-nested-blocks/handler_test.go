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

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Verify the grandchild's leaf value bubbles up through every level.
	if output.GrandchildValue != "deep-value" {
		t.Errorf("GrandchildValue: got %q, want %q", output.GrandchildValue, "deep-value")
	}
	if output.ChildValue != "child-wraps(deep-value)" {
		t.Errorf("ChildValue: got %q, want %q", output.ChildValue, "child-wraps(deep-value)")
	}
	if output.ParentValue != "parent-wraps(child-wraps(deep-value))" {
		t.Errorf("ParentValue: got %q, want %q", output.ParentValue, "parent-wraps(child-wraps(deep-value))")
	}

	// Assert the three-level nesting structure via positional golden file.
	// Pure sequential nesting with no parallelism is deterministic: each
	// RunInChildContext claims its operation ID synchronously on the
	// calling goroutine before entering the child body.
	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

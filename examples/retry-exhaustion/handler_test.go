// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if !strings.Contains(result.Error.Message, "persistent failure") {
		t.Errorf("expected error to contain %q, got %q", "persistent failure", result.Error.Message)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

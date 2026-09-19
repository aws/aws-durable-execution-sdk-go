// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: submitter calls real AWS API (slow without network)")
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	// The submitter step calls the real Lambda API which fails in local
	// testing (no real endpoint). The handler fails at the submitter step.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (submitter needs real AWS), got %s", result.Status)
	}

	// Whichever submitter fails first fails the All; the other branch is
	// left in a scheduling-dependent state. The golden lists the operations
	// every local run produces.
	extest.AssertSignature(t, result, extest.Subset)
}

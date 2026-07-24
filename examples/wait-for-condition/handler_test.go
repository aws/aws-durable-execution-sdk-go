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

	// The WaitForCondition example uses a wait strategy that validates
	// state == attempt. In the local runner, WaitForCondition state is
	// not persisted across retry boundaries in StepDetails.Result,
	// causing the state to reset to InitialState (0) on the second
	// attempt. This triggers the "state does not match attempt" error.
	// On the real backend, state is properly round-tripped via the
	// checkpoint payload.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (local runner does not persist WaitForCondition state across retries), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

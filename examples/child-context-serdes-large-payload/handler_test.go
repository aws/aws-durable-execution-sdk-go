// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandlerDefaultPath(t *testing.T) {
	// Exercise the fallback path used when SERDES_BASE_PATH is unset,
	// proving the deployed Lambda default is writable.
	t.Setenv("SERDES_BASE_PATH", "")

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	// Clean up files written to the default path.
	t.Cleanup(func() { os.RemoveAll("/tmp/durable-serdes") })

	entries, err := os.ReadDir("/tmp/durable-serdes")
	if err != nil {
		t.Fatalf("default path /tmp/durable-serdes not created: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected filesystem serdes to write files under the default path")
	}
}

func TestHandler(t *testing.T) {
	serdesDir := t.TempDir()
	t.Setenv("SERDES_BASE_PATH", serdesDir)

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Verify payload size exceeds the 256KB checkpoint threshold.
	if output.PayloadLength != payloadSize {
		t.Errorf("expected PayloadLength=%d, got %d", payloadSize, output.PayloadLength)
	}

	// Verify payload content integrity via SHA-256 hash.
	expectedPayload := strings.Repeat("ABCDEFGHIJ", payloadSize/10)
	expectedHash := sha256.Sum256([]byte(expectedPayload))
	expectedHashHex := hex.EncodeToString(expectedHash[:])
	if output.PayloadHash != expectedHashHex {
		t.Errorf("payload hash mismatch:\n  got  %s\n  want %s", output.PayloadHash, expectedHashHex)
	}

	// Verify the filesystem serdes wrote at least one file, confirming
	// the filesystem path was exercised rather than inline storage.
	entries, err := os.ReadDir(serdesDir)
	if err != nil {
		t.Fatalf("read serdes dir: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected filesystem serdes to write files under the base path, but directory is empty")
	}
}

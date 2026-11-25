// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	// Replace the submitter so WaitForCallback resolves the callback
	// through the local runner instead of the real Lambda API. The sent
	// payload leaves Metadata.Processed unset so the per-callback serdes
	// is what sets it before the handler reads the result.
	orig := sendCallback
	t.Cleanup(func() { sendCallback = orig })
	sendCallback = func(_ durable.StepContext, callbackID string) error {
		sent := CustomData{
			ID:        42,
			Message:   "serialized callback data",
			Timestamp: "2026-01-01T00:00:00Z",
		}
		sent.Metadata.Version = "1.0"
		return runner.SendCallbackSuccess(callbackID, sent)
	}

	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// JSONSerdes alone would leave Metadata.Processed false.
	if !out.IsProcessed || !out.ReceivedData.Metadata.Processed {
		t.Errorf("expected Metadata.Processed set by processedSerdes, got %+v", out)
	}
	if out.ReceivedData.ID != 42 || out.ReceivedData.Metadata.Version != "1.0" {
		t.Errorf("expected the sent fields unchanged, got %+v", out.ReceivedData)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

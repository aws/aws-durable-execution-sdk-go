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
	res := runner.RunUntilComplete(t, nil)

	// Execution suspends because callbacks need external resolution.
	if res.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", res.Status)
	}

	// Resolve the callback.
	callbacks := runner.OpenCallbacks()
	var cbID string
	for _, cb := range callbacks {
		if cb.Name == "my-callback" {
			cbID = cb.CallbackID
			break
		}
	}
	if cbID == "" {
		t.Fatal("my-callback not found in open callbacks")
	}
	if err := runner.SendCallbackSuccess(cbID, "callback-value"); err != nil {
		t.Fatalf("send callback success: %v", err)
	}

	res = runner.RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", res.Status)
	}

	output, err := durabletest.ResultAs[result](res)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.Message != "done" {
		t.Errorf("expected Message %q, got %q", "done", output.Message)
	}
	if output.Result != "callback-value" {
		t.Errorf("expected Result %q, got %q", "callback-value", output.Result)
	}

	durabletest.AssertGoldenSignature(t, res, filepath.Join("testdata", "signature.golden"))
}

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

	// Execution suspends because callbacks need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callbacks), got %s", result.Status)
	}

	// Resolve callback-1.
	callbacks := runner.OpenCallbacks()
	var cb1ID string
	for _, cb := range callbacks {
		if cb.Name == "callback-1" {
			cb1ID = cb.CallbackID
			break
		}
	}
	if cb1ID == "" {
		t.Fatal("callback-1 not found in open callbacks")
	}
	if err := runner.SendCallbackSuccess(cb1ID, "cb1-done"); err != nil {
		t.Fatalf("send callback-1 success: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback-2), got %s", result.Status)
	}

	// Resolve callback-2.
	callbacks = runner.OpenCallbacks()
	var cb2ID string
	for _, cb := range callbacks {
		if cb.Name == "callback-2" {
			cb2ID = cb.CallbackID
			break
		}
	}
	if cb2ID == "" {
		t.Fatal("callback-2 not found in open callbacks")
	}
	if err := runner.SendCallbackSuccess(cb2ID, "cb2-done"); err != nil {
		t.Fatalf("send callback-2 success: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback-3), got %s", result.Status)
	}

	// Resolve callback-3.
	callbacks = runner.OpenCallbacks()
	var cb3ID string
	for _, cb := range callbacks {
		if cb.Name == "callback-3" {
			cb3ID = cb.CallbackID
			break
		}
	}
	if cb3ID == "" {
		t.Fatal("callback-3 not found in open callbacks")
	}
	if err := runner.SendCallbackSuccess(cb3ID, "cb3-done"); err != nil {
		t.Fatalf("send callback-3 success: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

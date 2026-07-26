// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler, durable.WithCallbackDeserializer(uppercaseDeserializer{}))
	result := runner.RunUntilComplete(t, nil)

	// Execution suspends waiting for the two callbacks.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callbacks), got %s", result.Status)
	}

	// Resolve both callbacks with lowercase payloads. The custom
	// deserializer uppercases them, so the handler receives uppercase.
	callbacks := runner.OpenCallbacks()
	if len(callbacks) < 2 {
		t.Fatalf("expected at least 2 open callbacks, got %d", len(callbacks))
	}

	var cb1ID, cb2ID string
	for _, cb := range callbacks {
		switch cb.Name {
		case "approval-1":
			cb1ID = cb.CallbackID
		case "approval-2":
			cb2ID = cb.CallbackID
		}
	}
	if cb1ID == "" {
		t.Fatal("approval-1 not found in open callbacks")
	}
	if cb2ID == "" {
		t.Fatal("approval-2 not found in open callbacks")
	}

	if err := runner.SendCallbackSuccess(cb1ID, "hello"); err != nil {
		t.Fatalf("send callback-1 success: %v", err)
	}
	if err := runner.SendCallbackSuccess(cb2ID, "world"); err != nil {
		t.Fatalf("send callback-2 success: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s: error=%+v", result.Status, result.Error)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// The custom deserializer uppercases all callback payloads.
	// Default JSON decoding would yield "hello" / "world" — the uppercase
	// proves the handler-level WithCallbackDeserializer ran.
	if output.First != "HELLO" {
		t.Errorf("expected First=%q (uppercased by custom deserializer), got %q", "HELLO", output.First)
	}
	if output.Second != "WORLD" {
		t.Errorf("expected Second=%q (uppercased by custom deserializer), got %q", "WORLD", output.Second)
	}
}

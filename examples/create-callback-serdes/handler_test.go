// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	// Replace the callback sender so the step resolves the callback
	// through the local runner instead of the real Lambda API. Send a
	// lowercase Message so an uppercased result shows the per-callback
	// serdes decoded the payload before the handler saw it.
	orig := sendCallback
	t.Cleanup(func() { sendCallback = orig })
	sendCallback = func(_ context.Context, callbackID string, data CustomData) error {
		sent := data
		sent.Message = strings.ToLower(sent.Message)
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

	// JSONSerdes alone would return the Message unchanged.
	want := Result{ReceivedData: CustomData{
		ID:        99,
		Message:   "CUSTOM SERIALIZED DATA",
		Timestamp: "2026-07-01T12:00:00Z",
	}}
	if out != want {
		t.Errorf("expected %+v, got %+v", want, out)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

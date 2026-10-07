// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Set dummy credentials so config.LoadDefaultConfig resolves quickly
	// and the Lambda API call fails fast with an auth error rather than
	// timing out searching for real credentials.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}

	// The handler's "send-custom-data" Step calls the real Lambda API
	// (SendDurableExecutionCallbackSuccess) which is unavailable in local
	// testing. The step exhausts retries and the execution fails.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (no real AWS endpoint for send-custom-data step), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// verificationHandler creates a callback with messageSerdes and returns
// whatever the callback resolves with. Used to verify that the per-callback
// serdes decodes the payload before it reaches the handler.
func verificationHandler(ctx durable.Context, _ any) (CustomData, error) {
	cb, err := durable.CreateCallback[CustomData](ctx, "verify-serdes",
		durable.WithCallbackTimeout(30*time.Second),
		durable.WithCallbackSerdes(messageSerdes))
	if err != nil {
		return CustomData{}, err
	}
	return cb.Result()
}

func TestCallbackSerdesOverride(t *testing.T) {
	runner := durabletest.NewLocalRunner(verificationHandler)

	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", result.Status)
	}

	callbacks := runner.OpenCallbacks()
	if len(callbacks) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(callbacks))
	}

	// Send a lowercase Message; messageSerdes must uppercase it.
	sent := CustomData{ID: 7, Message: "hello callback", Timestamp: "2026-07-01T12:00:00Z"}
	if err := runner.SendCallbackSuccess(callbacks[0].CallbackID, sent); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	result, err = runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[CustomData](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Default JSON decoding would return the Message unchanged.
	want := CustomData{ID: 7, Message: "HELLO CALLBACK", Timestamp: "2026-07-01T12:00:00Z"}
	if out != want {
		t.Errorf("expected %+v, got %+v", want, out)
	}

	// This scenario runs the verification handler, not the example's;
	// its golden records the resolved callback.
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.verification.golden")
}

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

	runner := durabletest.NewLocalRunner(handler, durable.WithCallbackDeserializer(uppercaseDeserializer{}))
	result := runner.RunUntilComplete(t, nil)

	// The handler's submitter calls the real Lambda API
	// (SendDurableExecutionCallbackSuccess) which is unavailable in local
	// testing. The submitter step exhausts retries and the execution fails.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (submitter needs real AWS), got %s", result.Status)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// verificationHandler creates a callback and returns whatever the callback
// resolves with. Used to verify that the custom deserializer transforms
// the callback payload before it reaches the handler.
func verificationHandler(ctx durable.Context, _ any) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, "verify-deser",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func TestDeserializerTransformation(t *testing.T) {
	runner := durabletest.NewLocalRunner(verificationHandler,
		durable.WithCallbackDeserializer(uppercaseDeserializer{}))

	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", result.Status)
	}

	callbacks := runner.OpenCallbacks()
	if len(callbacks) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(callbacks))
	}

	// Send a lowercase payload; the custom deserializer must uppercase it.
	if err := runner.SendCallbackSuccess(callbacks[0].CallbackID, "hello world"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// The uppercaseDeserializer uppercases all string values during
	// deserialization. Verify the transformation was applied.
	if out != "HELLO WORLD" {
		t.Errorf("expected uppercased result %q, got %q", "HELLO WORLD", out)
	}

	// This scenario runs the verification handler, not the example's;
	// its golden records the resolved callback.
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.verification.golden")
}

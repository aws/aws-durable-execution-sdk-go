// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	var runner *durabletest.LocalRunner[any, Output]
	sendCallback = func(_ context.Context, callbackID, value string) error {
		return runner.SendCallbackSuccess(callbackID, value)
	}
	t.Cleanup(func() { sendCallback = completeCallback })

	runner = durabletest.NewLocalRunner(handler, options...)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.First != "HELLO FIRST" || out.Second != "HELLO SECOND" {
		t.Errorf("expected uppercased results, got %+v", out)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := event{
		FirstName: "Alice",
		LastName:  "Smith",
		Email:     "alice@example.com",
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.User.FirstName != "Alice" {
		t.Errorf("expected FirstName=%q, got %q", "Alice", out.User.FirstName)
	}
	if out.Greeting != "Hello, I'm Alice Smith. My email is alice@example.com" {
		t.Errorf("unexpected greeting: %q", out.Greeting)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}

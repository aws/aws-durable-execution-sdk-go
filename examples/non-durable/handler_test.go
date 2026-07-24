// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
)

func TestHandler(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		output, err := handler(context.Background(), Input{WaitMs: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if output.StatusCode != 200 {
			t.Errorf("expected StatusCode=200, got %d", output.StatusCode)
		}
		if output.Body == "" {
			t.Error("expected non-empty body")
		}
	})

	t.Run("failure", func(t *testing.T) {
		_, err := handler(context.Background(), Input{Failure: true})
		if err == nil {
			t.Fatal("expected error for failure input")
		}
	})
}

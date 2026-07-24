// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
)

func TestHandler(t *testing.T) {
	t.Run("empty callbackId returns error", func(t *testing.T) {
		_, err := handler(context.Background(), Input{
			Action: "success",
		})
		if err == nil {
			t.Fatal("expected error for empty callbackId")
		}
	})

	t.Run("unknown action returns error", func(t *testing.T) {
		_, err := handler(context.Background(), Input{
			CallbackID: "test-callback-id",
			Action:     "invalid",
		})
		if err == nil {
			t.Fatal("expected error for unknown action")
		}
	})
}

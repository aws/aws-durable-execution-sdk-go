// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// TestHandler pauses the execution during reserve-stock, resumes it,
// approves the order, and checks the result.
//
// The test is local only. OmitTokenOnCheckpoint and local callback
// completion have no cloud counterpart, so this example has no cloud run.
func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	event := Event{OrderID: "order-7"}

	// reserve-stock checkpoints twice: its start, then its result.
	// Withholding the token on the second call keeps the result and ends
	// the invocation PENDING.
	runner.OmitTokenOnCheckpoint(2)
	paused, err := runner.RunUntilComplete(event)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Status != durabletest.Pending {
		t.Fatalf("after pause: status = %s, want PENDING", paused.Status)
	}
	if op := paused.Operation("reserve-stock"); op == nil || op.Status != "SUCCEEDED" {
		t.Fatalf("after pause: reserve-stock = %+v, want SUCCEEDED", op)
	}
	if op := paused.Operation("manager-approval"); op != nil {
		t.Fatalf("after pause: manager-approval = %+v, want not started", op)
	}

	resumed, err := runner.RunUntilComplete(event)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != durabletest.Pending {
		t.Fatalf("after resume: status = %s, want PENDING awaiting the callback", resumed.Status)
	}
	open := runner.OpenCallbacks()
	if len(open) != 1 {
		t.Fatalf("open callbacks = %+v, want exactly one", open)
	}
	if err := runner.SendCallbackSuccess(open[0].CallbackID, "approved"); err != nil {
		t.Fatalf("send callback: %v", err)
	}

	done, err := runner.RunUntilComplete(event)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durabletest.Succeeded {
		t.Fatalf("after callback: status = %s, want SUCCEEDED", done.Status)
	}
	out, err := durabletest.ResultAs[Result](done)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	want := Result{Reservation: "reserved-order-7", Approval: "approved", Shipment: "shipped-order-7"}
	if out != want {
		t.Fatalf("result = %+v, want %+v", out, want)
	}

	extest.AssertSignature(t, done, extest.Ordered)
}

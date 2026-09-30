// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	input := Input{FunctionName: "arn:aws:lambda:us-east-1:123456789012:function:target"}

	runner := durabletest.NewLocalRunner(handler)
	res := runner.RunUntilComplete(t, input)

	// Execution suspends because the invoke needs external resolution.
	if res.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", res.Status)
	}
	if err := runner.CompleteChainedInvoke("invoke", json.RawMessage(`{"status":"completed"}`)); err != nil {
		t.Fatalf("complete invoke: %v", err)
	}

	res = runner.RunUntilComplete(t, input)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", res.Status)
	}

	output, err := durabletest.ResultAs[result](res)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	t.Logf("result %+v, target %s", output, output.Target)
	if output.Message != "done" {
		t.Errorf("message = %q, want %q", output.Message, "done")
	}
	if string(output.Target) != `{"status":"completed"}` {
		t.Errorf("target = %s, want %s", output.Target, `{"status":"completed"}`)
	}
	if !output.ResumedInReplay {
		t.Errorf("resumedInReplay = false, want true: the result must come from the resumed invocation")
	}
	if output.Logged != 1 {
		t.Errorf("logged = %d, want 1: the line after the replayed operation runs for the first time", output.Logged)
	}
	if output.ReplayingAtLine {
		t.Errorf("replayingAtLine = true, want false")
	}

	extest.AssertSignature(t, res, extest.Ordered)
}

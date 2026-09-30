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
	// The local runner has no Lambda API, so the step records the callback
	// ID and the test completes the callback once the execution blocks on
	// it.
	var callbackID string
	sendCallback = func(_ context.Context, id, _ string) error {
		callbackID = id
		return nil
	}
	t.Cleanup(func() { sendCallback = completeCallback })

	runner := durabletest.NewLocalRunner(handler)
	res := runner.RunUntilComplete(t, nil)
	if res.Status == durabletest.Pending {
		if err := runner.SendCallbackSuccess(callbackID, "approved"); err != nil {
			t.Fatalf("send callback: %v", err)
		}
		res = runner.RunUntilComplete(t, nil)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", res.Status)
	}

	output, err := durabletest.ResultAs[result](res)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	t.Logf("result %+v", output)
	if !output.ResumedInReplay {
		t.Errorf("resumedInReplay = false, want true: the result must come from a resumed invocation")
	}
	if output.LoggedAfterWait != 1 || output.LoggedAfterCallback != 1 {
		t.Errorf("logged after-wait %d, after-callback %d, want 1 and 1", output.LoggedAfterWait, output.LoggedAfterCallback)
	}
	if output.ReplayingAtLine {
		t.Errorf("replayingAtLine = true, want false")
	}

	extest.AssertSignature(t, res, extest.Ordered)
}

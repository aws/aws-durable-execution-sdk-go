// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// recordedTrace returns the stack trace checkpointed with the step's
// failure, as read from the StepFailed event.
func recordedTrace(t *testing.T, result *durabletest.TestResult) []string {
	t.Helper()
	for _, ev := range result.Events {
		if ev.EventType != types.EventTypeStepFailed || ev.StepFailedDetails == nil {
			continue
		}
		if ev.StepFailedDetails.Error == nil || ev.StepFailedDetails.Error.Payload == nil {
			t.Fatal("StepFailed event carries no error")
		}
		return ev.StepFailedDetails.Error.Payload.StackTrace
	}
	t.Fatal("no StepFailed event")
	return nil
}

// TestHandler runs the handler as main configures it, with capture off.
func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler, durable.WithStackTraces(false))
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
	want := Output{Message: "charge 4111-xxxx: card declined", StackFrames: 0}
	if out != want {
		t.Errorf("expected %+v, got %+v", want, out)
	}

	// The checkpoint holds the failure without frames.
	if trace := recordedTrace(t, result); len(trace) != 0 {
		t.Errorf("checkpointed trace = %q, want none", trace)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerDefaultCapture runs the same handler without the option, so
// the SDK captures the trace it records by default.
func TestHandlerDefaultCapture(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
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
	if out.StackFrames == 0 || out.StackFrames > durable.MaxStackTraceFrames {
		t.Errorf("StepError carries %d frames, want 1 to %d", out.StackFrames, durable.MaxStackTraceFrames)
	}

	trace := recordedTrace(t, result)
	if len(trace) != out.StackFrames {
		t.Errorf("checkpointed trace has %d frames, the StepError %d", len(trace), out.StackFrames)
	}
	// The trace is taken when the step body returns its error, so the
	// first frame is that body, the first function literal in handler,
	// with the file and line where it is declared. The package part of the
	// name depends on the build (main in the deployed binary), so only the
	// rest is checked.
	if len(trace) == 0 || !strings.Contains(trace[0], ".handler.func1 ") ||
		!strings.Contains(trace[0], "error-stack-traces/main.go:") {
		t.Errorf("first frame = %q, want the step body in handler", trace)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

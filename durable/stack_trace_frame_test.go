package durable_test

// A recorded stack-trace frame reads "function base.go:line". The file
// component is the base file name with no directory, so a frame never
// carries a path from the machine that built the binary.

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// failingStep is a named step body so the first frame names a user function.
func failingStep(_ durable.StepContext) (string, error) {
	return "", errors.New("boom from user code")
}

// frameFileComponent returns the file part of a "function file:line" frame:
// the text after the last space and before the last colon.
func frameFileComponent(frame string) string {
	sp := strings.LastIndex(frame, " ")
	if sp < 0 {
		return frame
	}
	fileLine := frame[sp+1:]
	colon := strings.LastIndex(fileLine, ":")
	if colon < 0 {
		return fileLine
	}
	return fileLine[:colon]
}

// assertNoDirectoryInFrames fails t for every frame whose file component
// contains a path separator.
func assertNoDirectoryInFrames(t *testing.T, trace []string) {
	t.Helper()
	for i, frame := range trace {
		file := frameFileComponent(frame)
		if strings.ContainsAny(file, "/\\") {
			t.Errorf("frame[%d] file component contains a directory separator: %q (file=%q)", i, frame, file)
		}
	}
}

// captureStepFailureTrace runs a handler whose step fails once and returns
// the StackTrace on the *StepError the handler received.
func captureStepFailureTrace(t *testing.T) []string {
	t.Helper()
	var captured []string
	h := func(ctx durable.Context, _ any) (string, error) {
		_, err := durable.Step(ctx, "failing", failingStep, durable.WithRetry(durable.NoRetry()))
		var se *durable.StepError
		if errors.As(err, &se) {
			captured = se.StackTrace
		}
		return "ok", nil
	}
	res, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	return captured
}

func TestStackTraceFramesHaveNoDirectory(t *testing.T) {
	captured := captureStepFailureTrace(t)
	if len(captured) == 0 {
		t.Fatalf("expected a non-empty stack trace by default")
	}
	t.Logf("recorded frame[0] = %q", captured[0])
	assertNoDirectoryInFrames(t, captured)
}

// TestStackTracesOnByDefault guards the invariant that capture stays on by
// default: the trace is non-empty and its first frame names the user step
// body.
func TestStackTracesOnByDefault(t *testing.T) {
	captured := captureStepFailureTrace(t)
	if len(captured) == 0 {
		t.Fatalf("expected a non-empty trace by default")
	}
	if !strings.Contains(captured[0], "failingStep") {
		t.Errorf("first frame should name the user step body failingStep: %q", captured[0])
	}
}

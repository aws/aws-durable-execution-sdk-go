// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// testArchive fails each Put with the next error in fail, then accepts
// every later one. It records the keys of the records it accepted.
type testArchive struct {
	mu       sync.Mutex
	fail     []error
	puts     int
	accepted []string
}

func (a *testArchive) Put(_ context.Context, key string, _ []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.puts++
	if len(a.fail) > 0 {
		err := a.fail[0]
		a.fail = a.fail[1:]
		return err
	}
	a.accepted = append(a.accepted, key)
	return nil
}

// useArchive replaces store for the duration of the test.
func useArchive(t *testing.T, a archive) {
	t.Helper()
	prev := store
	store = a
	t.Cleanup(func() { store = prev })
}

// stepStarts counts the StepStarted events recorded for the step name:
// one per run of its body that the SDK checkpointed a start for.
func stepStarts(result *durabletest.TestResult, name string) int {
	n := 0
	for _, ev := range result.Events {
		if ev.EventType == types.EventTypeStepStarted && aws.ToString(ev.Name) == name {
			n++
		}
	}
	return n
}

var input = Input{OrderID: "ORD-12345"}

func TestHandler(t *testing.T) {
	a := &testArchive{}
	useArchive(t, a)

	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Receipt](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := Receipt{OrderID: "ORD-12345", Status: "charged after reserved ORD-12345"}
	if out != want {
		t.Errorf("expected %+v, got %+v", want, out)
	}
	if len(a.accepted) != 1 {
		t.Errorf("expected 1 archived record, got %d", len(a.accepted))
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerArchiveUnavailable fails the first archive write with a
// transient error. The invocation ends with the RetryableSerdesError and
// records no outcome for "charge". The test then invokes the execution
// again, as the service would, and it completes from its last checkpoint.
func TestHandlerArchiveUnavailable(t *testing.T) {
	a := &testArchive{fail: []error{errArchiveUnavailable}}
	useArchive(t, a)

	runner := durabletest.NewLocalRunner(handler)

	// The local runner reports an invocation that ends with an error, as
	// the service would see it, as a runner error.
	_, err := runner.RunUntilComplete(input)
	if err == nil {
		t.Fatal("expected the first invocation to end with an error")
	}
	if !errors.Is(err, durable.ErrRetryableSerdes) || !errors.Is(err, errArchiveUnavailable) {
		t.Fatalf("expected a retryable serdes error wrapping errArchiveUnavailable, got %v", err)
	}

	// The archive is available again. The next invocation replays
	// "reserve" from its checkpoint and runs "charge" again.
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	if a.puts != 2 || len(a.accepted) != 1 {
		t.Errorf("expected 2 archive writes and 1 archived record, got %d writes and %d records", a.puts, len(a.accepted))
	}
	if got := stepStarts(result, "reserve"); got != 1 {
		t.Errorf("reserve started %d times, want 1: it was checkpointed before the failure", got)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerArchiveRejected fails the archive write with a permanent
// error. archiveSerdes returns it unchanged, so the step fails after one
// attempt with a SerdesError, which is never retried, and the execution
// fails.
func TestHandlerArchiveRejected(t *testing.T) {
	a := &testArchive{fail: []error{errArchiveRejected}}
	useArchive(t, a)

	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil || result.Error.Type != "StepError" ||
		!strings.Contains(result.Error.Message, "SerdesError") ||
		!strings.Contains(result.Error.Message, errArchiveRejected.Error()) {
		t.Errorf("expected a StepError carrying the SerdesError for the rejection, got %+v", result.Error)
	}
	if a.puts != 1 {
		t.Errorf("expected 1 archive write, got %d: a permanent failure is not retried", a.puts)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.rejected.golden")
}

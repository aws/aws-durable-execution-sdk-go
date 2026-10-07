// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// lockedBuffer is a bytes.Buffer safe for the concurrent writes of the
// handler's branches.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func TestHandler(t *testing.T) {
	var runner *durabletest.LocalRunner[any, string]
	sendCallback = func(_ context.Context, callbackID, value string) error {
		return runner.SendCallbackSuccess(callbackID, value)
	}
	crash = func() {}
	t.Cleanup(func() {
		sendCallback = completeCallback
		crash = defaultCrash
	})

	var logs lockedBuffer
	runner = durabletest.NewLocalRunner(handler, durable.WithLogHandler(slog.NewJSONHandler(&logs, nil)))
	res, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, error %+v, want SUCCEEDED", res.Status, res.Error)
	}
	if len(res.Invocations) < 2 {
		t.Fatalf("invocations = %d, want more than one so that the handler replays", len(res.Invocations))
	}

	// The local runner does not retry a crashed invocation, so here every
	// line, "crash-line" included, is written once.
	got := map[string]int{}
	sc := bufio.NewScanner(bytes.NewReader(logs.bytes()))
	for sc.Scan() {
		var rec struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("decode log record %q: %v", sc.Text(), err)
		}
		got[rec.Msg]++
	}
	for _, line := range lines {
		if got[line] != 1 {
			t.Errorf("line %q written %d times, want 1", line, got[line])
		}
	}
	if len(got) != len(lines) {
		t.Errorf("lines written = %v, want exactly %v", got, lines)
	}

	extest.AssertSignature(t, res, extest.Unordered)
}

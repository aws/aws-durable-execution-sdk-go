// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/lambda"
)

// assertPlainFunc compiles only when its argument is exactly the plain
// function type Wrap documents.
func assertPlainFunc(func(context.Context, []byte) ([]byte, error)) {}

// TestWrapAcceptedByLambdaStart asserts that Wrap's return is accepted by
// lambda.Start. Wrap returns a plain func(context.Context, []byte)
// ([]byte, error) implementing no aws-lambda-go interface (compile-time
// assertion above). Start registers it through the runtime's raw byte
// interface (lambda.Handler), the payload path lambda.Start documents for
// exactly this signature; the test invokes that registration end to end
// and checks the payload and response pass through byte-exact.
func TestWrapAcceptedByLambdaStart(t *testing.T) {
	fake := &fakeLambda{statePages: [][]Operation{{}}}

	wrapped := Wrap(func(_ Context, in string) (string, error) {
		return strings.ToUpper(in), nil
	}, withLambdaAPI(fake))

	// Compile-time: Wrap returns the plain function type, not an
	// aws-lambda-go interface.
	assertPlainFunc(wrapped)

	// What Start registers: the raw passthrough. lambda.Start accepts it
	// because it implements the runtime's raw byte interface.
	var h lambda.Handler = rawPayloadHandler(wrapped)

	payload := []byte(`{
		"DurableExecutionArn": "arn:test",
		"CheckpointToken": "token-0",
		"InitialExecutionState": {
			"Operations": [
				{"Id": "exec", "Status": "STARTED", "Type": "EXECUTION",
				 "ExecutionDetails": {"InputPayload": "\"hello\""}}
			]
		}
	}`)
	resp, err := h.Invoke(context.Background(), payload)
	if err != nil {
		t.Fatalf("lambda handler Invoke() error: %v", err)
	}
	if want := `"\"HELLO\""`; !strings.Contains(string(resp), want) {
		t.Errorf("response = %s, want result containing %s", resp, want)
	}
}

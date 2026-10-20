// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%v)", result.Status, result.Error)
	}
	// The wait suspends the first invocation; the second replays both
	// child contexts, which fails if a recorded subtype differs.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if want := (Output{Order: "order confirmed", Audit: "audit recorded"}); out != want {
		t.Errorf("result = %+v, want %+v", out, want)
	}

	// The order child records the custom subtype; the audit child keeps
	// the default.
	for name, want := range map[string]string{
		"order-saga": orderSagaSubType,
		"audit":      durable.OperationSubTypeRunInChildContext,
	} {
		op := result.Operation(name)
		if op == nil {
			t.Fatalf("%s context operation not found", name)
		}
		if op.SubType != want {
			t.Errorf("%s subtype = %q, want %q", name, op.SubType, want)
		}
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

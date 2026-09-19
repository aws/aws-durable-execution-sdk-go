// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !out.IsDeterministic {
		t.Error("expected IsDeterministic=true")
	}
	if out.ErrorPropsBeforeReplay != out.ErrorPropsAfterReplay {
		t.Errorf("error properties mismatch: before=%+v after=%+v",
			out.ErrorPropsBeforeReplay, out.ErrorPropsAfterReplay)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// run drives handler to completion with opts and returns the result and
// the operations the plugin reported.
func run(t *testing.T, opts ...durable.HandlerOption) (*durabletest.TestResult, []reportedOp) {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler, opts...)
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
	return result, out.Reported
}

// checkpointedSteps returns the names of the steps the execution
// checkpointed, whether or not a plugin was told about them.
func checkpointedSteps(result *durabletest.TestResult) []string {
	var names []string
	for _, op := range result.OperationsByType("STEP") {
		names = append(names, op.Name)
	}
	return names
}

func TestHandler(t *testing.T) {
	result, reported := run(t, options...)

	// Depth 0: only the operations claimed on the handler's Context, each
	// with ChildrenOmitted set because it lies at the bound. The steps
	// inside the child context are withheld.
	want := []reportedOp{
		{Name: "load-order", ChildrenOmitted: true},
		{Name: "fulfil", ChildrenOmitted: true},
		{Name: "notify", ChildrenOmitted: true},
	}
	if !slices.Equal(reported, want) {
		t.Errorf("reported operations = %+v, want %+v", reported, want)
	}

	// Omission affects notifications only: the withheld steps still ran
	// and were checkpointed.
	steps := checkpointedSteps(result)
	if !slices.Equal(steps, []string{"load-order", "reserve-stock", "charge-card", "notify"}) {
		t.Errorf("checkpointed steps = %v", steps)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerEveryDepth registers the same plugin without a depth bound,
// the default, so the steps inside the child context are reported too and
// nothing carries ChildrenOmitted. The operations are the same, so the
// default golden applies.
func TestHandlerEveryDepth(t *testing.T) {
	result, reported := run(t, durable.WithPlugins(plugin))

	want := []reportedOp{
		{Name: "load-order"},
		{Name: "reserve-stock"},
		{Name: "charge-card"},
		{Name: "fulfil"},
		{Name: "notify"},
	}
	if !slices.Equal(reported, want) {
		t.Errorf("reported operations = %+v, want %+v", reported, want)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

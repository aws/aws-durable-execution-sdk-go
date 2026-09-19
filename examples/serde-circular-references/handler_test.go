// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%v)", result.Status, result.Error)
	}
	// The wait suspends the first invocation; the second replays both
	// build steps from their checkpoints.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	out, err := durabletest.ResultAs[output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	wantDefault := defaultSerdesOutcome{
		Failed: true, IsStepError: true, IsSerdesError: true,
		Operation: "build-graph-default", ReportsCycle: true,
	}
	if out.DefaultSerdes != wantDefault {
		t.Errorf("defaultSerdes = %+v, want %+v", out.DefaultSerdes, wantDefault)
	}
	wantCustom := customSerdesOutcome{RootID: "order-1", ItemIDs: []string{"line-1", "line-2"}, ParentsPointToRoot: true}
	if !reflect.DeepEqual(out.CustomSerdes, wantCustom) {
		t.Errorf("customSerdes = %+v, want %+v", out.CustomSerdes, wantCustom)
	}

	// The default-serdes step is recorded as failed with the serdes
	// error, so a replay reproduces the same failure without re-running
	// the step.
	failed := result.Operation("build-graph-default")
	if failed == nil || failed.StepDetails == nil {
		t.Fatal("build-graph-default step not found")
	}
	if failed.Status != "FAILED" {
		t.Errorf("build-graph-default status = %s, want FAILED", failed.Status)
	}
	if failed.StepDetails.ErrorType != "SerdesError" {
		t.Errorf("build-graph-default ErrorType = %q, want SerdesError", failed.StepDetails.ErrorType)
	}
	if !strings.Contains(failed.StepDetails.ErrorMessage, "cycle") {
		t.Errorf("build-graph-default ErrorMessage = %q, want a cycle report", failed.StepDetails.ErrorMessage)
	}

	// The custom serdes checkpoints the graph as a flat node list whose
	// references are IDs, so the payload itself holds no cycle.
	flattened := result.Operation("build-graph-flattened")
	if flattened == nil || flattened.StepDetails == nil {
		t.Fatal("build-graph-flattened step not found")
	}
	var wire flatGraph
	if err := json.Unmarshal([]byte(flattened.StepDetails.Result), &wire); err != nil {
		t.Fatalf("decode flattened payload %q: %v", flattened.StepDetails.Result, err)
	}
	wantWire := flatGraph{Root: "order-1", Nodes: []flatNode{
		{ID: "order-1", Items: []string{"line-1", "line-2"}},
		{ID: "line-1", Parent: "order-1", Items: []string{}},
		{ID: "line-2", Parent: "order-1", Items: []string{}},
	}}
	if !reflect.DeepEqual(wire, wantWire) {
		t.Errorf("flattened payload = %+v, want %+v", wire, wantWire)
	}

	// Every operation runs sequentially on the handler goroutine, so the
	// sequence is deterministic.
	extest.AssertSignature(t, result, extest.Ordered)
}

func TestGraphSerdesRoundTrip(t *testing.T) {
	root := buildGraph()
	rebuilt, err := rebuildGraph(flattenGraph(root))
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.ID != root.ID || len(rebuilt.Items) != len(root.Items) {
		t.Fatalf("rebuilt = %+v, want the shape of %+v", rebuilt, root)
	}
	for i, item := range rebuilt.Items {
		if item.ID != root.Items[i].ID {
			t.Errorf("item %d ID = %q, want %q", i, item.ID, root.Items[i].ID)
		}
		if item.Parent != rebuilt {
			t.Errorf("item %d Parent does not point at the rebuilt root", i)
		}
	}
}

func TestRebuildGraphRejectsDanglingReference(t *testing.T) {
	_, err := rebuildGraph(flatGraph{Root: "a", Nodes: []flatNode{{ID: "a", Items: []string{"missing"}}}})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("err = %v, want an unknown-item error", err)
	}
	_, err = rebuildGraph(flatGraph{Root: "gone", Nodes: []flatNode{{ID: "a"}}})
	if err == nil || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("err = %v, want an unknown-root error", err)
	}
}

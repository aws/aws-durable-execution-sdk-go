package dcontext

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func newTestRoot(prefix string) *Context {
	c := NewRoot(context.Background(), "arn:test", nil, nil, types.LoggerConfig{}, false, prefix, nil)
	return c
}

func TestTaskEntityID_PrefixedAndUnprefixed(t *testing.T) {
	if got := TaskEntityID("", "fetch"); got != "DAG_NODE_T_fetch" {
		t.Fatalf("unprefixed: got %q", got)
	}
	if got := TaskEntityID("abc123", "fetch"); got != "abc123-DAG_NODE_T_fetch" {
		t.Fatalf("prefixed: got %q", got)
	}
}

func TestTaskEntityID_NestedRecursion(t *testing.T) {
	a := TaskEntityID("", "a")               // DAG_NODE_T_a
	ab := TaskEntityID(a, "b")               // DAG_NODE_T_a-DAG_NODE_T_b
	want := "DAG_NODE_T_a-DAG_NODE_T_b"
	if ab != want {
		t.Fatalf("nested: got %q want %q", ab, want)
	}
	abc := TaskEntityID(ab, "c")
	if abc != "DAG_NODE_T_a-DAG_NODE_T_b-DAG_NODE_T_c" {
		t.Fatalf("deep nested: got %q", abc)
	}
}

func TestHashOperationID_Deterministic(t *testing.T) {
	raw := TaskEntityID("root", "task")
	h1 := HashOperationID(raw)
	h2 := HashOperationID(raw)
	if h1 != h2 {
		t.Fatalf("hash not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("expected 64-hex SHA-256, got len %d (%q)", len(h1), h1)
	}
	if HashOperationID(TaskEntityID("root", "other")) == h1 {
		t.Fatalf("distinct names hashed to same ID")
	}
}

func TestNewNamedChild_DeterministicAndOrderIndependent(t *testing.T) {
	// Two independent root contexts (simulating two replays of the same
	// handler) must derive identical child prefixes and nested IDs for
	// the same task name, regardless of any unrelated counter activity.
	rootA := newTestRoot("EXEC")
	rootB := newTestRoot("EXEC")

	// Advance rootB's positional counter to prove name-based IDs do not
	// depend on the counter state.
	_ = rootB.NextStepID()
	_ = rootB.NextStepID()

	idA := HashOperationID(TaskEntityID(rootA.Prefix(), "fetch"))
	idB := HashOperationID(TaskEntityID(rootB.Prefix(), "fetch"))
	if idA != idB {
		t.Fatalf("name-based IDs differ across contexts: %q vs %q", idA, idB)
	}

	childA := rootA.NewNamedChild(idA, "fetch")
	childB := rootB.NewNamedChild(idB, "fetch")

	// Nested operation IDs under the named child must also be identical
	// and stable (same first nested step ID).
	nestedA := childA.PeekStepID()
	nestedB := childB.PeekStepID()
	if nestedA != nestedB {
		t.Fatalf("nested IDs under named child differ: %q vs %q", nestedA, nestedB)
	}

	// The named child's prefix must be the hashed entity ID (so its
	// nested ops derive under the name-based namespace).
	if childA.Prefix() != idA {
		t.Fatalf("named child prefix = %q, want hashed entity id %q", childA.Prefix(), idA)
	}
}

func TestNewNamedChild_DisjointFromPositionalCounter(t *testing.T) {
	root := newTestRoot("EXEC")
	positional := root.NextStepID() // a normal positional op ID
	named := HashOperationID(TaskEntityID(root.Prefix(), "fetch"))
	if positional == named {
		t.Fatalf("name-based ID collided with positional counter ID")
	}
}

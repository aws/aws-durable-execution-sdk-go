package durable

import "testing"

func TestOpIDsSequence(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{
			name: "root context mints bare positional IDs",
			want: []string{"1", "2", "3"},
		},
		{
			name:   "child context prefixes with its own entity ID",
			prefix: "7",
			want:   []string{"7-1", "7-2", "7-3"},
		},
		{
			name:   "nested child extends the prefix chain",
			prefix: "7-2",
			want:   []string{"7-2-1", "7-2-2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := &opIDs{prefix: tt.prefix}
			for i, want := range tt.want {
				if got := ids.peek(); got != want {
					t.Errorf("peek() before claim %d = %q, want %q", i+1, got, want)
				}
				if got := ids.next(); got != want {
					t.Errorf("next() claim %d = %q, want %q", i+1, got, want)
				}
			}
		})
	}
}

func TestOpIDsPeekDoesNotClaim(t *testing.T) {
	ids := &opIDs{}
	for range 3 {
		if got := ids.peek(); got != "1" {
			t.Fatalf("peek() = %q, want %q", got, "1")
		}
	}
	if got := ids.next(); got != "1" {
		t.Errorf("next() after repeated peek = %q, want %q", got, "1")
	}
}

func TestOpIDsChild(t *testing.T) {
	root := &opIDs{}
	childEntityID := root.next() // "1"
	child := root.child(childEntityID)

	if got := child.next(); got != "1-1" {
		t.Errorf("child next() = %q, want %q", got, "1-1")
	}
	// The child minter must not disturb the parent's sequence.
	if got := root.next(); got != "2" {
		t.Errorf("root next() after child mint = %q, want %q", got, "2")
	}

	grandchild := child.child("1-2")
	if got := grandchild.next(); got != "1-2-1" {
		t.Errorf("grandchild next() = %q, want %q", got, "1-2-1")
	}
}

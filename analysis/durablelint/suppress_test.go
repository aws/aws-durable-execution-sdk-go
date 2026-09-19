package durablelint

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestSuppress(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), GoroutineAnalyzer, "suppress")
}

func TestParseRules(t *testing.T) {
	tests := []struct {
		name string
		rest string
		want ruleSet
	}{
		{"empty", "", nil},
		{"spaces only", "   ", nil},
		{"reason only", " -- because", nil},
		{"one rule", " durablegoroutine", ruleSet{"durablegoroutine": true}},
		{"two rules", " a, b", ruleSet{"a": true, "b": true}},
		{"rules and reason", " a,b -- why", ruleSet{"a": true, "b": true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRules(tt.rest)
			if len(got) != len(tt.want) {
				t.Fatalf("parseRules(%q) = %v, want %v", tt.rest, got, tt.want)
			}
			for k := range tt.want {
				if !got[k] {
					t.Fatalf("parseRules(%q) = %v, want %v", tt.rest, got, tt.want)
				}
			}
		})
	}
}

func TestDirective(t *testing.T) {
	tests := []struct {
		text, keyword, rest string
		ok                  bool
	}{
		{"//durable:ignore", "//durable:ignore", "", true},
		{"//durable:ignore a", "//durable:ignore", " a", true},
		{"//durable:ignore-file", "//durable:ignore", "", false},
		{"//durable:ignored", "//durable:ignore", "", false},
		{"// durable:ignore", "//durable:ignore", "", false},
	}
	for _, tt := range tests {
		rest, ok := directive(tt.text, tt.keyword)
		if ok != tt.ok || rest != tt.rest {
			t.Errorf("directive(%q, %q) = (%q, %v), want (%q, %v)", tt.text, tt.keyword, rest, ok, tt.rest, tt.ok)
		}
	}
}

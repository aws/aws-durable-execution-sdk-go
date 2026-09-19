// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// parseSource writes src as a handler test and parses it.
func parseSource(t *testing.T, src string) (*HandlerTest, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "handler_test.go")
	header := "package main\n\nimport (\n\t\"testing\"\n\n\t\"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest\"\n)\n\n"
	if err := os.WriteFile(path, []byte(header+src), 0o644); err != nil {
		t.Fatal(err)
	}
	return ParseHandlerTest(path)
}

func TestParseHandlerTestDeclarations(t *testing.T) {
	ht, err := parseSource(t, `
func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, nil)
	if runner.Cloud() {
		extest.AssertSignatureFile(t, result, extest.Ordered, extest.CloudGoldenPath)
		return
	}
	extest.AssertSignature(t, result, extest.Ordered)

	t.Run("failure", func(t *testing.T) {
		result := runner.RunUntilComplete(t, 1)
		extest.AssertSignatureFile(t, result, extest.Unordered, "testdata/signature.failure.golden")
	})
}
`)
	if err != nil {
		t.Fatal(err)
	}
	want := []Declaration{
		{CloudGoldenPath, Ordered},
		{GoldenPath, Ordered},
		{"testdata/signature.failure.golden", Unordered},
	}
	if !reflect.DeepEqual(ht.Declarations, want) {
		t.Errorf("Declarations = %+v, want %+v", ht.Declarations, want)
	}
	if mode, err := ht.ModeFor(GoldenPath); err != nil || mode != Ordered {
		t.Errorf("ModeFor(GoldenPath) = %s, %v; want Ordered", mode, err)
	}
	if _, err := ht.ModeFor("testdata/signature.other.golden"); err == nil {
		t.Error("ModeFor on an unasserted golden succeeded")
	}
	if got := ht.Unasserted(); len(got) != 0 {
		t.Errorf("Unasserted = %v, want none", got)
	}
}

func TestParseHandlerTestUnasserted(t *testing.T) {
	ht, err := parseSource(t, `
func run(t *testing.T, event int) *durabletest.TestResult {
	return durabletest.NewLocalRunner(handler).RunUntilComplete(t, event)
}

func runAndAssert(t *testing.T, event int) {
	extest.AssertSignature(t, run(t, event), extest.Ordered)
}

func TestAsserted(t *testing.T) {
	extest.AssertSignature(t, run(t, 1), extest.Ordered)
}

func TestAssertedThroughHelper(t *testing.T) {
	runAndAssert(t, 1)
}

func TestDirectGap(t *testing.T) {
	durabletest.NewLocalRunner(handler).Run(t, 1)
}

func TestHelperGap(t *testing.T) {
	run(t, 1)
}

func TestSubtests(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		extest.AssertSignature(t, run(t, 1), extest.Ordered)
	})
	t.Run("gap", func(t *testing.T) {
		run(t, 2)
	})
	for _, n := range []string{"a"} {
		t.Run(n, func(t *testing.T) {
			run(t, 3)
		})
	}
}
`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TestDirectGap", "TestHelperGap", "TestSubtests/gap", "TestSubtests/<dynamic>"}
	if got := ht.Unasserted(); !reflect.DeepEqual(got, want) {
		t.Errorf("Unasserted = %v, want %v", got, want)
	}
}

// TestParseHandlerTestNestedHelpers covers a handler run reached through
// a chain of helpers, none of which asserts: the Test function at the
// top of the chain is the one required to assert.
func TestParseHandlerTestNestedHelpers(t *testing.T) {
	ht, err := parseSource(t, `
func runC(t *testing.T, event int) *durabletest.TestResult {
	return durabletest.NewLocalRunner(handler).RunUntilComplete(t, event)
}

func runB(t *testing.T, event int) *durabletest.TestResult {
	return runC(t, event)
}

func runA(t *testing.T, event int) *durabletest.TestResult {
	return runB(t, event)
}

func runAndAssert(t *testing.T, event int) {
	extest.AssertSignature(t, runA(t, event), extest.Ordered)
}

func TestTwoLevels(t *testing.T) {
	runB(t, 1)
}

func TestThreeLevels(t *testing.T) {
	runA(t, 1)
}

func TestAssertedAtTop(t *testing.T) {
	extest.AssertSignature(t, runA(t, 1), extest.Ordered)
}

func TestAssertedInHelper(t *testing.T) {
	runAndAssert(t, 1)
}

func TestSubtestThroughChain(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		runA(t, 1)
	})
}
`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TestTwoLevels", "TestThreeLevels", "TestSubtestThroughChain/gap"}
	if got := ht.Unasserted(); !reflect.DeepEqual(got, want) {
		t.Errorf("Unasserted = %v, want %v", got, want)
	}
}

// TestParseHandlerTestSubtestParameterNames covers subtests whose
// *testing.T parameter is not named t, and Test functions whose
// parameter is not named t. Each subtest is a scope of its own, and its
// Run call is not a handler run.
func TestParseHandlerTestSubtestParameterNames(t *testing.T) {
	ht, err := parseSource(t, `
func run(t *testing.T, event int) *durabletest.TestResult {
	return durabletest.NewLocalRunner(handler).RunUntilComplete(t, event)
}

func TestRenamed(tc *testing.T) {
	tc.Run("ok", func(subT *testing.T) {
		extest.AssertSignature(subT, run(subT, 1), extest.Ordered)
	})
	tc.Run("gap", func(subT *testing.T) {
		run(subT, 2)
	})
	tc.Run("nested", func(outer *testing.T) {
		outer.Run("ok", func(inner *testing.T) {
			extest.AssertSignature(inner, run(inner, 3), extest.Ordered)
		})
		outer.Run("gap", func(inner *testing.T) {
			run(inner, 4)
		})
		outer.Run("outer-t", func(_ *testing.T) {
			tc.Run("via-enclosing", func(t *testing.T) {
				run(t, 5)
			})
		})
	})
}

func TestNoParameterName(t *testing.T) {
	t.Run("gap", func(*testing.T) {
		run(t, 1)
	})
}
`)
	if err != nil {
		t.Fatal(err)
	}
	wantUnasserted := []string{
		"TestRenamed/gap",
		"TestRenamed/nested/gap",
		"TestRenamed/nested/outer-t/via-enclosing",
		"TestNoParameterName/gap",
	}
	if got := ht.Unasserted(); !reflect.DeepEqual(got, wantUnasserted) {
		t.Errorf("Unasserted = %v, want %v", got, wantUnasserted)
	}
	var names []string
	for _, s := range ht.Scopes {
		names = append(names, s.Name)
	}
	wantScopes := []string{
		"run",
		"TestRenamed",
		"TestRenamed/ok",
		"TestRenamed/gap",
		"TestRenamed/nested",
		"TestRenamed/nested/ok",
		"TestRenamed/nested/gap",
		"TestRenamed/nested/outer-t",
		"TestRenamed/nested/outer-t/via-enclosing",
		"TestNoParameterName",
		"TestNoParameterName/gap",
	}
	if !reflect.DeepEqual(names, wantScopes) {
		t.Errorf("scopes = %v, want %v", names, wantScopes)
	}
	for _, s := range ht.Scopes {
		if s.Asserts == 0 && s.Runs > 0 && !strings.HasSuffix(s.Name, "gap") && !strings.HasSuffix(s.Name, "via-enclosing") && s.Name != "run" {
			t.Errorf("scope %s counts %d handler runs; a subtest Run call was counted as a handler run", s.Name, s.Runs)
		}
	}
}

func TestParseHandlerTestRejectsIndirectArguments(t *testing.T) {
	cases := map[string]string{
		"mode variable": `
func TestHandler(t *testing.T) {
	mode := extest.Ordered
	extest.AssertSignature(t, nil, mode)
}
`,
		"golden variable": `
func TestHandler(t *testing.T) {
	path := "testdata/signature.golden"
	extest.AssertSignatureFile(t, nil, extest.Ordered, path)
}
`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseSource(t, src)
			if err == nil || !strings.Contains(err.Error(), "must be") {
				t.Fatalf("err = %v, want a rejected argument", err)
			}
		})
	}
}

func TestParseHandlerTestConflictingModes(t *testing.T) {
	ht, err := parseSource(t, `
func TestA(t *testing.T) { extest.AssertSignature(t, nil, extest.Ordered) }
func TestB(t *testing.T) { extest.AssertSignature(t, nil, extest.Subset) }
`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ht.ModeFor(GoldenPath); err == nil || !strings.Contains(err.Error(), "more than one mode") {
		t.Fatalf("ModeFor = %v, want a conflict error", err)
	}
}

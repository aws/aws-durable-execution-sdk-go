package durable_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// unkeyedLiteralFixture is a program in a separate module that writes
// unkeyed literals of guarded structs. The literals supply a value for
// every field, including the blank guard, so the only possible diagnostic
// is the one the guard exists to produce. Each line that must fail to
// compile carries a "// want" comment holding a regexp the compiler
// diagnostic for that line must match. Lines without a want comment must
// compile.
const unkeyedLiteralFixture = `package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func main() {
	_ = durable.Branch[string]{[0]func(){}, "name", nil}       // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.RetryConfig{[0]func(){}, 3, 0, 0, 0, "", nil}       // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.LinearRetryConfig{[0]func(){}, 6, 0, 0, 0, "", nil} // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.RetryAttempt{[0]func(){}, nil, 1, 0}                // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.WaitConfig[int]{[0]func(){}, 60, 0, 0, 0, "", nil}  // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.SerdesConfig{[0]func(){}, nil, nil}                   // want "implicit assignment to unexported field _ in struct literal"
	_ = durable.Branch[string]{Name: "name"}
	_ = durable.RetryConfig{MaxAttempts: 3}
	_ = durable.LinearRetryConfig{MaxAttempts: 6}
	_ = durable.RetryAttempt{Attempt: 1}
	_ = durable.WaitConfig[int]{MaxAttempts: 60}
	_ = durable.SerdesConfig{Serdes: durable.JSONSerdes}
}
`

var wantComment = regexp.MustCompile(`//\s*want\s+"([^"]*)"`)

// TestUnkeyedLiteralFailsToCompile proves the leading blank [0]func() field
// blocks unkeyed literals from outside the package. It compiles the fixture
// above with the real toolchain and matches every diagnostic against the
// fixture's want comments, so removing the guard from a struct makes this
// test fail.
func TestUnkeyedLiteralFailsToCompile(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go toolchain not found on PATH: %v", err)
	}

	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err != nil {
		t.Fatalf("repository root go.mod not found at %s: %v", repoRoot, err)
	}
	goSum, err := os.ReadFile(filepath.Join(repoRoot, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	goMod := "module example.com/guardcheck\n\ngo 1.24\n\n" +
		"require github.com/aws/aws-durable-execution-sdk-go v0.0.0\n\n" +
		"replace github.com/aws/aws-durable-execution-sdk-go => " + repoRoot + "\n"
	writeFixtureFile(t, filepath.Join(dir, "go.mod"), []byte(goMod))
	writeFixtureFile(t, filepath.Join(dir, "go.sum"), goSum)
	writeFixtureFile(t, filepath.Join(dir, "main.go"), []byte(unkeyedLiteralFixture))

	cmd := exec.Command(goBin, "build", "-o", os.DevNull, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod", "GOTOOLCHAIN=local")
	out, runErr := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	if runErr == nil || !errors.As(runErr, &exitErr) {
		t.Fatalf("go build on the unkeyed-literal fixture succeeded or did not exit; output:\n%s\nerr: %v", out, runErr)
	}

	wants := parseWants(t, unkeyedLiteralFixture)
	got := parseDiagnosticLines(out)

	for line, pattern := range wants {
		msg, ok := got[line]
		if !ok {
			t.Errorf("fixture line %d: want diagnostic matching %q, got none\noutput:\n%s", line, pattern, out)
			continue
		}
		if !regexp.MustCompile(pattern).MatchString(msg) {
			t.Errorf("fixture line %d: diagnostic %q does not match %q", line, msg, pattern)
		}
	}
	for line, msg := range got {
		if _, ok := wants[line]; !ok {
			t.Errorf("fixture line %d: unexpected diagnostic %q (keyed literal must compile)", line, msg)
		}
	}
}

func writeFixtureFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// parseWants maps 1-based fixture line numbers to the regexp in that line's
// want comment.
func parseWants(t *testing.T, src string) map[int]string {
	t.Helper()
	wants := map[int]string{}
	for i, line := range strings.Split(src, "\n") {
		if m := wantComment.FindStringSubmatch(line); m != nil {
			wants[i+1] = m[1]
		}
	}
	if len(wants) == 0 {
		t.Fatal("fixture has no want comments")
	}
	return wants
}

var diagnosticLine = regexp.MustCompile(`main\.go:(\d+):\d+: (.*)$`)

// parseDiagnosticLines maps 1-based fixture line numbers to the compiler
// messages reported for that line, joined with "; ".
func parseDiagnosticLines(out []byte) map[int]string {
	got := map[int]string{}
	for _, raw := range bytes.Split(out, []byte("\n")) {
		m := diagnosticLine.FindSubmatch(raw)
		if m == nil {
			continue
		}
		line, err := strconv.Atoi(string(m[1]))
		if err != nil {
			continue
		}
		if prev, ok := got[line]; ok {
			got[line] = prev + "; " + string(m[2])
		} else {
			got[line] = string(m[2])
		}
	}
	return got
}

// TestGuardedStructsHaveLeadingBlankField checks that every exported struct
// meant for keyed construction begins with the blank [0]func() field. The
// leading position matters: a trailing zero-size field is padded, a leading
// one is free.
func TestGuardedStructsHaveLeadingBlankField(t *testing.T) {
	types := []reflect.Type{
		reflect.TypeFor[durable.RetryConfig](),
		reflect.TypeFor[durable.LinearRetryConfig](),
		reflect.TypeFor[durable.RetryDecision](),
		reflect.TypeFor[durable.RetryAttempt](),
		reflect.TypeFor[durable.CompletionConfig](),
		reflect.TypeFor[durable.BatchItem[string]](),
		reflect.TypeFor[durable.BatchResult[string]](),
		reflect.TypeFor[durable.Branch[string]](),
		reflect.TypeFor[durable.Settled[string]](),
		reflect.TypeFor[durable.SerdesContext](),
		reflect.TypeFor[durable.ConditionConfig[string]](),
		reflect.TypeFor[durable.WaitDecision](),
		reflect.TypeFor[durable.WaitConfig[string]](),
		reflect.TypeFor[durable.FileSystemSerdesConfig](),
	}
	guard := reflect.TypeFor[[0]func()]()
	for _, typ := range types {
		if typ.NumField() == 0 {
			t.Errorf("%s: no fields", typ)
			continue
		}
		f := typ.Field(0)
		if f.Name != "_" || f.Type != guard {
			t.Errorf("%s: first field is %s %s, want _ [0]func()", typ, f.Name, f.Type)
		}
		if typ.Comparable() {
			t.Errorf("%s: is comparable, want the guard to remove comparability", typ)
		}
	}
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package readme checks the Go code blocks of the repository's README.md.
//
// Every fenced code block whose info string starts with "go" is compiled
// against the SDK in this checkout, and a block that declares a test
// function is also run. So a README snippet that stops compiling fails the
// examples module's test suite.
//
// A block is one of two kinds. A block that contains a package clause is a
// complete file and is compiled as written. Any other block is a set of
// top-level declarations. The test wraps it in a package clause, a fixed
// import set (see [fragmentImports]), and, when the block declares no main
// function, an empty main. A declaration block therefore needs no import
// lines of its own, and must not import anything outside that set.
package readme

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// fragmentImports is the import set given to a declaration block. Each
// import is referenced once so that an unused one does not fail the build.
const fragmentImports = `import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

var (
	_ = context.Background
	_ = errors.New
	_ = fmt.Sprintf
	_ = log.Printf
	_ = slog.Default
	_ = os.Stderr
	_ = strings.ToUpper
	_ = testing.Short
	_ = time.Second
	_ durable.Void
	_ durabletest.TestResult
)
`

// block is one fenced Go code block of the README.
type block struct {
	line int // line of the opening fence, for messages
	code string
}

var (
	fenceOpen = regexp.MustCompile("^```go(\\s|$)")
	pkgClause = regexp.MustCompile(`(?m)^package\s+\w+`)
	mainFunc  = regexp.MustCompile(`(?m)^func main\(\)`)
	testFunc  = regexp.MustCompile(`(?m)^func Test\w*\(t \*testing\.T\)`)
)

// goBlocks returns the Go code blocks of a Markdown document in order.
func goBlocks(doc string) []block {
	var blocks []block
	var cur *block
	var buf strings.Builder
	for i, line := range strings.Split(doc, "\n") {
		switch {
		case cur == nil && fenceOpen.MatchString(line):
			cur = &block{line: i + 1}
			buf.Reset()
		case cur != nil && strings.HasPrefix(line, "```"):
			cur.code = buf.String()
			blocks = append(blocks, *cur)
			cur = nil
		case cur != nil:
			buf.WriteString(line)
			buf.WriteString("\n")
		}
	}
	return blocks
}

// fileFor returns the file name and contents that compile b.
func fileFor(b block) (name, contents string) {
	if pkgClause.MatchString(b.code) {
		return "main.go", b.code
	}
	var sb strings.Builder
	sb.WriteString("package main\n\n")
	sb.WriteString(fragmentImports)
	sb.WriteString("\n")
	sb.WriteString(b.code)
	if testFunc.MatchString(b.code) {
		// The caller writes a main.go beside a test file.
		return "snippet_test.go", sb.String()
	}
	if !mainFunc.MatchString(b.code) {
		sb.WriteString("\nfunc main() {}\n")
	}
	return "main.go", sb.String()
}

func TestREADMEGoBlocksCompile(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	blocks := goBlocks(string(doc))
	if len(blocks) == 0 {
		t.Fatal("README.md has no Go code blocks")
	}

	work := t.TempDir()
	env := append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("go", args...)
		cmd.Dir = work
		cmd.Env = env
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out.String())
		}
	}

	run("mod", "init", "example.com/readme")
	run("mod", "edit",
		"-replace=github.com/aws/aws-durable-execution-sdk-go="+root,
		"-require=github.com/aws/aws-durable-execution-sdk-go@v0.0.0")

	for i, b := range blocks {
		dir := filepath.Join(work, fmt.Sprintf("block%02d_line%d", i+1, b.line))
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		name, contents := fileFor(b)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
		// A test file needs a non-test file beside it for the package clause.
		if name == "snippet_test.go" {
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	// tidy resolves the SDK's dependencies for the throwaway module. vet
	// type-checks every block, test files included, and test runs the
	// blocks that declare tests.
	run("mod", "tidy")
	run("vet", "./...")
	run("test", "./...")
}

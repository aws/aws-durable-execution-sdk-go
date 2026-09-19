// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestEveryExampleAssertsSignature fails when a durable example does not
// assert its operation signature on every run. For each example it
// requires that:
//
//   - the handler test declares one comparison mode for the default
//     golden, so the cloud matrix can compare a deployed run the same way;
//   - every Test function and subtest that runs the handler, directly or
//     through a helper, asserts a signature;
//   - every golden file the test names exists, every golden file under
//     testdata is named by the test or is the cloud golden the cloud
//     matrix asserts, and each holds only operation signatures, so a
//     run-specific value cannot be recorded by mistake.
//
// An example is durable when its main.go starts a durable handler; a
// plain Lambda has no operation log and is not checked.
func TestEveryExampleAssertsSignature(t *testing.T) {
	root := filepath.Join("..", "..")
	mains, err := filepath.Glob(filepath.Join(root, "*", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(mains) == 0 {
		t.Fatalf("no examples found under %s", root)
	}

	var checked []string
	for _, main := range mains {
		dir := filepath.Dir(main)
		name := filepath.Base(dir)
		src, err := os.ReadFile(main)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(src, []byte("durable.Start(")) {
			continue
		}
		checked = append(checked, name)
		checkExample(t, name, dir)
	}
	sort.Strings(checked)
	if len(checked) < 50 {
		t.Fatalf("only %d durable examples found; the glob or the durable.Start marker is wrong", len(checked))
	}
}

func checkExample(t *testing.T, name, dir string) {
	t.Helper()
	ht, err := ParseHandlerTest(filepath.Join(dir, "handler_test.go"))
	if err != nil {
		t.Errorf("%s: %v", name, err)
		return
	}
	if _, err := ht.ModeFor(GoldenPath); err != nil {
		t.Errorf("%s: handler_test.go: %v", name, err)
	}
	if gaps := ht.Unasserted(); len(gaps) > 0 {
		t.Errorf("%s: handler_test.go runs the handler without asserting a signature in %s", name, strings.Join(gaps, ", "))
	}

	// The cloud matrix asserts the cloud golden of every example, so it
	// needs no declaration in the handler test.
	named := map[string]bool{filepath.FromSlash(CloudGoldenPath): true}
	for _, d := range ht.Declarations {
		named[filepath.FromSlash(d.Golden)] = true
		if !strings.HasPrefix(d.Golden, "testdata/signature.") || !strings.HasSuffix(d.Golden, ".golden") {
			t.Errorf("%s: golden %q is not named testdata/signature.<scenario>.golden", name, d.Golden)
		}
	}
	for _, d := range ht.Declarations {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(d.Golden))); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	present, err := filepath.Glob(filepath.Join(dir, "testdata", "signature*.golden"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range present {
		rel, _ := filepath.Rel(dir, path)
		if !named[rel] {
			t.Errorf("%s: %s is not asserted by handler_test.go", name, rel)
		}
		checkGolden(t, name, path)
	}
}

// checkGolden parses a golden file as operation signatures and rejects
// any other field, so a run-specific value cannot be recorded by mistake.
func checkGolden(t *testing.T, name, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("%s: %v", name, err)
		return
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var sigs []durabletest.OperationSignature
	if err := dec.Decode(&sigs); err != nil {
		t.Errorf("%s: %s is not a list of operation signatures: %v", name, filepath.Base(path), err)
		return
	}
	for i, sig := range sigs {
		if sig.Type == "" || sig.Status == "" {
			t.Errorf("%s: %s entry %d lacks a type or status: %+v", name, filepath.Base(path), i, sig)
		}
		for _, v := range []string{sig.Type, sig.SubType, sig.Name, sig.Status} {
			if strings.HasPrefix(v, "arn:") || strings.ContainsAny(v, " \t\n") {
				t.Errorf("%s: %s entry %d holds a value that is not an identifier: %q", name, filepath.Base(path), i, v)
			}
		}
	}
}

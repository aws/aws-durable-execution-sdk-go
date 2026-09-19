// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// GoldenPath is the golden file that holds an example's operation
// signature, relative to the example directory. A Go test binary runs
// with its package directory as the working directory, so every example
// reads the same relative path.
const GoldenPath = "testdata/signature.golden"

// CloudGoldenPath is the golden file for an example whose deployed run
// produces a different operation sequence from its local run. Only such
// an example has one; see [AssertSignatureFile] and [AssertCloudSignature].
const CloudGoldenPath = "testdata/signature.cloud.golden"

// SignatureMode selects how [AssertSignature] compares an execution's
// operation signature with the golden file. Each example records its
// mode in its handler test, next to the reason when the mode is not
// [Ordered]. The zero value is not a mode, so a declaration cannot pick
// one by default.
type SignatureMode int

const (
	unsetMode SignatureMode = iota

	// Ordered compares the full operation sequence position by position.
	// It is the default for examples whose operations run sequentially.
	Ordered

	// Unordered compares the operations as a multiset: the same operations
	// with the same counts, in any order. Use it when concurrent branches
	// (Map, Parallel, Go, the Async operations) checkpoint in an order
	// that depends on scheduling.
	Unordered

	// Subset requires every operation in the golden file to be present
	// and tolerates operations that are not listed. Use it when an
	// operation is optional, such as a fire-and-forget WaitAsync that may
	// or may not reach a checkpoint, or when early completion leaves a
	// scheduling-dependent set of branches unstarted.
	Subset
)

// String returns the mode name used in test output.
func (m SignatureMode) String() string {
	switch m {
	case Ordered:
		return "Ordered"
	case Unordered:
		return "Unordered"
	case Subset:
		return "Subset"
	default:
		return fmt.Sprintf("SignatureMode(%d)", int(m))
	}
}

// AssertSignature compares the operation signature of result with the
// example's golden file at [GoldenPath] using mode. The signature is the
// type, subtype, name, and status of every operation, in checkpoint
// order; see [durabletest.EventSignature]. It contains no run-specific
// values, so the same golden file serves the local runner and the
// deployed function.
//
// Setting UPDATE_GOLDEN=1 writes the current signature to the golden
// file instead of comparing. In [Subset] mode the written file lists
// every operation the run produced; delete the optional ones before
// committing it, because a later run that lacks them would fail.
func AssertSignature(t *testing.T, result *durabletest.TestResult, mode SignatureMode) {
	t.Helper()
	AssertSignatureFile(t, result, mode, GoldenPath)
}

// AssertSignatureFile is [AssertSignature] against the golden file at
// path, relative to the example directory. A scenario whose operations
// differ from the default scenario's keeps its own file next to
// [GoldenPath], named testdata/signature.<scenario>.golden. An example
// whose deployed run differs from its local run by design keeps the
// cloud sequence in [CloudGoldenPath] and selects it by [Runner.Cloud].
// Every file is regenerated the same way, with UPDATE_GOLDEN=1.
func AssertSignatureFile(t *testing.T, result *durabletest.TestResult, mode SignatureMode, path string) {
	t.Helper()
	path = filepath.FromSlash(path)
	actual := durabletest.EventSignature(result)

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		writeGolden(t, path, actual)
		if mode == Subset {
			t.Logf("wrote golden file %s (UPDATE_GOLDEN=1); remove optional operations before committing", path)
		} else {
			t.Logf("wrote golden file %s (UPDATE_GOLDEN=1)", path)
		}
		return
	}

	expected, err := ReadGolden(path)
	if err != nil {
		t.Fatalf("%v\nRun with UPDATE_GOLDEN=1 to create it.", err)
	}
	if err := Compare(mode, expected, actual); err != nil {
		t.Fatalf("operation signature mismatch against %q: %v\n\nExpected (golden, %s):\n%s\n\nActual:\n%s\n\nRegenerate with: UPDATE_GOLDEN=1 go test -run %s .",
			path, err, mode, indent(expected), indent(actual), t.Name())
	}
}

// AssertCloudSignature compares the signature of a deployed run of the
// example in dir with the example's golden files using mode, the mode
// its handler test declares for [GoldenPath]. When the example has a
// file at [CloudGoldenPath], the deployed run must match it; otherwise
// it must match [GoldenPath], the same file the local test asserts.
//
// Setting UPDATE_GOLDEN=1 never touches [GoldenPath], which belongs to
// the local test. It writes [CloudGoldenPath] when the deployed run
// differs from [GoldenPath] under mode, or rewrites it when it exists.
// A cloud golden that the deployed run makes redundant is reported as
// an error, so a difference that no longer exists cannot stay
// documented.
func AssertCloudSignature(t *testing.T, result *durabletest.TestResult, mode SignatureMode, dir string) {
	t.Helper()
	localPath := filepath.Join(dir, filepath.FromSlash(GoldenPath))
	cloudPath := filepath.Join(dir, filepath.FromSlash(CloudGoldenPath))
	actual := durabletest.EventSignature(result)

	local, err := ReadGolden(localPath)
	if err != nil {
		t.Fatalf("%v", err)
	}
	localErr := Compare(mode, local, actual)

	cloud, err := ReadGolden(cloudPath)
	hasCloud := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%v", err)
	}

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		switch {
		case localErr == nil && hasCloud:
			t.Fatalf("the deployed run matches %s; delete %s, it is redundant", localPath, cloudPath)
		case localErr == nil:
			return
		default:
			writeGolden(t, cloudPath, actual)
			t.Logf("wrote cloud golden file %s (UPDATE_GOLDEN=1)", cloudPath)
		}
		return
	}

	if !hasCloud {
		if localErr != nil {
			t.Fatalf("deployed run does not match %q: %v\n\nExpected (golden, %s):\n%s\n\nActual:\n%s\n\nIf the deployed run differs by design, record it with: UPDATE_GOLDEN=1 go test -tags cloud ./cloud -run 'TestExamples/%s$'",
				localPath, localErr, mode, indent(local), indent(actual), filepath.Base(dir))
		}
		return
	}
	if localErr == nil {
		t.Fatalf("the deployed run matches %s; delete %s, it is redundant", localPath, cloudPath)
	}
	if err := Compare(mode, cloud, actual); err != nil {
		t.Fatalf("deployed run does not match %q: %v\n\nExpected (cloud golden, %s):\n%s\n\nActual:\n%s\n\nRegenerate with: UPDATE_GOLDEN=1 go test -tags cloud ./cloud -run 'TestExamples/%s$'",
			cloudPath, err, mode, indent(cloud), indent(actual), filepath.Base(dir))
	}
}

// ReadGolden parses the golden file at path. The error wraps
// [os.ErrNotExist] when the file is missing.
func ReadGolden(path string) ([]durabletest.OperationSignature, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading golden file %q: %w", path, err)
	}
	var sigs []durabletest.OperationSignature
	if err := json.Unmarshal(data, &sigs); err != nil {
		return nil, fmt.Errorf("reading golden file %q: parse JSON: %w", path, err)
	}
	return sigs, nil
}

// Compare reports how actual fails to match expected under mode, or nil
// when it matches.
func Compare(mode SignatureMode, expected, actual []durabletest.OperationSignature) error {
	switch mode {
	case Ordered:
		if len(expected) != len(actual) {
			return fmt.Errorf("%d operations, want %d", len(actual), len(expected))
		}
		for i := range expected {
			if expected[i] != actual[i] {
				return fmt.Errorf("operation %d is %+v, want %+v", i, actual[i], expected[i])
			}
		}
		return nil
	case Unordered:
		if len(expected) != len(actual) {
			return fmt.Errorf("%d operations, want %d", len(actual), len(expected))
		}
		return missing(expected, actual)
	case Subset:
		return missing(expected, actual)
	default:
		return fmt.Errorf("unknown signature mode %s", mode)
	}
}

// missing reports the first required operation that actual lacks,
// counting duplicates.
func missing(required, actual []durabletest.OperationSignature) error {
	counts := make(map[durabletest.OperationSignature]int, len(actual))
	for _, sig := range actual {
		counts[sig]++
	}
	for _, req := range required {
		if counts[req] <= 0 {
			return fmt.Errorf("missing operation %+v", req)
		}
		counts[req]--
	}
	return nil
}

func writeGolden(t *testing.T, path string, sigs []durabletest.OperationSignature) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating golden file directory: %v", err)
	}
	if err := os.WriteFile(path, append(indent(sigs), '\n'), 0o644); err != nil {
		t.Fatalf("writing golden file %q: %v", path, err)
	}
}

func indent(sigs []durabletest.OperationSignature) []byte {
	if sigs == nil {
		sigs = []durabletest.OperationSignature{}
	}
	data, err := json.MarshalIndent(sigs, "", "  ")
	if err != nil {
		panic(err)
	}
	return data
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// OperationSignature is the deterministic, comparable subset of a single
// checkpointed operation's identity. It captures the operation's type,
// subtype, name, and status — the shape of the operation log — while
// normalizing away timestamps, tokens, hashed IDs, and other
// run-specific values that would make golden files brittle.
type OperationSignature struct {
	// Type is the operation type (e.g. "STEP", "WAIT", "CALLBACK").
	Type string `json:"type"`

	// SubType further qualifies the type (e.g. "Step",
	// "WaitForCondition", "RunInChildContext").
	SubType string `json:"subType,omitempty"`

	// Name is the user-assigned operation name, empty for unnamed ops.
	Name string `json:"name,omitempty"`

	// Status is the operation's terminal or last-observed status.
	Status string `json:"status"`
}

// EventSignature derives a stable, deterministic signature from a
// [TestResult]'s operation log. The signature captures the
// type/subtype/name/status sequence of operations while normalizing
// away timestamps, tokens, hashed IDs, and other run-specific values.
//
// This is useful for golden-file assertions: the signature captures
// WHAT operations ran and in what order, without being brittle to
// irrelevant changes between runs.
func EventSignature(result *TestResult) []OperationSignature {
	if result == nil {
		return nil
	}
	sigs := make([]OperationSignature, 0, len(result.Operations))
	for _, op := range result.Operations {
		sigs = append(sigs, OperationSignature{
			Type:    op.Type,
			SubType: op.SubType,
			Name:    op.Name,
			Status:  op.Status,
		})
	}
	return sigs
}

// AssertGoldenSignature compares the event signature of result against a
// golden file at goldenPath. If the file does not exist or the
// environment variable UPDATE_GOLDEN is set to "1", the golden file is
// written (or overwritten) with the current signature.
//
// On mismatch, the test fails with a diff showing expected vs actual.
//
// # Regenerating Golden Files
//
// Set UPDATE_GOLDEN=1 to regenerate:
//
//	UPDATE_GOLDEN=1 go test ./durable/durabletest/ -run TestMyScenario
//
// This follows the standard Go testdata golden-file convention.
func AssertGoldenSignature(t *testing.T, result *TestResult, goldenPath string) {
	t.Helper()

	actual := EventSignature(result)

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		writeGolden(t, goldenPath, actual)
		t.Logf("wrote golden file %s (UPDATE_GOLDEN=1)", goldenPath)
		return
	}

	expected, err := readGolden(goldenPath)
	if err != nil {
		t.Fatalf("reading golden file %q: %v\nRun with UPDATE_GOLDEN=1 to create it.", goldenPath, err)
	}

	if !signaturesEqual(expected, actual) {
		expectedJSON, _ := json.MarshalIndent(expected, "", "  ")
		actualJSON, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf(
			"operation signature mismatch against %q\n\nExpected (golden):\n%s\n\nActual:\n%s\n\nRegenerate with: UPDATE_GOLDEN=1 go test -run %s ./...",
			goldenPath, expectedJSON, actualJSON, t.Name(),
		)
	}
}

func signaturesEqual(a, b []OperationSignature) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readGolden(path string) ([]OperationSignature, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	var sigs []OperationSignature
	if err := json.Unmarshal(data, &sigs); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	return sigs, nil
}

func writeGolden(t *testing.T, path string, sigs []OperationSignature) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating golden file directory: %v", err)
	}
	data, err := json.MarshalIndent(sigs, "", "  ")
	if err != nil {
		t.Fatalf("marshaling golden signatures: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing golden file %q: %v", path, err)
	}
}

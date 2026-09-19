// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// twoSteps is the handler whose signature testdata/signature.golden
// records: step "first" then step "second".
func twoSteps(ctx durable.Context, _ any) (string, error) {
	if _, err := durable.Step(ctx, "first", func(_ durable.StepContext) (string, error) {
		return "1", nil
	}); err != nil {
		return "", err
	}
	return durable.Step(ctx, "second", func(_ durable.StepContext) (string, error) {
		return "2", nil
	})
}

// twoStepsReversed runs the same two steps in the other order: a
// deliberate change to the operation sequence.
func twoStepsReversed(ctx durable.Context, _ any) (string, error) {
	if _, err := durable.Step(ctx, "second", func(_ durable.StepContext) (string, error) {
		return "2", nil
	}); err != nil {
		return "", err
	}
	return durable.Step(ctx, "first", func(_ durable.StepContext) (string, error) {
		return "1", nil
	})
}

// threeSteps runs the golden's two steps and one more.
func threeSteps(ctx durable.Context, event any) (string, error) {
	if _, err := twoSteps(ctx, event); err != nil {
		return "", err
	}
	return durable.Step(ctx, "third", func(_ durable.StepContext) (string, error) {
		return "3", nil
	})
}

// oneStep runs only the golden's first step.
func oneStep(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "first", func(_ durable.StepContext) (string, error) {
		return "1", nil
	})
}

func runLocal(t *testing.T, h durable.Handler[any, string]) *durabletest.TestResult {
	t.Helper()
	result := durabletest.NewLocalRunner(h).RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	return result
}

func TestAssertSignaturePasses(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "")
	cases := []struct {
		name    string
		handler durable.Handler[any, string]
		mode    SignatureMode
	}{
		{"ordered/same sequence", twoSteps, Ordered},
		{"unordered/same sequence", twoSteps, Unordered},
		{"unordered/reversed sequence", twoStepsReversed, Unordered},
		{"subset/same sequence", twoSteps, Subset},
		{"subset/extra operation", threeSteps, Subset},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			AssertSignature(t, runLocal(t, tc.handler), tc.mode)
		})
	}
}

// mismatchEnv names the case a child process runs. The parent test
// re-executes the test binary with it set so that the expected t.Fatalf
// fails the child, not the parent.
const mismatchEnv = "EXTEST_SIGNATURE_MISMATCH_CASE"

var mismatchCases = map[string]struct {
	handler durable.Handler[any, string]
	mode    SignatureMode
	want    string
}{
	"ordered/reversed sequence": {twoStepsReversed, Ordered, "operation 0 is"},
	"ordered/extra operation":   {threeSteps, Ordered, "3 operations, want 2"},
	"unordered/extra operation": {threeSteps, Unordered, "3 operations, want 2"},
	"subset/missing operation":  {oneStep, Subset, "missing operation"},
}

// TestAssertSignatureMismatchFails shows that a deliberate change to the
// operation sequence fails the golden comparison. Each case runs in a
// child process because the assertion calls t.Fatalf; the parent checks
// that the child failed and reported the mismatch.
func TestAssertSignatureMismatchFails(t *testing.T) {
	if name := os.Getenv(mismatchEnv); name != "" {
		tc := mismatchCases[name]
		AssertSignature(t, runLocal(t, tc.handler), tc.mode)
		return
	}
	for name, tc := range mismatchCases {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestAssertSignatureMismatchFails$", "-test.v")
			cmd.Env = append(os.Environ(), mismatchEnv+"="+name, "UPDATE_GOLDEN=")
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("child process did not fail; err = %v\noutput:\n%s", err, out)
			}
			for _, want := range []string{"operation signature mismatch", tc.want} {
				if !strings.Contains(string(out), want) {
					t.Fatalf("child output lacks %q:\n%s", want, out)
				}
			}
		})
	}
}

func TestAssertSignatureWritesGolden(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "1")
	for _, mode := range []SignatureMode{Ordered, Unordered, Subset} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Chdir(t.TempDir())
			AssertSignature(t, runLocal(t, twoSteps), mode)
			got, err := os.ReadFile(filepath.FromSlash(GoldenPath))
			if err != nil {
				t.Fatalf("golden file not written: %v", err)
			}
			for _, name := range []string{`"name": "first"`, `"name": "second"`} {
				if !strings.Contains(string(got), name) {
					t.Fatalf("golden file lacks %s:\n%s", name, got)
				}
			}
		})
	}
}

func TestAssertSignatureMissingGoldenFails(t *testing.T) {
	if os.Getenv(mismatchEnv) == "missing-golden" {
		t.Chdir(t.TempDir())
		AssertSignature(t, runLocal(t, twoSteps), Subset)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAssertSignatureMissingGoldenFails$")
	cmd.Env = append(os.Environ(), mismatchEnv+"=missing-golden", "UPDATE_GOLDEN=")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child process did not fail; err = %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(string(out), "Run with UPDATE_GOLDEN=1 to create it") {
		t.Fatalf("child output lacks the regeneration hint:\n%s", out)
	}
}

func TestSignatureModeString(t *testing.T) {
	for mode, want := range map[SignatureMode]string{Ordered: "Ordered", Unordered: "Unordered", Subset: "Subset", 0: "SignatureMode(0)", 7: "SignatureMode(7)"} {
		if got := mode.String(); got != want {
			t.Errorf("SignatureMode(%d).String() = %q, want %q", int(mode), got, want)
		}
	}
}

func TestCompareRejectsUnsetMode(t *testing.T) {
	sigs := durabletest.EventSignature(runLocal(t, twoSteps))
	if err := Compare(unsetMode, sigs, sigs); err == nil || !strings.Contains(err.Error(), "unknown signature mode") {
		t.Fatalf("Compare with the zero mode returned %v, want unknown mode error", err)
	}
}

// cloudDir copies the package's golden into a fresh example directory so
// AssertCloudSignature can be exercised against it.
func cloudDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	golden, err := os.ReadFile(filepath.FromSlash(GoldenPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(GoldenPath)), golden, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestAssertCloudSignatureMatchesLocalGolden(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "")
	dir := cloudDir(t)
	AssertCloudSignature(t, runLocal(t, twoSteps), Ordered, dir)
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(CloudGoldenPath))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a matching deployed run must not create a cloud golden; stat = %v", err)
	}
}

func TestAssertCloudSignatureWritesCloudGoldenOnDifference(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "1")
	dir := cloudDir(t)
	before, _ := os.ReadFile(filepath.Join(dir, filepath.FromSlash(GoldenPath)))

	AssertCloudSignature(t, runLocal(t, threeSteps), Ordered, dir)

	after, _ := os.ReadFile(filepath.Join(dir, filepath.FromSlash(GoldenPath)))
	if string(before) != string(after) {
		t.Fatal("UPDATE_GOLDEN in cloud mode rewrote the local golden")
	}
	cloud, err := ReadGolden(filepath.Join(dir, filepath.FromSlash(CloudGoldenPath)))
	if err != nil {
		t.Fatalf("cloud golden not written: %v", err)
	}
	if len(cloud) != 3 || cloud[2].Name != "third" {
		t.Fatalf("cloud golden = %+v, want the three-step sequence", cloud)
	}

	// The written cloud golden is what a later comparison uses.
	t.Setenv("UPDATE_GOLDEN", "")
	AssertCloudSignature(t, runLocal(t, threeSteps), Ordered, dir)
}

// cloudFailureCases run in a child process; see TestAssertSignatureMismatchFails.
var cloudFailureCases = map[string]struct {
	handler durable.Handler[any, string]
	cloud   bool // whether a cloud golden for threeSteps exists
	want    string
}{
	"differs without cloud golden": {threeSteps, false, "If the deployed run differs by design"},
	"differs from cloud golden":    {oneStep, true, "does not match"},
	"redundant cloud golden":       {twoSteps, true, "it is redundant"},
}

func TestAssertCloudSignatureFails(t *testing.T) {
	if name := os.Getenv(mismatchEnv); strings.HasPrefix(name, "cloud/") {
		tc := cloudFailureCases[strings.TrimPrefix(name, "cloud/")]
		dir := cloudDir(t)
		if tc.cloud {
			three := durabletest.EventSignature(runLocal(t, threeSteps))
			writeGolden(t, filepath.Join(dir, filepath.FromSlash(CloudGoldenPath)), three)
		}
		AssertCloudSignature(t, runLocal(t, tc.handler), Ordered, dir)
		return
	}
	for name, tc := range cloudFailureCases {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestAssertCloudSignatureFails$", "-test.v")
			cmd.Env = append(os.Environ(), mismatchEnv+"=cloud/"+name, "UPDATE_GOLDEN=")
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("child process did not fail; err = %v\noutput:\n%s", err, out)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Fatalf("child output lacks %q:\n%s", tc.want, out)
			}
		})
	}
}

package testing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestEventSignatures_SingleStep_ExcludesRootExecutionAndNonDeterministicFields
// confirms the core contract: a single-step handler's signature is
// exactly one entry (the step), the root EXECUTION operation is
// excluded, and the signature is stable across repeated runs of an
// otherwise-identical handler (no timestamps/IDs leaking through).
func TestEventSignatures_SingleStep_ExcludesRootExecutionAndNonDeterministicFields(t *testing.T) {
	handler := func(event string, dc types.DurableContext) (string, error) {
		return operations.Step(dc, "greet", func(sc types.StepContext) (string, error) {
			return "hello " + event, nil
		})
	}

	runner := New(durable.Handler[string, string](handler), nil)
	result, err := runner.Run("world")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	sigs := EventSignatures(result)
	want := []EventSignature{
		{Depth: 0, Index: 1, Type: types.OperationTypeStep, SubType: "Step", Status: types.OperationStatusSucceeded, Name: "greet"},
	}
	if !equalSignatures(sigs, want) {
		t.Fatalf("EventSignatures mismatch:\ngot:  %+v\nwant: %+v", sigs, want)
	}

	// Run a SECOND, independent execution of the exact same handler and
	// confirm the signature is bit-for-bit identical - this is the
	// property a golden-file assertion actually depends on: if this
	// varied run-to-run for no behavioral reason (e.g. because it leaked
	// a timestamp or a raw operation ID), NO golden file could ever be
	// committed successfully.
	result2, err := runner.Run("world")
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if !equalSignatures(EventSignatures(result2), want) {
		t.Fatalf("expected EventSignatures to be identical across independent runs of the same handler")
	}
}

func TestEventSignatures_NestedChildContext_CapturesDepthAndOrder(t *testing.T) {
	handler := func(event string, dc types.DurableContext) (string, error) {
		return operations.RunInChildContext(dc, "child", func(cc types.DurableContext) (string, error) {
			a, err := operations.Step(cc, "step-a", func(sc types.StepContext) (string, error) { return "a", nil })
			if err != nil {
				return "", err
			}
			b, err := operations.Step(cc, "step-b", func(sc types.StepContext) (string, error) { return "b", nil })
			if err != nil {
				return "", err
			}
			return a + b, nil
		})
	}

	runner := New(durable.Handler[string, string](handler), nil)
	result, err := runner.Run("x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	sigs := EventSignatures(result)
	want := []EventSignature{
		{Depth: 0, Index: 1, Type: types.OperationTypeContext, SubType: "RunInChildContext", Status: types.OperationStatusSucceeded, Name: "child"},
		{Depth: 1, Index: 1, Type: types.OperationTypeStep, SubType: "Step", Status: types.OperationStatusSucceeded, Name: "step-a"},
		{Depth: 1, Index: 2, Type: types.OperationTypeStep, SubType: "Step", Status: types.OperationStatusSucceeded, Name: "step-b"},
	}
	if !equalSignatures(sigs, want) {
		t.Fatalf("EventSignatures mismatch:\ngot:  %+v\nwant: %+v", sigs, want)
	}
}

func TestEventSignatures_FailedStep_CapturesFailedStatus(t *testing.T) {
	handler := func(event string, dc types.DurableContext) (string, error) {
		return operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", assertErr("boom")
		}, operations.WithStepRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: false}
		}))
	}

	runner := New(durable.Handler[string, string](handler), nil)
	result, err := runner.Run("x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED, got %s", result.GetStatus())
	}

	sigs := EventSignatures(result)
	want := []EventSignature{
		{Depth: 0, Index: 1, Type: types.OperationTypeStep, SubType: "Step", Status: types.OperationStatusFailed, Name: "always-fails"},
	}
	if !equalSignatures(sigs, want) {
		t.Fatalf("EventSignatures mismatch:\ngot:  %+v\nwant: %+v", sigs, want)
	}
}

func TestAssertEventSignatures_MatchesGoldenFile(t *testing.T) {
	handler := func(event string, dc types.DurableContext) (string, error) {
		return operations.Step(dc, "greet", func(sc types.StepContext) (string, error) {
			return "hello " + event, nil
		})
	}
	runner := New(durable.Handler[string, string](handler), nil)
	result, err := runner.Run("world")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dir := t.TempDir()
	golden := filepath.Join(dir, "single-step.history.json")

	// No golden file exists yet - AssertEventSignatures must fail with a
	// clear "run with UPDATE_GOLDEN=1" message, not panic or silently
	// pass. Verified directly via readGoldenFile (the exact function
	// AssertEventSignatures delegates to for this check) rather than via
	// AssertEventSignatures itself, since that calls t.Fatalf on
	// failure - which would fail this very test, not just demonstrate
	// the behavior.
	if _, err := readGoldenFile(golden); err == nil {
		t.Fatal("expected reading a nonexistent golden file to fail")
	}

	// Regenerate via the UPDATE_GOLDEN=1 path.
	t.Setenv("UPDATE_GOLDEN", "1")
	AssertEventSignatures(t, result, golden)
	t.Setenv("UPDATE_GOLDEN", "")

	if _, err := os.Stat(golden); err != nil {
		t.Fatalf("expected UPDATE_GOLDEN=1 to create the golden file: %v", err)
	}

	// Now assert against the freshly-written golden file - must pass.
	AssertEventSignatures(t, result, golden)

	// A second, independent run of the SAME handler must also match the
	// same golden file - this is the actual regression-detection
	// property the whole feature exists for.
	result2, err := runner.Run("world")
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	AssertEventSignatures(t, result2, golden)
}

func TestAssertEventSignatures_DetectsOperationLogChange(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "drifted.history.json")

	oneStepHandler := func(event string, dc types.DurableContext) (string, error) {
		return operations.Step(dc, "only-step", func(sc types.StepContext) (string, error) { return "ok", nil })
	}
	runner := New(durable.Handler[string, string](oneStepHandler), nil)
	result, err := runner.Run("x")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Setenv("UPDATE_GOLDEN", "1")
	AssertEventSignatures(t, result, golden)
	t.Setenv("UPDATE_GOLDEN", "")

	// A DIFFERENT handler (an extra step added) run against the SAME
	// golden file must be reported as a mismatch - this is exactly the
	// "unintended operation-log change" detection this task's own
	// framing calls out as the point of the feature, which a bare
	// result-equality check would never catch (both handlers can return
	// unrelated results, or even coincidentally the same one).
	twoStepHandler := func(event string, dc types.DurableContext) (string, error) {
		if _, err := operations.Step(dc, "only-step", func(sc types.StepContext) (string, error) { return "ok", nil }); err != nil {
			return "", err
		}
		return operations.Step(dc, "extra-step", func(sc types.StepContext) (string, error) { return "ok2", nil })
	}
	runner2 := New(durable.Handler[string, string](twoStepHandler), nil)
	result2, err := runner2.Run("x")
	if err != nil {
		t.Fatalf("Run (drifted handler): %v", err)
	}

	drifted := EventSignatures(result2)
	goldenSigs, err := readGoldenFile(golden)
	if err != nil {
		t.Fatalf("readGoldenFile: %v", err)
	}
	if equalSignatures(drifted, goldenSigs) {
		t.Fatal("expected the two-step handler's signature to differ from the committed one-step golden file")
	}
}

func assertErr(msg string) error { return &simpleErr{msg} }

type simpleErr struct{ msg string }

func (e *simpleErr) Error() string { return e.msg }

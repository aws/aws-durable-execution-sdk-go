package durable_test

// These tests cover the checkpoint-marshal sites other than JSONSerdes and
// the step result. Each runs a handler whose stored value contains <, > and
// &, and asserts that the stored bytes keep those characters literal.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func runHTMLSucceeded[I, O any](t *testing.T, h func(durable.Context, I) (O, error), event I) *durabletest.TestResult {
	t.Helper()
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(event)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	return r
}

func storedContextResult(t *testing.T, r *durabletest.TestResult, name string) string {
	t.Helper()
	op := r.Operation(name)
	if op == nil || op.ContextDetails == nil {
		t.Fatalf("no stored context operation named %q", name)
	}
	return op.ContextDetails.Result
}

// TestAllSettledStoresHTMLLiteral covers Settled.MarshalJSON: the AllSettled
// outcome elements, both a fulfilled value and a rejected error message.
func TestAllSettledStoresHTMLLiteral(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		good := durable.StepAsync(ctx, "good", func(durable.StepContext) (string, error) {
			return rawHTML, nil
		})
		bad := durable.StepAsync(ctx, "bad", func(durable.StepContext) (string, error) {
			return "", errors.New(rawHTML)
		}, durable.WithRetry(durable.NoRetry()))
		settled, err := durable.AllSettled(ctx, "collect", []*durable.Future[string]{good, bad})
		if err != nil {
			return 0, err
		}
		return len(settled), nil
	}
	r := runHTMLSucceeded(t, h, nil)
	got := storedContextResult(t, r, "collect")
	t.Logf("stored AllSettled payload: %s", got)
	assertNoHTMLEscape(t, "stored AllSettled payload", got)
	if strings.Count(got, rawHTML) < 2 {
		t.Errorf("stored AllSettled payload: want the value and the error message literal: %s", got)
	}
}

// TestMapAggregateStoresHTMLLiteral covers the Map aggregate record, both an
// item result and the error data of a failed item.
func TestMapAggregateStoresHTMLLiteral(t *testing.T) {
	one := 1
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []string{"ok", "fail"},
			func(c durable.Context, item string, _ int) (string, error) {
				return durable.Step(c, "s", func(durable.StepContext) (string, error) {
					if item == "fail" {
						return "", errors.New(rawHTML)
					}
					return rawHTML, nil
				}, durable.WithRetry(durable.NoRetry()))
			},
			durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: &one}))
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r := runHTMLSucceeded(t, h, nil)
	got := storedContextResult(t, r, "m")
	t.Logf("stored Map payload: %s", got)
	assertNoHTMLEscape(t, "stored Map payload", got)
	if strings.Count(got, rawHTML) < 2 {
		t.Errorf("stored Map payload: want the item result and the item error literal: %s", got)
	}
}

// TestHandlerResultKeepsHTMLLiteral covers the handler's final result.
func TestHandlerResultKeepsHTMLLiteral(t *testing.T) {
	h := func(_ durable.Context, _ any) (htmlValue, error) {
		return htmlValue{HTML: rawHTML}, nil
	}
	r := runHTMLSucceeded(t, h, nil)
	t.Logf("stored handler result: %s", r.RawResult)
	assertNoHTMLEscape(t, "stored handler result", r.RawResult)
}

// TestFileSystemSerdesKeepsHTMLLiteral covers FileSystemSerdes: the inline
// envelope in overflow mode, and the written file and its envelope in
// always mode.
func TestFileSystemSerdesKeepsHTMLLiteral(t *testing.T) {
	ctx := context.Background()
	meta := durable.SerdesContext{}
	v := htmlValue{HTML: rawHTML}

	inline := durable.NewFileSystemSerdes(t.TempDir(), durable.FileSystemSerdesConfig{
		Mode: durable.FileSystemSerdesModeOverflow,
	})
	env, err := inline.Marshal(ctx, meta, v)
	if err != nil {
		t.Fatalf("overflow Marshal: %v", err)
	}
	t.Logf("inline envelope: %s", env)
	assertNoHTMLEscape(t, "inline envelope", string(env))
	var back htmlValue
	if err := inline.Unmarshal(ctx, meta, env, &back); err != nil || back != v {
		t.Fatalf("inline round trip = %+v, %v; want %+v", back, err, v)
	}

	always := durable.NewFileSystemSerdes(t.TempDir(), durable.FileSystemSerdesConfig{
		Mode: durable.FileSystemSerdesModeAlways,
		GeneratePreview: func(any) map[string]any {
			return map[string]any{"html": rawHTML}
		},
	})
	env, err = always.Marshal(ctx, meta, v)
	if err != nil {
		t.Fatalf("always Marshal: %v", err)
	}
	t.Logf("file envelope: %s", env)
	assertNoHTMLEscape(t, "file envelope preview", string(env))
	path := fileOfEnvelope(t, env)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	t.Logf("written file: %s", data)
	assertNoHTMLEscape(t, "written file", string(data))
	back = htmlValue{}
	if err := always.Unmarshal(ctx, meta, env, &back); err != nil || back != v {
		t.Fatalf("file round trip = %+v, %v; want %+v", back, err, v)
	}
}

func fileOfEnvelope(t *testing.T, env []byte) string {
	t.Helper()
	var e struct {
		File string `json:"file"`
	}
	if err := durable.JSONSerdes.Unmarshal(context.Background(), durable.SerdesContext{}, env, &e); err != nil || e.File == "" {
		t.Fatalf("envelope has no file: %s (%v)", env, err)
	}
	return e.File
}

package durable

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestBatchCompletionErrorTyped(t *testing.T) {
	// The batch-level fallback of Err() returns a typed
	// *BatchCompletionError carrying the completion reason.
	result := BatchResult[string]{
		Items: []BatchItem[string]{
			{Index: 0, Status: BatchItemSucceeded, Result: "ok"},
			{Index: 1, Status: BatchItemFailed}, // no per-item error
		},
		Reason: CompletionFailureToleranceExceeded,
	}

	got := result.Err()
	var bce *BatchCompletionError
	if !errors.As(got, &bce) {
		t.Fatalf("Err() = %v (%T), want *BatchCompletionError", got, got)
	}
	if bce.Reason != CompletionFailureToleranceExceeded {
		t.Errorf("Reason = %v, want CompletionFailureToleranceExceeded", bce.Reason)
	}
	if !strings.Contains(got.Error(), "batch failed") {
		t.Errorf("Error() = %q, want message containing 'batch failed'", got.Error())
	}
	if !strings.Contains(got.Error(), "FAILURE_TOLERANCE_EXCEEDED") {
		t.Errorf("Error() = %q, want reason in message", got.Error())
	}

	// Like every terminal SDK failure, it also matches *OperationError.
	var opErr *OperationError
	if !errors.As(got, &opErr) {
		t.Errorf("Err() does not match *OperationError")
	}
}

func TestBatchCompletionErrorWireType(t *testing.T) {
	// The wire-error mapper stamps the public type name.
	we := errorObjectFromError(&BatchCompletionError{Reason: CompletionFailureToleranceExceeded}, nil)
	if we.ErrorType != "BatchCompletionError" {
		t.Errorf("ErrorType = %q, want %q", we.ErrorType, "BatchCompletionError")
	}
}

func TestBatchCompletionErrorSurfacesOnResultSerdesReplay(t *testing.T) {
	// A BatchResult replayed through a custom operation-level result
	// serdes is reconstructed purely from exported state, so per-item
	// errors (an interface field) do not survive. Err() must then
	// surface the typed batch-level error on the replay path.
	handler := func(ctx Context, _ any) (string, error) {
		items := []string{"ok", "bad"}
		br, err := Map(ctx, "proj-serde", items, func(_ Context, item string, _ int) (string, error) {
			if item == "bad" {
				return "", errors.New("item exploded")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithBatchResultSerdes(jsonProjectionSerdes{}),
			WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
		if err != nil {
			return "", err
		}
		var bce *BatchCompletionError
		if errors.As(br.Err(), &bce) {
			return "typed:" + bce.Reason.String(), nil
		}
		if br.Err() != nil {
			return "untyped", nil
		}
		return "nil", nil
	}

	// Phase 1: LIVE. Items carry their own errors, so the first item
	// error wins over the batch-level fallback.
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), handler)
	assertSucceeded(t, resp)
	var live string
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if live != "untyped" {
		t.Fatalf("live verdict = %q, want %q (per-item error wins live)", live, "untyped")
	}

	// Extract the Map parent SUCCEED payload written by the custom serdes.
	var mapPayload string
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	// Phase 2: REPLAY from the checkpointed payload. Item errors are
	// gone; the typed batch-level error must surface.
	replayFake := &fakeLambda{}
	replayResp := invokeBatch(t, replayFake, batchPayload(`null`,
		wireOperation{
			Id:             hashID("1"),
			Status:         "SUCCEEDED",
			ContextDetails: &wireContextDetails{Result: mapPayload},
		},
	), handler)
	assertSucceeded(t, replayResp)
	var replay string
	if err := json.Unmarshal([]byte(replayResp.Result), &replay); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}
	if want := "typed:FAILURE_TOLERANCE_EXCEEDED"; replay != want {
		t.Errorf("replay verdict = %q, want %q", replay, want)
	}
}

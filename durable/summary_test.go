package durable

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// contextSucceedUpdate returns the SUCCEED update of the context operation
// with the given wire subtype, or fails the test when there is none.
func contextSucceedUpdate(t *testing.T, fake *fakeLambda, subType string) OperationUpdate {
	t.Helper()
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext && u.Action == OperationActionSucceed && aws.ToString(u.SubType) == subType {
			return u
		}
	}
	t.Fatalf("no SUCCEED update for a %s context operation", subType)
	return OperationUpdate{}
}

// --- child context ---

func TestChildSummaryStoredForOversizedResult(t *testing.T) {
	// A result over the limit is not stored. With WithChildSummary the
	// SUCCEED checkpoint carries the summary as its payload alongside
	// ReplayChildren, and the summary function sees the child's result.
	fake := &fakeLambda{}
	large := strings.Repeat("x", checkpointSizeLimitBytes+1)
	var calls int32
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		out, err := RunInChildContext(ctx, "big", func(Context) (string, error) {
			return large, nil
		}, WithChildSummary(func(result string) string {
			atomic.AddInt32(&calls, 1)
			return fmt.Sprintf("%d bytes", len(result))
		}))
		if err != nil {
			return "", err
		}
		return fmt.Sprint(len(out)), nil
	})
	if want := fmt.Sprintf(`{"Status":"SUCCEEDED","Result":"\"%d\""}`, len(large)); resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if calls != 1 {
		t.Errorf("summary function called %d times, want 1", calls)
	}

	u := contextSucceedUpdate(t, fake, operationSubTypeRunInChildContext)
	if u.ContextOptions == nil || !aws.ToBool(u.ContextOptions.ReplayChildren) {
		t.Error("expected ContextOptions.ReplayChildren = true")
	}
	if got, want := aws.ToString(u.Payload), fmt.Sprintf("%d bytes", len(large)); got != want {
		t.Errorf("payload = %q, want the summary %q", got, want)
	}
}

func TestChildSummaryNotCalledForSmallResult(t *testing.T) {
	// A result within the limit is stored whole; the summary function is
	// not called and the payload is the serialized result.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "small", func(Context) (string, error) {
			return "tiny", nil
		}, WithChildSummary(func(string) string {
			t.Error("summary function called for a result within the limit")
			return "unused"
		}))
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"tiny\""}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	u := contextSucceedUpdate(t, fake, operationSubTypeRunInChildContext)
	if got := aws.ToString(u.Payload); got != `"tiny"` {
		t.Errorf("payload = %q, want the serialized result", got)
	}
	if u.ContextOptions != nil {
		t.Error("small result should not set ContextOptions")
	}
}

func TestChildSummaryReplayIgnoresSummary(t *testing.T) {
	// Replay of a SUCCEEDED child whose payload is a summary re-executes
	// the body: the value comes from the child's operations, never from
	// the summary, and the summary function does not run.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: "3 items", ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"reconstructed"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				t.Fatal("step body should not execute on replay")
				return "", nil
			})
		}, WithChildSummary(func(string) string {
			t.Error("summary function called on replay")
			return ""
		}))
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"reconstructed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if len(fake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(fake.gotUpdateBatches))
	}
}

func TestChildSummaryOversizedIsTruncated(t *testing.T) {
	// A summary longer than the limit is cut to the limit on a UTF-8
	// boundary. Three-byte runes make the boundary observable: the limit
	// is not a multiple of three, so a byte-exact cut would split a rune.
	fake := &fakeLambda{}
	large := strings.Repeat("x", checkpointSizeLimitBytes+1)
	summary := strings.Repeat("€", checkpointSizeLimitBytes) // 3 bytes each
	invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "big", func(Context) (string, error) {
			return large, nil
		}, WithChildSummary(func(string) string { return summary }))
		return "", err
	})
	got := aws.ToString(contextSucceedUpdate(t, fake, operationSubTypeRunInChildContext).Payload)
	if len(got) > checkpointSizeLimitBytes {
		t.Fatalf("summary payload is %d bytes, over the %d limit", len(got), checkpointSizeLimitBytes)
	}
	if !utf8.ValidString(got) {
		t.Error("truncated summary is not valid UTF-8")
	}
	if !strings.HasPrefix(summary, got) || len(got) < checkpointSizeLimitBytes-utf8.UTFMax {
		t.Errorf("truncated summary is %d bytes and not a maximal prefix", len(got))
	}
}

func TestChildSummaryEmptyLeavesPayloadAbsent(t *testing.T) {
	fake := &fakeLambda{}
	large := strings.Repeat("x", checkpointSizeLimitBytes+1)
	invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "big", func(Context) (string, error) {
			return large, nil
		}, WithChildSummary(func(string) string { return "" }))
		return "", err
	})
	u := contextSucceedUpdate(t, fake, operationSubTypeRunInChildContext)
	if u.Payload != nil {
		t.Errorf("payload = %q, want absent for an empty summary", *u.Payload)
	}
	if u.ContextOptions == nil || !aws.ToBool(u.ContextOptions.ReplayChildren) {
		t.Error("expected ContextOptions.ReplayChildren = true")
	}
}

func TestChildSummaryTypeMismatchIsConfigurationError(t *testing.T) {
	// A summary function for another result type fails the operation
	// before any checkpoint is written, so the defect surfaces on the
	// first run whatever the result size.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "typed", func(Context) (int, error) {
			return 1, nil
		}, WithChildSummary(func(string) string { return "" }))
		if err == nil {
			return "", fmt.Errorf("expected a configuration error")
		}
		if !strings.Contains(err.Error(), "WithChildSummary") || !strings.Contains(err.Error(), "want func(int) string") {
			return "", fmt.Errorf("unexpected error text: %v", err)
		}
		return "ok", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext {
			t.Fatalf("a context checkpoint was written for the misconfigured child: %+v", u)
		}
	}
}

func TestChildSummaryAsyncStoredForOversizedResult(t *testing.T) {
	// Go forwards WithChildSummary to the asynchronous path.
	fake := &fakeLambda{}
	large := strings.Repeat("x", checkpointSizeLimitBytes+1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "big", func(Context) (string, error) {
			return large, nil
		}, WithChildSummary(func(result string) string { return fmt.Sprintf("%d bytes", len(result)) }))
		out, err := fut.Result()
		if err != nil {
			return "", err
		}
		return fmt.Sprint(len(out)), nil
	})
	if want := fmt.Sprintf(`{"Status":"SUCCEEDED","Result":"\"%d\""}`, len(large)); resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	u := contextSucceedUpdate(t, fake, operationSubTypeRunInChildContext)
	if got, want := aws.ToString(u.Payload), fmt.Sprintf("%d bytes", len(large)); got != want {
		t.Errorf("payload = %q, want the summary %q", got, want)
	}
}

func TestChildSummaryAsyncTypeMismatchFailsFuture(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "typed", func(Context) (int, error) { return 1, nil },
			WithChildSummary(func(string) string { return "" }))
		_, err := fut.Result()
		if err == nil || !strings.Contains(err.Error(), "WithChildSummary") {
			return "", fmt.Errorf("expected a configuration error, got %v", err)
		}
		return "ok", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
}

// --- batch ---

// summaryBatchShape is what the batch tests return from the handler so
// live and replayed runs can be compared.
type summaryBatchShape struct {
	Total   int    `json:"total"`
	Success int    `json:"success"`
	Reason  string `json:"reason"`
	Bytes   int    `json:"bytes"`
}

// summaryBatchHandler returns a handler running a Map whose aggregate
// exceeds the checkpoint limit while each item stays under it, with a
// summary function that counts its calls.
func summaryBatchHandler(calls *int32, nesting NestingMode) Handler[any, summaryBatchShape] {
	big := strings.Repeat("x", 100*1024)
	return func(ctx Context, _ any) (summaryBatchShape, error) {
		br, err := Map(ctx, "big", []int{0, 1, 2}, func(c Context, _ int, idx int) (string, error) {
			return Step(c, fmt.Sprintf("s%d", idx), func(StepContext) (string, error) { return big, nil })
		},
			WithNesting(nesting),
			WithBatchSummary(func(r BatchResult[string]) string {
				atomic.AddInt32(calls, 1)
				return fmt.Sprintf("%d/%d done", r.SuccessCount(), r.TotalCount())
			}))
		if err != nil {
			return summaryBatchShape{}, err
		}
		bytes := 0
		for _, s := range br.Results() {
			bytes += len(s)
		}
		return summaryBatchShape{Total: br.TotalCount(), Success: br.SuccessCount(), Reason: br.Reason.String(), Bytes: bytes}, nil
	}
}

// batchSummaryLiveThenReplay runs handler live, asserts the parent's
// SUCCEED checkpoint carries the summary in its record, then replays from
// the recorded checkpoint log and asserts the same result without calling
// the summary function again.
func batchSummaryLiveThenReplay(t *testing.T, nesting NestingMode) {
	t.Helper()
	var calls int32
	handler := summaryBatchHandler(&calls, nesting)

	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), handler)
	assertSucceeded(t, resp)
	var live summaryBatchShape
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatal(err)
	}
	if live.Success != 3 || live.Bytes != 3*100*1024 {
		t.Fatalf("live result %+v, want 3 successes of 100 KiB", live)
	}
	if calls != 1 {
		t.Errorf("summary function called %d times live, want 1", calls)
	}

	parent := contextSucceedUpdate(t, fake, operationSubTypeMap)
	if parent.ContextOptions == nil || !aws.ToBool(parent.ContextOptions.ReplayChildren) {
		t.Fatal("parent SUCCEED did not set ReplayChildren; aggregate did not exceed the limit")
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(parent.Payload)), &stored); err != nil {
		t.Fatalf("parent payload is not a JSON record: %v", err)
	}
	if got := stored["summary"]; got != "3/3 done" {
		t.Errorf(`record "summary" = %v, want "3/3 done"`, got)
	}
	if _, ok := parseBatchReplayRecord(aws.ToString(parent.Payload)); !ok {
		t.Error("record with a summary no longer parses as a replay record")
	}

	// Rebuild the checkpoint log from the live updates and replay.
	replayOps := []wireOperation{{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: aws.ToString(parent.Payload), ReplayChildren: true},
	}}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext {
			continue
		}
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed {
			replayOps = append(replayOps, wireOperation{
				Id:          aws.ToString(u.Id),
				ParentId:    aws.ToString(u.ParentId),
				Status:      "SUCCEEDED",
				StepDetails: &wireStepDetails{Result: aws.ToString(u.Payload)},
			})
		}
	}
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == operationSubTypeMapIteration && u.Action == OperationActionSucceed {
			replayOps = append(replayOps, wireOperation{
				Id:             aws.ToString(u.Id),
				ParentId:       aws.ToString(u.ParentId),
				Status:         "SUCCEEDED",
				ContextDetails: &wireContextDetails{Result: aws.ToString(u.Payload)},
			})
		}
	}

	replayFake := &fakeLambda{}
	replayResp := invokeBatch(t, replayFake, batchPayload(`null`, replayOps...), handler)
	assertSucceeded(t, replayResp)
	var replayed summaryBatchShape
	if err := json.Unmarshal([]byte(replayResp.Result), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed != live {
		t.Errorf("replayed %+v, live %+v", replayed, live)
	}
	if calls != 1 {
		t.Errorf("summary function called %d times in total, want 1 (never on replay)", calls)
	}
	if len(replayFake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(replayFake.gotUpdateBatches))
	}
}

func TestBatchSummaryStoredAndReplayedNormal(t *testing.T) {
	batchSummaryLiveThenReplay(t, NestingNormal)
}

func TestBatchSummaryStoredAndReplayedFlat(t *testing.T) {
	batchSummaryLiveThenReplay(t, NestingFlat)
}

func TestBatchSummaryNotCalledForSmallResult(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (int, error) {
		br, err := Map(ctx, "small", []int{1, 2}, func(_ Context, item int, _ int) (int, error) {
			return item, nil
		}, WithBatchSummary(func(BatchResult[int]) string {
			t.Error("summary function called for a result within the limit")
			return ""
		}))
		if err != nil {
			return 0, err
		}
		return br.SuccessCount(), nil
	})
	assertSucceeded(t, resp)
	parent := contextSucceedUpdate(t, fake, operationSubTypeMap)
	if parent.ContextOptions != nil {
		t.Error("small result should not set ContextOptions")
	}
	if strings.Contains(aws.ToString(parent.Payload), `"summary"`) {
		t.Errorf("payload %q carries a summary key", aws.ToString(parent.Payload))
	}
}

func TestBatchSummaryParallel(t *testing.T) {
	fake := &fakeLambda{}
	big := strings.Repeat("y", 150*1024)
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (int, error) {
		br, err := Parallel(ctx, "par", []Branch[string]{
			{Name: "a", Func: func(Context) (string, error) { return big, nil }},
			{Name: "b", Func: func(Context) (string, error) { return big, nil }},
		}, WithBatchSummary(func(r BatchResult[string]) string {
			return fmt.Sprintf("branches=%d", r.SuccessCount())
		}))
		if err != nil {
			return 0, err
		}
		return br.SuccessCount(), nil
	})
	assertSucceeded(t, resp)
	parent := contextSucceedUpdate(t, fake, operationSubTypeParallel)
	var stored map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(parent.Payload)), &stored); err != nil {
		t.Fatalf("parent payload is not a JSON record: %v", err)
	}
	if got := stored["summary"]; got != "branches=2" {
		t.Errorf(`record "summary" = %v, want "branches=2"`, got)
	}
}

func TestBatchSummaryTypeMismatchIsConfigurationError(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(ctx Context) error
	}{
		{"Map", func(ctx Context) error {
			_, err := Map(ctx, "m", []int{1}, func(Context, int, int) (int, error) { return 1, nil },
				WithBatchSummary(func(BatchResult[string]) string { return "" }))
			return err
		}},
		{"Parallel", func(ctx Context) error {
			_, err := Parallel(ctx, "p", []Branch[int]{{Name: "a", Func: func(Context) (int, error) { return 1, nil }}},
				WithBatchSummary(func(BatchResult[string]) string { return "" }))
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLambda{}
			resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
				err := tc.run(ctx)
				if err == nil {
					return "", fmt.Errorf("expected a configuration error")
				}
				if !strings.Contains(err.Error(), "WithBatchSummary") || !strings.Contains(err.Error(), "want func(durable.BatchResult[int]) string") {
					return "", fmt.Errorf("unexpected error text: %v", err)
				}
				return "ok", nil
			})
			assertSucceeded(t, resp)
			for _, u := range updateBatch(t, fake) {
				if u.Type == OperationTypeContext {
					t.Fatalf("a context checkpoint was written for the misconfigured batch: %+v", u)
				}
			}
		})
	}
}

// --- panic recovery ---

// summaryPanicChildTests exercises a panicking WithChildSummary function on
// the synchronous and asynchronous child paths. Each case returns the
// error the operation reports for an oversized result.
var summaryPanicChildTests = []struct {
	name string
	run  func(ctx Context, large string) error
}{
	{"RunInChildContext", func(ctx Context, large string) error {
		_, err := RunInChildContext(ctx, "big", func(Context) (string, error) { return large, nil },
			WithChildSummary(func(string) string { panic("summary boom") }))
		return err
	}},
	{"Go", func(ctx Context, large string) error {
		fut := Go(ctx, "big", func(Context) (string, error) { return large, nil },
			WithChildSummary(func(string) string { panic("summary boom") }))
		_, err := fut.Result()
		return err
	}},
}

func TestChildSummaryPanicFailsOperation(t *testing.T) {
	// A panic in the summary function is recovered into an error naming
	// the child. The synchronous path returns it; the asynchronous path
	// settles the future with it instead of unwinding its goroutine. No
	// SUCCEED checkpoint is written for the child.
	large := strings.Repeat("x", checkpointSizeLimitBytes+1)
	for _, tc := range summaryPanicChildTests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLambda{}
			resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
				err := tc.run(ctx, large)
				if err == nil {
					return "", fmt.Errorf("expected the summary panic as an error")
				}
				want := `durable: child context "big": WithChildSummary function panicked: summary boom`
				if err.Error() != want {
					return "", fmt.Errorf("error = %q, want %q", err.Error(), want)
				}
				return "ok", nil
			})
			if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
				t.Fatalf("response = %s, want %s", resp, want)
			}
			for _, u := range updateBatch(t, fake) {
				if u.Type == OperationTypeContext && u.Action == OperationActionSucceed {
					t.Fatalf("a SUCCEED checkpoint was written for the child whose summary panicked: %+v", u)
				}
			}
		})
	}
}

func TestBatchSummaryPanicFailsOperation(t *testing.T) {
	// A panic in the batch summary function is recovered into an error
	// naming the batch, returned in place of the result, and no parent
	// SUCCEED checkpoint is written.
	big := strings.Repeat("y", 150*1024)
	for _, tc := range []struct {
		name string
		run  func(ctx Context) error
	}{
		{"Map", func(ctx Context) error {
			_, err := Map(ctx, "big", []int{0, 1}, func(Context, int, int) (string, error) { return big, nil },
				WithBatchSummary(func(BatchResult[string]) string { panic("summary boom") }))
			return err
		}},
		{"Parallel", func(ctx Context) error {
			_, err := Parallel(ctx, "big", []Branch[string]{
				{Name: "a", Func: func(Context) (string, error) { return big, nil }},
				{Name: "b", Func: func(Context) (string, error) { return big, nil }},
			}, WithBatchSummary(func(BatchResult[string]) string { panic("summary boom") }))
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLambda{}
			resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
				err := tc.run(ctx)
				if err == nil {
					return "", fmt.Errorf("expected the summary panic as an error")
				}
				want := `durable: batch "big": WithBatchSummary function panicked: summary boom`
				if err.Error() != want {
					return "", fmt.Errorf("error = %q, want %q", err.Error(), want)
				}
				return "ok", nil
			})
			assertSucceeded(t, resp)
			for _, u := range updateBatch(t, fake) {
				if u.Type == OperationTypeContext && u.Action == OperationActionSucceed && aws.ToString(u.Name) == "big" {
					t.Fatalf("a SUCCEED checkpoint was written for the batch whose summary panicked: %+v", u)
				}
			}
		})
	}
}

func TestMarshalBatchReplayRecordTruncatesSummaryToFit(t *testing.T) {
	base := batchReplayRecord{
		Reason:       CompletionAllCompleted,
		StartedTotal: 3,
		IndexSet:     replayIndexSetStarted,
		Indexes:      []int{},
	}

	// Three-byte runes with a limit that is not a multiple of three, so
	// the cut must land on a rune boundary. Without escaping, the record
	// fills the limit to within a couple of runes.
	t.Run("multibyte", func(t *testing.T) {
		record := base
		record.Summary = strings.Repeat("€", checkpointSizeLimitBytes)
		b := marshalWithinLimit(t, record)
		if len(b) < checkpointSizeLimitBytes-2*utf8.UTFMax {
			t.Errorf("record is %d bytes; the summary was cut further than the %d limit requires", len(b), checkpointSizeLimitBytes)
		}
	})

	// Control characters are escaped to two bytes each, so the encoded
	// summary is longer than the raw one. The record must still fit.
	t.Run("escaped", func(t *testing.T) {
		record := base
		record.Summary = strings.Repeat("\t€", checkpointSizeLimitBytes)
		marshalWithinLimit(t, record)
	})
}

// marshalWithinLimit marshals record and asserts the result fits the
// checkpoint limit, parses, and carries a valid UTF-8 prefix of the
// original summary.
func marshalWithinLimit(t *testing.T, record batchReplayRecord) []byte {
	t.Helper()
	b, err := marshalBatchReplayRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > checkpointSizeLimitBytes {
		t.Fatalf("record is %d bytes, over the %d limit", len(b), checkpointSizeLimitBytes)
	}
	var parsed batchReplayRecord
	if err := json.Unmarshal(b, &parsed); err != nil {
		t.Fatalf("truncated record does not parse: %v", err)
	}
	if parsed.Summary == "" {
		t.Fatal("summary was dropped although a prefix fits")
	}
	if !utf8.ValidString(parsed.Summary) || !strings.HasPrefix(record.Summary, parsed.Summary) {
		t.Error("truncated summary is not a valid UTF-8 prefix of the original")
	}
	if _, ok := parseBatchReplayRecord(string(b)); !ok {
		t.Error("truncated record is not accepted as a replay record")
	}
	return b
}

func TestMarshalBatchReplayRecordOmitsEmptySummary(t *testing.T) {
	b, err := marshalBatchReplayRecord(batchReplayRecord{
		Reason: CompletionAllCompleted, StartedTotal: 1, IndexSet: replayIndexSetStarted, Indexes: []int{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "summary") {
		t.Errorf("record %s carries a summary key without a summary", b)
	}
}

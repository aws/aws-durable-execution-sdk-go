package durable

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// itemView is a serializable projection of a batch item used to compare the
// live and replayed results for exact identity.
type itemView struct {
	Index  int    `json:"index"`
	Status int    `json:"status"`
	Result string `json:"result"`
}

// replayShape captures everything the determinism contract must preserve:
// per-item index/status/result in order, the total count, and the reason.
type replayShape struct {
	Total  int        `json:"total"`
	Reason string     `json:"reason"`
	Items  []itemView `json:"items"`
}

// TestMapReplayRecordAbandonedOverSizeLimit is the determinism guard for a
// concurrent batch that completes early on MinSuccessful while branches are
// still in flight, with a parent aggregate that exceeds the checkpoint size
// limit. Live returns two large successes plus two abandoned (STARTED)
// branches; without a size-independent decision record the ReplayChildren
// fallback would re-derive only the two successes. The test asserts that the
// items, total count, per-item statuses and completion reason are identical
// live and replayed.
//
// Determinism does not depend on timing: the two abandoned branches never
// return a success (they spin on durable steps until the abandon signal
// unwinds them at a step boundary), so the only successes are the two
// immediate ones, which meet MinSuccessful=2 on every run regardless of
// goroutine scheduling.
func TestMapReplayRecordAbandonedOverSizeLimit(t *testing.T) {
	// Each success result is large enough that two of them exceed the
	// 256 KB parent aggregate limit, while each stays individually under
	// the per-child limit so children checkpoint their own payloads.
	big := strings.Repeat("x", 170*1024)

	handler := func(ctx Context, _ any) (replayShape, error) {
		items := []int{0, 1, 2, 3}
		br, err := Map(ctx, "big", items, func(c Context, item int, idx int) (string, error) {
			if idx < 2 {
				return big, nil
			}
			// Admitted but never terminal: spin on durable steps so this
			// branch cannot succeed. Once the batch completes early the
			// abandon signal unwinds it at the next step, reporting it
			// STARTED.
			for {
				if _, werr := Step(c, "spin", func(StepContext) (string, error) {
					return "", nil
				}); werr != nil {
					return "", werr
				}
			}
		}, WithCompletion(CompletionConfig{MinSuccessful: 2}))
		if err != nil {
			return replayShape{}, err
		}
		views := make([]itemView, 0, len(br.Items))
		for i := range br.Items {
			v := itemView{Index: br.Items[i].Index, Status: int(br.Items[i].Status)}
			if br.Items[i].Status == BatchItemSucceeded {
				v.Result = br.Items[i].Result
			}
			views = append(views, v)
		}
		return replayShape{Total: br.TotalCount(), Reason: br.Reason.String(), Items: views}, nil
	}

	// Phase 1: live execution.
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), handler)
	assertSucceeded(t, resp)

	var live replayShape
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if live.Total != 4 {
		t.Fatalf("live total = %d, want 4 (2 succeeded + 2 abandoned)", live.Total)
	}
	if live.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Fatalf("live reason = %q, want MIN_SUCCESSFUL_REACHED", live.Reason)
	}
	if len(live.Items) != 4 ||
		live.Items[0].Status != int(BatchItemSucceeded) ||
		live.Items[1].Status != int(BatchItemSucceeded) ||
		live.Items[2].Status != int(BatchItemStarted) ||
		live.Items[3].Status != int(BatchItemStarted) {
		t.Fatalf("live item shape unexpected: %+v", live.Items)
	}

	// Collect the parent's decision record and each child's final state
	// from the live checkpoint stream.
	var mapRecord string
	var mapReplayChildren bool
	childStatus := map[string]string{}
	childPayload := map[string]string{}
	childOrder := []string{}
	for _, batch := range fake.gotUpdateBatches {
		for _, u := range batch {
			id := aws.ToString(u.Id)
			switch aws.ToString(u.SubType) {
			case operationSubTypeMap:
				if u.Action == types.OperationActionSucceed {
					mapRecord = aws.ToString(u.Payload)
					mapReplayChildren = u.ContextOptions != nil && aws.ToBool(u.ContextOptions.ReplayChildren)
				}
			case operationSubTypeMapIteration:
				if _, seen := childStatus[id]; !seen {
					childOrder = append(childOrder, id)
				}
				switch u.Action {
				case types.OperationActionStart:
					childStatus[id] = "STARTED"
				case types.OperationActionSucceed:
					childStatus[id] = "SUCCEEDED"
					childPayload[id] = aws.ToString(u.Payload)
				case types.OperationActionFail:
					childStatus[id] = "FAILED"
				}
			}
		}
	}
	if !mapReplayChildren {
		t.Fatal("parent Map SUCCEED did not set ReplayChildren; aggregate did not exceed the size limit")
	}
	if mapRecord == "" {
		t.Fatal("parent Map SUCCEED carried no decision record on the ReplayChildren path")
	}
	if len(childOrder) != 4 {
		t.Fatalf("expected 4 child iteration ops, got %d", len(childOrder))
	}

	// Phase 2: replay from the reconstructed checkpoint log. Type/SubType
	// are left unset so replay-consistency validation is skipped, matching
	// the existing batch replay tests; status and context details drive the
	// reconstruction.
	replayOps := []wireOperation{{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: mapRecord, ReplayChildren: true},
	}}
	for _, id := range childOrder {
		op := wireOperation{
			Id:             id,
			ParentId:       hashID("1"),
			Status:         childStatus[id],
			ContextDetails: &wireContextDetails{Result: childPayload[id]},
		}
		replayOps = append(replayOps, op)
	}

	replayFake := &fakeLambda{}
	replayResp := invokeBatch(t, replayFake, batchPayload(`null`, replayOps...), handler)
	assertSucceeded(t, replayResp)

	var replayed replayShape
	if err := json.Unmarshal([]byte(replayResp.Result), &replayed); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}

	// The core assertion: identical shape live versus replayed.
	if replayed.Total != live.Total {
		t.Errorf("replay total = %d, live = %d", replayed.Total, live.Total)
	}
	if replayed.Reason != live.Reason {
		t.Errorf("replay reason = %q, live = %q", replayed.Reason, live.Reason)
	}
	if len(replayed.Items) != len(live.Items) {
		t.Fatalf("replay item count = %d, live = %d", len(replayed.Items), len(live.Items))
	}
	for i := range live.Items {
		if replayed.Items[i] != live.Items[i] {
			t.Errorf("item %d: replay %+v, live %+v (result lengths replay=%d live=%d)",
				i, itemView{Index: replayed.Items[i].Index, Status: replayed.Items[i].Status},
				itemView{Index: live.Items[i].Index, Status: live.Items[i].Status},
				len(replayed.Items[i].Result), len(live.Items[i].Result))
		}
	}

	// Replay of a fully-recorded terminal batch issues no new checkpoints.
	if len(replayFake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0 (pure replay)", len(replayFake.gotUpdateBatches))
	}
}

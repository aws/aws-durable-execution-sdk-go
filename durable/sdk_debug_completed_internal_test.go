package durable

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

// completedSink records the operation completed records it receives, with
// whether each carries replay=true.
type completedSink struct {
	mu       sync.Mutex
	replayed []bool
}

type completedHandler struct {
	sink   *completedSink
	replay bool
}

func (h completedHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h completedHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message != debugMsgOperationCompleted {
		return nil
	}
	replay := h.replay
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == logKeyReplay && a.Value.Bool() {
			replay = true
		}
		return true
	})
	h.sink.mu.Lock()
	h.sink.replayed = append(h.sink.replayed, replay)
	h.sink.mu.Unlock()
	return nil
}

func (h completedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	for _, a := range attrs {
		if a.Key == logKeyReplay && a.Value.Bool() {
			h.replay = true
		}
	}
	return h
}

func (h completedHandler) WithGroup(string) slog.Handler { return h }

// TestOperationCompletedFollowsOutcomeReplay checks that the operation
// completed record takes its replay state from the outcome it reports, not
// from the context. A context that is live reports a replayed outcome as a
// replayed record, and a context that is replaying reports a newly
// observed outcome as a live record.
func TestOperationCompletedFollowsOutcomeReplay(t *testing.T) {
	cases := []struct {
		name           string
		ctxReplaying   bool
		outcomeReplay  bool
		emitReplayed   bool
		wantRecords    int
		wantReplayAttr bool
	}{
		{"live context, replayed outcome, suppress", false, true, false, 0, false},
		{"live context, replayed outcome, emit", false, true, true, 1, true},
		{"replaying context, new outcome, suppress", true, false, false, 1, false},
		{"replaying context, new outcome, emit", true, false, true, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := []*operation{execOp()}
			if tc.ctxReplaying {
				ops = append(ops, checkpointed("1", statusSucceeded))
			}
			ec := newTestContext(t, ops)
			if ec.IsReplaying() != tc.ctxReplaying {
				t.Fatalf("IsReplaying = %v, want %v", ec.IsReplaying(), tc.ctxReplaying)
			}
			sink := &completedSink{}
			ec.setLogDefaults(logDefaults{handler: completedHandler{sink: sink}, emitReplayed: tc.emitReplayed})

			info := ec.operationHookInfo("1", "op", string(OperationTypeStep), OperationSubTypeStep, tc.outcomeReplay)
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)

			sink.mu.Lock()
			defer sink.mu.Unlock()
			if len(sink.replayed) != tc.wantRecords {
				t.Fatalf("operation completed records = %d, want %d", len(sink.replayed), tc.wantRecords)
			}
			if tc.wantRecords == 1 && sink.replayed[0] != tc.wantReplayAttr {
				t.Fatalf("replay attribute = %v, want %v", sink.replayed[0], tc.wantReplayAttr)
			}
		})
	}
}

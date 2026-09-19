// Command plugin_replay_flags implements conformance requirement 10-13: a
// non-terminal step observed during replay reports replay=true, and a
// step that is already terminal is not re-emitted.
//
// The Go SDK reports a step replayed from a terminal checkpoint on every
// invocation that replays it: OnOperationStart with IsReplay true and the
// checkpointed terminal status, then OnOperationEnd with IsReplay true. The
// plugin below recognizes that pair from the start hook's own info
// (IsReplay set and a terminal status) and emits neither record, so each
// step's live start and terminal end appear exactly once. A step re-entered
// before it settled, such as one whose retry is due, reports its
// checkpointed non-terminal status with IsReplay true and is emitted.
//
// The op field of each record is OperationHookInfo.ID. The Go SDK fills it
// with the operation's positional id (for example 1 or 1-1), while the
// execution history and the ParentID field carry the checkpointed
// operation id. The runner compares op against the history id, so the
// requirement is registered as not implemented in template_plugin.yaml
// until the two ids agree.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// replayFlagsPlugin reports the operation lifecycle of step operations.
// replayedTerminal holds, for the current invocation, the ids of steps
// whose start reported a replayed terminal checkpoint; their end is the
// second half of that replay and is skipped too.
type replayFlagsPlugin struct {
	mu               sync.Mutex
	replayedTerminal map[string]bool
}

func (p *replayFlagsPlugin) plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.Capture(info.ExecutionArn)
			p.mu.Lock()
			p.replayedTerminal = map[string]bool{}
			p.mu.Unlock()
		},
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			if info.IsReplay && pluginlog.OperationTerminal(info.Status) {
				p.mu.Lock()
				p.replayedTerminal[info.ID] = true
				p.mu.Unlock()
				return
			}
			out.Emit(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-start",
				"op":     info.ID,
				"replay": info.IsReplay,
			})
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			p.mu.Lock()
			skip := p.replayedTerminal[info.ID]
			delete(p.replayedTerminal, info.ID)
			p.mu.Unlock()
			if skip {
				return
			}
			out.Emit(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"status": string(info.Status),
			})
		},
	}
}

func handler(ctx durable.Context, _ any) (string, error) {
	// Step A succeeds on its first attempt and is terminal before any replay.
	if _, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "step-a", nil
	}); err != nil {
		return "", err
	}
	// Step B fails on its first attempt and succeeds on the second.
	if _, err := durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		if sc.Attempt() < 2 {
			return "", fmt.Errorf("step B first attempt failed")
		}
		return "step-b", nil
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  2,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	}))); err != nil {
		return "", err
	}
	return "Operation succeeded", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins((&replayFlagsPlugin{}).plugin()))
}

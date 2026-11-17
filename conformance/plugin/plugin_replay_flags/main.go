// Command plugin_replay_flags implements conformance requirement 10-13: a
// non-terminal step observed during replay reports replay=true, and a
// step that is already terminal is not re-emitted.
//
// The Go SDK dispatches no hook for a step replayed from a terminal
// checkpoint. The invocation that ran the step's last attempt dispatched
// its start and its end. A step re-entered before it settled, such as one
// whose retry is due, reports its checkpointed non-terminal status with
// IsReplay true. So the plugin below emits every step hook it receives.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newReplayFlagsPlugin is a durable.PluginFactory. It reports the
// operation lifecycle of step operations.
func newReplayFlagsPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-start",
				"op":     info.ID,
				"replay": info.IsReplay,
			}, inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"status": string(info.Status),
			}, inv.ExecutionArn)
		},
	}, nil
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
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newReplayFlagsPlugin)))
}

// Command plugin_retry_exhaustion implements conformance requirement 10-15:
// the attempt hooks fire for every attempt until the retries are
// exhausted, then the operation-end hook reports FAILED.
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
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the attempt hooks and the terminal end of step
// operations. The end fires only once the step has an outcome: the first
// attempt's retryable failure suspends the invocation without an end.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
	},
	OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "attempt-start",
			"n":      info.Attempt,
			"op":     info.ID,
		})
	},
	OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{
			"plugin":  "CONFPLUGIN",
			"hook":    "attempt-end",
			"n":       info.Attempt,
			"outcome": string(info.Outcome),
			"op":      info.ID,
		})
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
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

func handler(ctx durable.Context, _ any) (string, error) {
	// Two attempts in total, one second apart; both fail.
	if _, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("always fails")
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  2,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	}))); err != nil {
		return "", err
	}
	return "unreachable", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}

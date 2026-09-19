// Command plugin_attempt_hooks_retry implements conformance requirement
// 10-3: a plugin reports each attempt of a retried step with its attempt
// number and outcome.
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
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the attempt hooks of step operations. Attempts are
// numbered from 1 and the numbering continues across invocations. The
// attempt hooks run on the goroutine that runs the step body, so their
// order is deterministic.
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
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		// The step context's attempt counter is 1 on the first attempt.
		// The step fails once and succeeds on the second attempt.
		if sc.Attempt() < 2 {
			return "", fmt.Errorf("Attempt %d failed", sc.Attempt())
		}
		return "Operation succeeded", nil
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}

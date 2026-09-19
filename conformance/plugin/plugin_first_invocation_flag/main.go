// Command plugin_first_invocation_flag implements conformance requirement
// 10-6: a plugin sees the first-invocation flag true once, then false on
// the replay that resumes after a wait.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the invocation lifecycle. The invocation-end of the
// suspending first invocation carries the PENDING status; the terminal end
// on the replay carries SUCCEEDED.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
		out.EmitWithArn(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "invocation-start",
			"first":  info.IsFirstInvocation,
		}, info.ExecutionArn)
	},
	OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
		out.EmitWithArn(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "invocation-end",
			"status": string(info.Status),
		}, info.ExecutionArn)
	},
}

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return "Wait completed", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}

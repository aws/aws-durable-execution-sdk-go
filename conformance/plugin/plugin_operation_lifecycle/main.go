// Command plugin_operation_lifecycle implements conformance requirement
// 10-2: a plugin reports the operation-start and operation-end hooks of a
// step, carrying the operation id the history checkpointed.
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

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the lifecycle of step operations. The invocation-start
// hook only captures the execution ARN.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
	},
	OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "operation-start",
			"op":     info.ID,
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

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}

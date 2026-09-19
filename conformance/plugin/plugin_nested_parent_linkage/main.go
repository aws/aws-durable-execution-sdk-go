// Command plugin_nested_parent_linkage implements conformance requirement
// 10-11: the operation-end hook reports the parent id of a step nested in
// a child context, and no parent for the child context itself.
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

// plugin reports the parent linkage of every operation that reaches a
// terminal status.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "operation-end",
			"op":     info.ID,
			"parent": pluginlog.OrNone(info.ParentID),
			"status": string(info.Status),
		})
	},
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return "Hello, " + name + "!", nil
		})
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}

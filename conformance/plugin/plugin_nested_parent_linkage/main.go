// Command plugin_nested_parent_linkage implements conformance requirement
// 10-11: the operation-end hook reports the parent id of a step nested in
// a child context, and no parent for the child context itself.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the parent linkage of every operation that reaches a
// terminal status.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"parent": pluginlog.OrNone(info.ParentID),
				"status": string(info.Status),
			}, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return "Hello, " + name + "!", nil
		})
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}

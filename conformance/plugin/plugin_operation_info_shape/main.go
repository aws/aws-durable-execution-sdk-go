// Command plugin_operation_info_shape implements conformance requirement
// 10-20: the operation-start and operation-end hooks of a step carry the
// full operation field set.
//
// The plugin dumps the info parameter of each hook through
// [pluginlog.InfoRecord]: every field of OperationHookInfo under its
// canonical camelCase name, with an unset field omitted. The requirement
// asserts the presence of each probed key, so an omitted field is the
// signal that the Go info type lacks it or left it unset.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin dumps the start and end info of step operations.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(pluginlog.InfoRecord("operation-start", info), inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(pluginlog.InfoRecord("operation-end", info), inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "greet", func(_ durable.StepContext) (string, error) {
		return "task-a", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}

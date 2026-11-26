// Command plugin_operation_change_shape implements conformance requirement
// 10-22: an item of the operation-change hook's UpdatedOperations carries
// the full operation field set, and the same ID is present in the hook's
// Operations map.
//
// The plugin dumps each step item through [pluginlog.InfoRecord]: every
// field of OperationHookInfo under its canonical camelCase name, with an
// unset field omitted. The requirement asserts the presence of each probed
// key, so an omitted field is the signal that the item lacks it or the SDK
// left it unset.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin emits one record for each step in a change's UpdatedOperations:
// the dump of the item, plus the hook-level fields executionArn,
// updatedOperationsCount, operationsCount, and inFullMap.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationChange: func(_ context.Context, info durable.OperationChangeHookInfo) {
			for id, item := range info.UpdatedOperations {
				if pluginlog.Upper(item.Type) != string(durable.OperationTypeStep) {
					continue
				}
				rec := pluginlog.InfoRecord("operation-change", item)
				if info.ExecutionArn != "" {
					rec["executionArn"] = info.ExecutionArn
				}
				rec["updatedOperationsCount"] = len(info.UpdatedOperations)
				rec["operationsCount"] = len(info.Operations)
				_, rec["inFullMap"] = info.Operations[id]
				out.EmitWithArn(rec, inv.ExecutionArn)
			}
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

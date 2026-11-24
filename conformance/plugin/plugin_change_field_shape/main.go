// Command plugin_change_field_shape implements conformance requirement
// 10-22: each step entry of the operation-change hook's UpdatedOperations
// carries the full operation field set and is present in Operations.
//
// The plugin dumps each entry through [pluginlog.InfoRecord]: every field
// of OperationHookInfo under its canonical camelCase name, with an unset
// field omitted. The requirement asserts the presence of each probed key,
// so an omitted field is the signal that the entry lacks it or left it
// unset. The record also carries the fields of the change info itself.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin dumps the step entries of each change.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationChange: func(_ context.Context, info durable.OperationChangeHookInfo) {
			for id, op := range info.UpdatedOperations {
				if pluginlog.Upper(op.Type) != string(durable.OperationTypeStep) {
					continue
				}
				rec := pluginlog.InfoRecord("operation-change", op)
				rec["executionArn"] = info.ExecutionArn
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

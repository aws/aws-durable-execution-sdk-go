// Command plugin_operation_change implements conformance requirement 10-8:
// the operation-change hook reports each updated step operation and
// whether it is present in the full operation map.
//
// The handler runs one step. The SDK dispatches OnOperationChange for each
// checkpoint response that changes an operation's status, so the response
// that records the step as SUCCEEDED reports it during the invocation that
// ran it.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newOperationChangePlugin is a durable.PluginFactory: the handler calls it
// once per invocation and the returned Plugin serves that invocation alone.
// The plugin emits one record for each step in a change's
// UpdatedOperations, with in_full_map reporting whether the change's
// Operations map holds the same ID.
func newOperationChangePlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationChange: func(_ context.Context, info durable.OperationChangeHookInfo) {
			for id, op := range info.UpdatedOperations {
				if pluginlog.Upper(op.Type) != string(durable.OperationTypeStep) {
					continue
				}
				_, inFull := info.Operations[id]
				out.EmitWithArn(map[string]any{
					"plugin":      "CONFPLUGIN",
					"hook":        "operation-change",
					"op":          id,
					"status":      string(op.Status),
					"in_full_map": inFull,
				}, inv.ExecutionArn)
			}
		},
	}, nil
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newOperationChangePlugin)))
}

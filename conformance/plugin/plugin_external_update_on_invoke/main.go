// Command plugin_external_update_on_invoke implements conformance
// requirement 10-9: a plugin reports a wait that completed between two
// invocations as an updated operation on the invocation that resumes.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports, from the invocation-start hook, every wait operation the
// SDK lists as updated since the previous invocation. The first invocation
// lists nothing: no operation has been checkpointed yet.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			for id, op := range info.UpdatedOperations {
				if pluginlog.Upper(op.Type) != string(durable.OperationTypeWait) {
					continue
				}
				out.EmitWithArn(map[string]any{
					"plugin": "CONFPLUGIN",
					"hook":   "updated-on-invoke",
					"op":     id,
					"status": string(op.Status),
					"first":  info.IsFirstInvocation,
				}, info.ExecutionArn)
			}
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return "Wait completed", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}

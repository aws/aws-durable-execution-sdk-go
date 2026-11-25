// Command plugin_wait_operation_hooks implements conformance requirement
// 10-10: the operation-start and operation-end hooks fire for a wait
// operation.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the lifecycle of wait operations. The wait starts live in
// the first invocation. It completes while the execution is suspended, so
// the invocation that resumes replays it from its terminal checkpoint and
// dispatches its end.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if pluginlog.Upper(info.Type) != string(durable.OperationTypeWait) {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-start",
				"op":     info.ID,
				"type":   pluginlog.Upper(info.Type),
			}, inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if pluginlog.Upper(info.Type) != string(durable.OperationTypeWait) {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"type":   pluginlog.Upper(info.Type),
				"status": string(info.Status),
			}, inv.ExecutionArn)
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

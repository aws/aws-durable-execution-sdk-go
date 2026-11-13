// Command plugin_operation_info_shape implements conformance requirement
// 10-20: the operation-start and operation-end hooks of a step carry the
// full operation field set.
//
// The plugin dumps the info parameter of each hook as one record: every
// field of OperationHookInfo under its canonical camelCase name, with an
// unset field omitted rather than written as a zero value. The requirement
// asserts the presence of each probed key, so an omitted field is the
// signal that the Go info type lacks it or left it unset.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// operationRecord maps info to the canonical record of hook. A string field
// is written when non-empty, a time when non-zero, the attempt when
// positive, the error when non-nil. Status and type tokens are written as
// the SDK reports them; the suite upper-cases both, and the Go tokens are
// already upper case.
func operationRecord(hook string, info durable.OperationHookInfo) map[string]any {
	rec := map[string]any{
		"plugin":   "CONFPLUGIN",
		"hook":     hook,
		"id":       info.ID,
		"type":     pluginlog.Upper(info.Type),
		"isReplay": info.IsReplay,
	}
	if info.Name != "" {
		rec["name"] = info.Name
	}
	if info.SubType != "" {
		rec["subType"] = info.SubType
	}
	if info.ParentID != "" {
		rec["parentId"] = info.ParentID
	}
	if info.Status != "" {
		rec["status"] = string(info.Status)
	}
	if !info.StartTimestamp.IsZero() {
		rec["startTimestamp"] = info.StartTimestamp.UTC().Format(time.RFC3339Nano)
	}
	if !info.EndTimestamp.IsZero() {
		rec["endTimestamp"] = info.EndTimestamp.UTC().Format(time.RFC3339Nano)
	}
	if info.Result != "" {
		rec["result"] = info.Result
	}
	if info.Error != nil {
		rec["error"] = pluginlog.ErrorMessage(info.Error)
	}
	if info.Attempt > 0 {
		rec["attempt"] = info.Attempt
	}
	return rec
}

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin dumps the start and end info of step operations.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(operationRecord("operation-start", info), inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(operationRecord("operation-end", info), inv.ExecutionArn)
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

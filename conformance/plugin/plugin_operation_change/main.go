// Command plugin_operation_change implements conformance requirement 10-8:
// the operation-change hook reports each updated step operation and
// whether it is present in the full operation map.
//
// The Go SDK dispatches OnOperationChange only for operations whose status
// changed between two invocations: a wait that elapsed, a callback that was
// resolved, a chained invoke that finished. A step that reaches its
// terminal status inside the invocation that started it is not reported
// through that hook. This handler's single step does exactly that, so the
// plugin below emits no record for it, and the requirement is registered
// as not implemented in template_plugin.yaml until the hook also reports
// in-invocation terminal transitions.
package main

import (
	"context"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// operationChangePlugin reports step operations from the operation-change
// hook. The change info carries the updated operations. The full operation
// map is the one the invocation-start hook received, extended with every
// operation the change itself reports: an operation that reaches a
// terminal status during the invocation is part of the execution's
// operations even though the start snapshot predates it.
type operationChangePlugin struct {
	mu   sync.Mutex
	full map[string]durable.OperationHookInfo
}

// fullMap returns the operations known at invocation start merged with the
// operations of one change.
func (p *operationChangePlugin) fullMap(updated map[string]durable.OperationHookInfo) map[string]durable.OperationHookInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	full := make(map[string]durable.OperationHookInfo, len(p.full)+len(updated))
	for id, op := range p.full {
		full[id] = op
	}
	for id, op := range updated {
		full[id] = op
	}
	return full
}

func (p *operationChangePlugin) plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.Capture(info.ExecutionArn)
			p.mu.Lock()
			p.full = info.Operations
			p.mu.Unlock()
		},
		OnOperationChange: func(_ context.Context, info durable.OperationChangeHookInfo) {
			full := p.fullMap(info.UpdatedOperations)
			for id, op := range info.UpdatedOperations {
				if pluginlog.Upper(op.Type) != string(durable.OperationTypeStep) {
					continue
				}
				_, inFull := full[id]
				out.Emit(map[string]any{
					"plugin":      "CONFPLUGIN",
					"hook":        "operation-change",
					"op":          id,
					"status":      string(op.Status),
					"in_full_map": inFull,
				})
			}
		},
	}
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins((&operationChangePlugin{}).plugin()))
}

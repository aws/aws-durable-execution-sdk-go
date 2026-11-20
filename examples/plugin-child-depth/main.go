// Command plugin-child-depth demonstrates
// [durable.WithPluginChildOperationsDepth], which bounds how deep in the
// operation tree the operations reported to plugins lie. Both
// WithPluginChildOperationsDepth and the plugin API are experimental and
// may change in a future release.
//
// The handler runs a step, a child context holding two steps, and a final
// step. The plugin records every operation it is told has ended. With a
// depth of 0 it is told only about the operations claimed on the
// handler's Context: the two outer steps and the child context. Every
// operation at that depth is reported with ChildrenOmitted set, meaning
// that anything nested inside it is withheld. For the child context that
// is its two steps, so a plugin that finds no children under it knows the
// subtree was not reported rather than empty. The withheld steps run and
// checkpoint exactly as reported ones do; only the notifications are
// omitted.
package main

import (
	"context"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// reportedOp is one operation the plugin was told has ended.
type reportedOp struct {
	Name            string `json:"name"`
	ChildrenOmitted bool   `json:"childrenOmitted,omitempty"`
}

// recorder collects the reported operations of the current invocation. It
// is safe for concurrent use because the plugin dispatch contract permits
// hooks to fire from several goroutines.
type recorder struct {
	mu  sync.Mutex
	ops []reportedOp
}

// reset discards the operations of earlier invocations. A warm Lambda
// container keeps package-level state between invocations.
func (r *recorder) reset() {
	r.mu.Lock()
	r.ops = nil
	r.mu.Unlock()
}

func (r *recorder) record(op reportedOp) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []reportedOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reportedOp(nil), r.ops...)
}

// rec is shared by the plugin and the handler.
var rec = &recorder{}

// plugin records each operation that ends, with its ChildrenOmitted flag.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, _ durable.InvocationHookInfo) {
		rec.reset()
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		rec.record(reportedOp{Name: info.Name, ChildrenOmitted: info.ChildrenOmitted})
	},
}

// options registers the plugin and reports operations down to depth 0:
// those claimed on the handler's Context. The steps inside the child
// context have depth 1 and are omitted.
var options = []durable.HandlerOption{
	durable.WithPlugins(plugin),
	durable.WithPluginChildOperationsDepth(0),
}

// Output lists the operations the plugin was told have ended before the
// handler returned.
type Output struct {
	Reported []reportedOp `json:"reported"`
}

func step(ctx durable.Context, name string) error {
	_, err := durable.Step(ctx, name, func(_ durable.StepContext) (bool, error) {
		return true, nil
	})
	return err
}

func handler(ctx durable.Context, _ any) (Output, error) {
	if err := step(ctx, "load-order"); err != nil {
		return Output{}, err
	}

	// The child context has depth 0; the two steps inside it have depth 1.
	if _, err := durable.RunInChildContext(ctx, "fulfil", func(cctx durable.Context) (bool, error) {
		if err := step(cctx, "reserve-stock"); err != nil {
			return false, err
		}
		return true, step(cctx, "charge-card")
	}); err != nil {
		return Output{}, err
	}

	if err := step(ctx, "notify"); err != nil {
		return Output{}, err
	}

	return Output{Reported: rec.snapshot()}, nil
}

func main() { durable.Start(handler, options...) }

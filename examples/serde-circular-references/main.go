// Command serde-circular-references shows what happens when an operation
// result contains a reference cycle. It corresponds to the reference example
// serde/circular-references.
//
// The reference SDK's runtime stringifies a cyclic object graph with a
// cycle-safe encoder. Go's [encoding/json] instead returns an error when it
// encounters a cycle, so this adaptation shows both halves of that
// behaviour:
//
//   - With the default serdes, a step that returns a cyclic graph fails.
//     The step's [durable.StepError] names the step and wraps a
//     [durable.SerdesError]; the handler can inspect both with [errors.As].
//   - With a custom serdes that flattens the graph into a list of nodes
//     keyed by ID, the same value checkpoints and is rebuilt on replay with
//     its cycle restored, so a child's Parent is again the very root node.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// OrderNode is one node of an order graph. Parent points back at the
// containing node, so a wired-up graph contains a reference cycle.
type OrderNode struct {
	ID     string
	Parent *OrderNode
	Items  []*OrderNode
}

// buildGraph returns a root order with two line items whose Parent
// pointers refer back to the root.
func buildGraph() *OrderNode {
	root := &OrderNode{ID: "order-1"}
	for _, id := range []string{"line-1", "line-2"} {
		root.Items = append(root.Items, &OrderNode{ID: id, Parent: root})
	}
	return root
}

// flatNode is the wire form of one OrderNode: references are replaced by
// node IDs, so the encoded graph contains no cycle.
type flatNode struct {
	ID     string   `json:"id"`
	Parent string   `json:"parent,omitempty"`
	Items  []string `json:"items"`
}

// flatGraph is the wire form of a whole graph: the root's ID and every
// reachable node in depth-first order.
type flatGraph struct {
	Root  string     `json:"root"`
	Nodes []flatNode `json:"nodes"`
}

// flattenGraph walks the graph from root once per node and records each
// node's references as IDs.
func flattenGraph(root *OrderNode) flatGraph {
	graph := flatGraph{Root: root.ID}
	seen := map[string]bool{}
	var visit func(n *OrderNode)
	visit = func(n *OrderNode) {
		if n == nil || seen[n.ID] {
			return
		}
		seen[n.ID] = true
		fn := flatNode{ID: n.ID, Items: []string{}}
		if n.Parent != nil {
			fn.Parent = n.Parent.ID
		}
		for _, item := range n.Items {
			fn.Items = append(fn.Items, item.ID)
		}
		graph.Nodes = append(graph.Nodes, fn)
		visit(n.Parent)
		for _, item := range n.Items {
			visit(item)
		}
	}
	visit(root)
	return graph
}

// rebuildGraph allocates one OrderNode per wire node and wires the
// references back into pointers, restoring the cycle.
func rebuildGraph(graph flatGraph) (*OrderNode, error) {
	nodes := make(map[string]*OrderNode, len(graph.Nodes))
	for _, fn := range graph.Nodes {
		nodes[fn.ID] = &OrderNode{ID: fn.ID}
	}
	for _, fn := range graph.Nodes {
		n := nodes[fn.ID]
		if fn.Parent != "" {
			parent, ok := nodes[fn.Parent]
			if !ok {
				return nil, fmt.Errorf("node %q refers to unknown parent %q", fn.ID, fn.Parent)
			}
			n.Parent = parent
		}
		for _, id := range fn.Items {
			item, ok := nodes[id]
			if !ok {
				return nil, fmt.Errorf("node %q refers to unknown item %q", fn.ID, id)
			}
			n.Items = append(n.Items, item)
		}
	}
	root, ok := nodes[graph.Root]
	if !ok {
		return nil, fmt.Errorf("root %q is not among the nodes", graph.Root)
	}
	return root, nil
}

// graphSerdes flattens an *OrderNode on the way into the checkpoint and
// rebuilds it, cycle included, on the way out.
var graphSerdes = durable.SerdesOf(
	func(_ context.Context, _ durable.SerdesContext, root *OrderNode) ([]byte, error) {
		return json.Marshal(flattenGraph(root))
	},
	func(_ context.Context, _ durable.SerdesContext, data []byte) (*OrderNode, error) {
		var graph flatGraph
		if err := json.Unmarshal(data, &graph); err != nil {
			return nil, err
		}
		return rebuildGraph(graph)
	},
)

// defaultSerdesOutcome records how the default serdes failed on the cycle.
type defaultSerdesOutcome struct {
	Failed        bool   `json:"failed"`
	IsStepError   bool   `json:"isStepError"`
	IsSerdesError bool   `json:"isSerdesError"`
	Operation     string `json:"operation"`
	ReportsCycle  bool   `json:"reportsCycle"`
}

// customSerdesOutcome records what the rebuilt graph looks like after
// replay.
type customSerdesOutcome struct {
	RootID             string   `json:"rootId"`
	ItemIDs            []string `json:"itemIds"`
	ParentsPointToRoot bool     `json:"parentsPointToRoot"`
}

type output struct {
	DefaultSerdes defaultSerdesOutcome `json:"defaultSerdes"`
	CustomSerdes  customSerdesOutcome  `json:"customSerdes"`
}

func handler(ctx durable.Context, _ any) (output, error) {
	var out output

	// The default serdes is encoding/json, which rejects the cycle. The
	// step is not retried: a value that cannot be encoded will not encode
	// on a later attempt either.
	_, err := durable.Step(ctx, "build-graph-default", func(_ durable.StepContext) (*OrderNode, error) {
		return buildGraph(), nil
	}, durable.WithRetry(durable.NoRetry()))
	if err == nil {
		return output{}, errors.New("expected the default serdes to reject the cyclic graph")
	}
	var stepErr *durable.StepError
	var serdesErr *durable.SerdesError
	out.DefaultSerdes = defaultSerdesOutcome{
		Failed:        true,
		IsStepError:   errors.As(err, &stepErr),
		IsSerdesError: errors.As(err, &serdesErr),
		ReportsCycle:  strings.Contains(err.Error(), "cycle"),
	}
	if stepErr != nil {
		out.DefaultSerdes.Operation = stepErr.Name
	}

	// The same value checkpoints once its serdes breaks the cycle.
	root, err := durable.Step(ctx, "build-graph-flattened", func(_ durable.StepContext) (*OrderNode, error) {
		return buildGraph(), nil
	}, durable.WithStepSerdes(graphSerdes))
	if err != nil {
		return output{}, err
	}

	// The wait forces a replay, so root below is the graph rebuilt from
	// the checkpoint rather than the one the step returned.
	if err := durable.Wait(ctx, "replay", 1*time.Second); err != nil {
		return output{}, err
	}

	out.CustomSerdes, err = durable.Step(ctx, "inspect-graph", func(_ durable.StepContext) (customSerdesOutcome, error) {
		res := customSerdesOutcome{RootID: root.ID, ItemIDs: []string{}, ParentsPointToRoot: len(root.Items) > 0}
		for _, item := range root.Items {
			res.ItemIDs = append(res.ItemIDs, item.ID)
			// Pointer identity: the rebuilt child refers to the rebuilt
			// root itself, not to a copy of it.
			if item.Parent != root {
				res.ParentsPointToRoot = false
			}
		}
		return res, nil
	})
	if err != nil {
		return output{}, err
	}
	return out, nil
}

func main() { durable.Start(handler) }

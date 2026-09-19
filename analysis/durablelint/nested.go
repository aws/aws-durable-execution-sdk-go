package durablelint

import (
	"fmt"
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// NestedOpAnalyzer reports durable operations created inside a step body.
var NestedOpAnalyzer = newAnalyzer("durablenestedop", nestedOpDoc, runNestedOp)

const nestedOpDoc = `report durable operations created inside a step body

A step is a single atomic unit of work: its body runs once and its result is
checkpointed. A durable operation created inside the body of durable.Step,
durable.StepAsync, the check function of durable.WaitForCondition, or the
submitter of durable.WaitForCallback must capture a Context from an
enclosing scope, is skipped entirely when the step is replayed from its
checkpoint, and so leaves the operation log inconsistent between runs. The
rule reports every such call. To group operations, use
durable.RunInChildContext or durable.Go.

Limitations: a step body that calls a named helper which in turn creates
durable operations is not analysed.`

func runNestedOp(pass *analysis.Pass) (any, error) {
	sup := newSuppressions(pass)
	in := inspectorOf(pass)
	in.WithStack([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := operationCall(pass.TypesInfo, call)
		if !ok {
			return true
		}
		for i := len(stack) - 2; i >= 0; i-- {
			switch stack[i].(type) {
			case *ast.FuncDecl:
				return true
			case *ast.FuncLit:
				switch roleOf(pass.TypesInfo, stack, i) {
				case roleChildFunc:
					return true
				case roleStepBody:
					sup.report(pass, call, fmt.Sprintf(
						"durable.%s is called inside a step body; a step is one atomic unit of work, use durable.RunInChildContext to group operations", name))
					return true
				}
			}
		}
		return true
	})
	return nil, nil
}

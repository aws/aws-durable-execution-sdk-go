package durablelint

import (
	"fmt"
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// GoroutineAnalyzer reports durable operations invoked on a goroutine the
// SDK did not start.
var GoroutineAnalyzer = newAnalyzer("durablegoroutine", goroutineDoc, runGoroutine)

const goroutineDoc = `report durable operations run on goroutines started with the go statement

A Context is owned by the goroutine the SDK created it on. A durable
operation invoked from any other goroutine claims its operation ID in a
scheduling-dependent order and fails replay. The rule reports a durable
operation that is lexically inside:

  - a go statement, directly or in an immediately invoked function literal;
  - a function literal passed to (*errgroup.Group).Go or TryGo, including a
    group from errgroup.WithContext;
  - a function literal passed to (*sync.WaitGroup).Go.

Use durable.Go instead: it starts the goroutine and gives it a Context of
its own.

Limitations: a function literal that is first assigned to a variable and
started later (f := func() {...}; go f()) and a named function started
with go are not analysed. A go statement inside a durable.Go body is still
reported, because that goroutine does not own the child Context either.`

func runGoroutine(pass *analysis.Pass) (any, error) {
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
		// go durable.Wait(ctx, ...)
		if j := skipParens(stack, len(stack)-2); j >= 0 {
			if _, ok := stack[j].(*ast.GoStmt); ok {
				sup.report(pass, call, fmt.Sprintf(
					"durable.%s is started with the go statement; use durable.Go so the goroutine owns a Context", name))
				return true
			}
		}
		for i := len(stack) - 2; i >= 0; i-- {
			switch stack[i].(type) {
			case *ast.FuncDecl:
				return true
			case *ast.FuncLit:
				switch roleOf(pass.TypesInfo, stack, i) {
				case roleChildFunc, roleStepBody:
					return true
				case roleGoroutine:
					sup.report(pass, call, fmt.Sprintf(
						"durable.%s runs on a goroutine started by %s; use durable.Go so the goroutine owns a Context",
						name, launcherName(stack, i)))
					return true
				}
			}
		}
		return true
	})
	return nil, nil
}

// launcherName describes how the goroutine-role function literal at
// stack[i] is started, for a diagnostic.
func launcherName(stack []ast.Node, i int) string {
	j := skipParens(stack, i-1)
	if j < 0 {
		return "the go statement"
	}
	call, ok := stack[j].(*ast.CallExpr)
	if !ok || ast.Unparen(call.Fun) == stack[i] {
		return "the go statement"
	}
	return callName(call)
}

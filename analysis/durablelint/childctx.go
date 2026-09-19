package durablelint

import (
	"fmt"
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// ChildContextAnalyzer reports a captured Context used inside a function
// that receives a Context of its own.
var ChildContextAnalyzer = newAnalyzer("durablechildctx", childContextDoc, runChildContext)

const childContextDoc = `report a parent Context used where the child Context was intended

The function passed to durable.RunInChildContext, durable.Go, durable.Map,
durable.Retry, or a Parallel or Select branch receives a child Context of
its own. Durable operations inside that function must use the child
Context: an operation on a captured outer Context claims an operation ID
on the parent, from a goroutine that does not own it, and fails replay.
The rule reports a durable operation whose Context argument is a variable
declared outside the innermost enclosing child-context callback. A helper
closure inside the callback is checked against the callback; a closure
that is not passed to one of those SDK functions is not a callback and is
never checked on its own.

Limitations: the Context argument must be a plain identifier; a Context
stored in a struct field or returned by a function is not checked. A
Context copied into a local variable inside the callback is assumed to be
the child.`

func runChildContext(pass *analysis.Pass) (any, error) {
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
		if !ok || len(call.Args) == 0 {
			return true
		}
		ident, ok := ast.Unparen(call.Args[0]).(*ast.Ident)
		if !ok {
			return true
		}
		used, ok := pass.TypesInfo.Uses[ident].(*types.Var)
		if !ok {
			return true
		}
		fn, params := contextFunc(pass.TypesInfo, stack)
		if fn == nil {
			return true
		}
		for _, p := range params {
			if p == used {
				return true
			}
		}
		if used.Pos() >= fn.Pos() && used.Pos() < fn.End() {
			return true // declared inside the function: assumed to alias its Context
		}
		sup.report(pass, call, fmt.Sprintf(
			"durable.%s uses %s from an enclosing scope inside a function that receives its own Context; %s",
			name, ident.Name, useAdvice(params)))
		return true
	})
	return nil, nil
}

// contextFunc returns the innermost SDK child-context callback enclosing
// the innermost node of stack, with its durable.Context parameters. A
// helper function literal that is not itself such a callback is looked
// through, so a helper closure inside a child callback is checked against
// the callback. It returns nil when a step body or a function declaration
// is reached first, or when no such callback exists.
func contextFunc(info *types.Info, stack []ast.Node) (ast.Node, []types.Object) {
	for i := len(stack) - 2; i >= 0; i-- {
		switch stack[i].(type) {
		case *ast.FuncLit:
			switch roleOf(info, stack, i) {
			case roleStepBody:
				return nil, nil
			case roleChildFunc:
				return stack[i], contextParams(info, stack[i])
			}
		case *ast.FuncDecl:
			return nil, nil
		}
	}
	return nil, nil
}

// useAdvice renders the fix for a diagnostic: the Context parameter to use,
// or a request to name it when it is blank.
func useAdvice(params []types.Object) string {
	var names []string
	for _, p := range params {
		if p.Name() != "_" {
			names = append(names, p.Name())
		}
	}
	if len(names) == 0 {
		return "name that parameter and use it"
	}
	return "use " + strings.Join(names, " or ")
}

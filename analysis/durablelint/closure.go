package durablelint

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// ClosureAnalyzer reports writes to captured variables inside a function
// whose result the SDK checkpoints.
var ClosureAnalyzer = newAnalyzer("durableclosure", closureDoc, runClosure)

const closureDoc = `report writes to captured variables inside checkpointed functions

The SDK checkpoints the result of a step body and of a child-context
function, and on replay returns the stored result without running the
function again. Every side effect of the function is then skipped. A write
to a variable declared outside the function is such a side effect: on the
first run the variable changes, on replay it keeps its old value, and the
code after the operation sees different state on each run. The rule
reports an assignment, a compound assignment, an increment or decrement,
or a range clause that writes a variable declared outside the innermost
enclosing step body or child-context function, including writes made from
a helper closure inside that function. Reading a captured variable is
allowed.

Return the value from the function instead: the SDK checkpoints the return
value, so it is the same on every run.

Limitations: only writes to a plain identifier are reported. A write
through a pointer, to a struct field, a slice element or a map entry of a
captured variable, or a method call that mutates it, is not reported. A
function that is first assigned to a variable and then passed to the SDK
is not analysed.`

func runClosure(pass *analysis.Pass) (any, error) {
	sup := newSuppressions(pass)
	in := inspectorOf(pass)
	in.WithStack([]ast.Node{(*ast.AssignStmt)(nil), (*ast.IncDecStmt)(nil), (*ast.RangeStmt)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push {
			return false
		}
		var targets []ast.Expr
		switch n := n.(type) {
		case *ast.AssignStmt:
			if n.Tok == token.DEFINE {
				return true // declares new variables in the function's own scope
			}
			targets = n.Lhs
		case *ast.IncDecStmt:
			targets = []ast.Expr{n.X}
		case *ast.RangeStmt:
			if n.Tok != token.ASSIGN {
				return true
			}
			targets = []ast.Expr{n.Key, n.Value}
		}
		fn, role := checkpointedFunc(pass.TypesInfo, stack)
		if fn == nil {
			return true
		}
		for _, t := range targets {
			if t == nil {
				continue
			}
			id, ok := ast.Unparen(t).(*ast.Ident)
			if !ok || id.Name == "_" {
				continue
			}
			v, ok := pass.TypesInfo.Uses[id].(*types.Var)
			if !ok || v.IsField() {
				continue
			}
			if v.Pos() >= fn.Pos() && v.Pos() < fn.End() {
				continue // declared inside the function: not captured
			}
			sup.report(pass, id, fmt.Sprintf(
				"%s is declared outside this %s and is written inside it; the write is skipped on replay, return the value instead",
				id.Name, roleName(role)))
		}
		return true
	})
	return nil, nil
}

// checkpointedFunc returns the innermost function literal enclosing the
// innermost node of stack whose result the SDK checkpoints: a step body or
// a child-context function. Helper closures that play neither role are
// looked through. It returns nil when a function declaration is reached
// first.
func checkpointedFunc(info *types.Info, stack []ast.Node) (ast.Node, funcRole) {
	for i := len(stack) - 2; i >= 0; i-- {
		switch stack[i].(type) {
		case *ast.FuncLit:
			switch role := roleOf(info, stack, i); role {
			case roleStepBody, roleChildFunc:
				return stack[i], role
			}
		case *ast.FuncDecl:
			return nil, roleNone
		}
	}
	return nil, roleNone
}

// roleName renders a checkpointed role for a diagnostic.
func roleName(role funcRole) string {
	if role == roleStepBody {
		return "step body"
	}
	return "child context function"
}

// Package durablelint provides static analyzers that report determinism
// violations in handlers written against the AWS Durable Execution SDK for
// Go. Each rule is an independent [analysis.Analyzer]; [Analyzers] lists
// them all for use with multichecker or golangci-lint.
//
// The rules identify SDK calls by package path and function name, so they
// work on any package that imports the SDK, including packages that alias
// the import.
//
// # Suppression
//
// A diagnostic is suppressed by a comment on the reported line or on the
// line directly above it. A comment that follows code covers its own line;
// a comment on a line of its own covers the next line:
//
//	//durable:ignore
//	//durable:ignore durablegoroutine,durablenestedop
//
// Without a rule list the comment suppresses every rule on that line. A
// file is excluded from a rule, or from every rule, with a comment
// anywhere in the file:
//
//	//durable:ignore-file
//	//durable:ignore-file durablenondeterminism
//
// Under golangci-lint the standard //nolint directive also applies.
package durablelint

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzers lists every rule in this package.
var Analyzers = []*analysis.Analyzer{
	GoroutineAnalyzer,
	NestedOpAnalyzer,
	NondeterminismAnalyzer,
	ChildContextAnalyzer,
	ClosureAnalyzer,
}

// sdkPath is the import path of the SDK package whose operations the rules
// recognise.
const sdkPath = "github.com/aws/aws-durable-execution-sdk-go/durable"

// operations names the SDK functions that claim an operation ID on the
// Context they receive. Calling one of them from the wrong goroutine, from
// inside a step body, or on the wrong Context breaks replay.
var operations = map[string]bool{
	"Step":                   true,
	"StepAsync":              true,
	"Wait":                   true,
	"WaitAsync":              true,
	"Invoke":                 true,
	"InvokeAsync":            true,
	"RunInChildContext":      true,
	"RunInChildContextAsync": true,
	"Go":                     true,
	"WaitForCondition":       true,
	"CreateCallback":         true,
	"WaitForCallback":        true,
	"Map":                    true,
	"Parallel":               true,
	"Select":                 true,
	"Retry":                  true,
	"All":                    true,
	"AllSettled":             true,
	"Any":                    true,
	"Race":                   true,
	"Join":                   true,
}

// stepBodies maps an SDK function to the index of its argument that is a
// step body: a function that receives a StepContext and must not create
// durable operations.
var stepBodies = map[string]int{
	"Step":             2,
	"StepAsync":        2,
	"WaitForCondition": 2,
	"WaitForCallback":  2,
}

// childFuncs maps an SDK function to the index of its argument that
// receives a child Context of its own.
var childFuncs = map[string]int{
	"RunInChildContext":      2,
	"RunInChildContextAsync": 2,
	"Go":                     2,
	"Map":                    3,
	"Retry":                  2,
}

// sdkFunc returns the SDK function called by call, or nil when the callee is
// not a package-level function of the SDK package.
func sdkFunc(info *types.Info, call *ast.CallExpr) *types.Func {
	var id *ast.Ident
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		id = fun
	case *ast.SelectorExpr:
		id = fun.Sel
	case *ast.IndexExpr:
		// Explicit instantiation: durable.Step[string](...).
		return sdkFunc(info, &ast.CallExpr{Fun: fun.X})
	case *ast.IndexListExpr:
		return sdkFunc(info, &ast.CallExpr{Fun: fun.X})
	default:
		return nil
	}
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != sdkPath {
		return nil
	}
	if sig, ok := fn.Type().(*types.Signature); !ok || sig.Recv() != nil {
		return nil
	}
	return fn
}

// operationCall reports whether call invokes a durable operation and
// returns its SDK name.
func operationCall(info *types.Info, call *ast.CallExpr) (string, bool) {
	fn := sdkFunc(info, call)
	if fn == nil || !operations[fn.Name()] {
		return "", false
	}
	return fn.Name(), true
}

// isSDKType reports whether t is the named SDK type name (Context or
// StepContext), looking through pointers.
func isSDKType(t types.Type, name string) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == sdkPath && named.Obj().Name() == name
}

// funcType returns the *ast.FuncType and body of a FuncDecl or FuncLit.
func funcType(n ast.Node) (*ast.FuncType, *ast.BlockStmt) {
	switch f := n.(type) {
	case *ast.FuncDecl:
		return f.Type, f.Body
	case *ast.FuncLit:
		return f.Type, f.Body
	}
	return nil, nil
}

// contextParams returns the objects of every parameter of fn whose type is
// the SDK Context.
func contextParams(info *types.Info, fn ast.Node) []types.Object {
	ft, _ := funcType(fn)
	if ft == nil || ft.Params == nil {
		return nil
	}
	var objs []types.Object
	for _, field := range ft.Params.List {
		for _, name := range field.Names {
			obj := info.Defs[name]
			if obj != nil && isSDKType(obj.Type(), "Context") {
				objs = append(objs, obj)
			}
		}
	}
	return objs
}

// hasContextParam reports whether fn declares a parameter of the SDK
// Context type. It checks the parameter types rather than their names, so
// it also recognises blank (_) and unnamed parameters, which declare no
// object.
func hasContextParam(info *types.Info, fn ast.Node) bool {
	ft, _ := funcType(fn)
	if ft == nil || ft.Params == nil {
		return false
	}
	for _, field := range ft.Params.List {
		if isSDKType(info.TypeOf(field.Type), "Context") {
			return true
		}
	}
	return false
}

// argIndex returns the position of arg among call's arguments, looking
// through parentheses, or -1.
func argIndex(call *ast.CallExpr, arg ast.Expr) int {
	for i, a := range call.Args {
		if ast.Unparen(a) == arg {
			return i
		}
	}
	return -1
}

// funcRole is the role a function literal plays for the SDK, found by
// looking at the call that receives it.
type funcRole int

const (
	roleNone      funcRole = iota
	roleStepBody           // receives a StepContext; may not create operations
	roleChildFunc          // receives its own child Context
	roleGoroutine          // runs on a goroutine the SDK does not own
)

// roleOf classifies the function literal at stack[i] from the call that
// receives it. The stack is the inspector's enclosing-node stack, innermost
// last.
func roleOf(info *types.Info, stack []ast.Node, i int) funcRole {
	lit, ok := stack[i].(*ast.FuncLit)
	if !ok {
		return roleNone
	}
	j := skipParens(stack, i-1)
	if j < 0 {
		return roleNone
	}
	switch p := stack[j].(type) {
	case *ast.CallExpr:
		if ast.Unparen(p.Fun) == lit {
			// Immediately invoked literal: go func() { ... }().
			if k := skipParens(stack, j-1); k >= 0 {
				if _, ok := stack[k].(*ast.GoStmt); ok {
					return roleGoroutine
				}
			}
			return roleNone
		}
		idx := argIndex(p, lit)
		if idx < 0 {
			return roleNone
		}
		if fn := sdkFunc(info, p); fn != nil {
			if pos, ok := stepBodies[fn.Name()]; ok && pos == idx {
				return roleStepBody
			}
			if pos, ok := childFuncs[fn.Name()]; ok && pos == idx {
				return roleChildFunc
			}
			return roleNone
		}
		if idx == 0 && isGoroutineLauncher(info, p) {
			return roleGoroutine
		}
	case *ast.KeyValueExpr:
		// Branch{Func: func(ctx durable.Context) ...} for Parallel and Select.
		key, ok := p.Key.(*ast.Ident)
		if !ok || key.Name != "Func" || ast.Unparen(p.Value) != lit || j < 1 {
			return roleNone
		}
		if cl, ok := stack[j-1].(*ast.CompositeLit); ok && isSDKType(info.TypeOf(cl), "Branch") {
			return roleChildFunc
		}
	}
	return roleNone
}

// skipParens returns the index of the first node at or below i in the stack
// that is not a parenthesised expression, or -1.
func skipParens(stack []ast.Node, i int) int {
	for i >= 0 {
		if _, ok := stack[i].(*ast.ParenExpr); !ok {
			return i
		}
		i--
	}
	return -1
}

// isGoroutineLauncher reports whether call is a method call that runs its
// first argument on a new goroutine: (*errgroup.Group).Go, (*errgroup.Group).TryGo,
// and (*sync.WaitGroup).Go.
func isGoroutineLauncher(info *types.Info, call *ast.CallExpr) bool {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	fn, ok := info.Uses[sel.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	switch fn.Pkg().Path() {
	case "golang.org/x/sync/errgroup":
		return fn.Name() == "Go" || fn.Name() == "TryGo"
	case "sync":
		return fn.Name() == "Go" && isNamed(sig.Recv().Type(), "sync", "WaitGroup")
	}
	return false
}

// isNamed reports whether t is the named type pkg.name, looking through
// pointers.
func isNamed(t types.Type, pkg, name string) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == pkg && named.Obj().Name() == name
}

// newAnalyzer builds an analyzer with the shared requirements.
func newAnalyzer(name, doc string, run func(*analysis.Pass) (any, error)) *analysis.Analyzer {
	return &analysis.Analyzer{
		Name:     name,
		Doc:      doc,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Run:      run,
	}
}

// inspectorOf returns the shared AST inspector for pass.
func inspectorOf(pass *analysis.Pass) *inspector.Inspector {
	in, ok := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	if !ok {
		panic("durablelint: inspect analyzer result missing")
	}
	return in
}

// callName renders the callee of call for a diagnostic, as written.
func callName(call *ast.CallExpr) string {
	var b strings.Builder
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		b.WriteString(fun.Name)
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			b.WriteString(x.Name)
			b.WriteByte('.')
		}
		b.WriteString(fun.Sel.Name)
	case *ast.IndexExpr:
		return callName(&ast.CallExpr{Fun: fun.X})
	case *ast.IndexListExpr:
		return callName(&ast.CallExpr{Fun: fun.X})
	default:
		return "call"
	}
	return b.String()
}

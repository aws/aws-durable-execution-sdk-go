// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// Declaration is one signature assertion found in a handler test: the
// golden file it compares against, relative to the example directory,
// and the mode it compares with.
type Declaration struct {
	Golden string
	Mode   SignatureMode
}

// Scope is one function body in the handler test: a Test function, a
// t.Run subtest, or a helper. Runs counts the handler runs in the body,
// not counting nested subtests; Asserts counts the signature assertions.
// A call to a helper that runs the handler and returns without asserting
// counts as a run in the caller.
type Scope struct {
	Name    string
	Runs    int
	Asserts int

	// calls lists the package-level functions the body calls by name.
	calls []string
}

// HandlerTest is what [ParseHandlerTest] finds in an example's
// handler_test.go.
type HandlerTest struct {
	// Declarations lists every signature assertion, in source order.
	Declarations []Declaration

	// Scopes lists every Test function and subtest body.
	Scopes []Scope
}

// ParseHandlerTest reads the signature assertions and the handler runs
// out of the handler test at path without running it. The cloud matrix
// uses the declaration for [GoldenPath] to compare a deployed run the way
// the local test does, so the mode is recorded once, in the handler test.
// The coverage test uses the scopes to require that every body that runs
// the handler also asserts a signature.
//
// A golden path is recognized when it is [GoldenPath], [CloudGoldenPath],
// or a string literal; a mode is recognized when it is one of the
// package constants. Any other argument form is an error, so a test
// cannot hide its choice behind a variable.
func ParseHandlerTest(path string) (*HandlerTest, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}
	pkg := importName(file, "github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest")
	testingPkg := importName(file, "testing")

	var ht HandlerTest
	var firstErr error
	fail := func(pos token.Pos, format string, args ...any) {
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %s", fset.Position(pos), fmt.Sprintf(format, args...))
		}
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ts := testingParams(testingPkg, fn.Type, nil)
		walkScopes(fn.Name.Name, fn.Body, testingPkg, ts, func(name string, body *ast.BlockStmt, ts testingNames) {
			scope := Scope{Name: name}
			inspectShallow(body, ts, func(n ast.Node) {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return
				}
				recv, _ := sel.X.(*ast.Ident)
				switch {
				case recv != nil && recv.Name == pkg && sel.Sel.Name == "AssertSignature":
					scope.Asserts++
					if len(call.Args) != 3 {
						fail(call.Pos(), "AssertSignature takes 3 arguments")
						return
					}
					mode, err := modeOf(pkg, call.Args[2])
					if err != nil {
						fail(call.Args[2].Pos(), "%v", err)
						return
					}
					ht.Declarations = append(ht.Declarations, Declaration{Golden: GoldenPath, Mode: mode})
				case recv != nil && recv.Name == pkg && sel.Sel.Name == "AssertSignatureFile":
					scope.Asserts++
					if len(call.Args) != 4 {
						fail(call.Pos(), "AssertSignatureFile takes 4 arguments")
						return
					}
					mode, err := modeOf(pkg, call.Args[2])
					if err != nil {
						fail(call.Args[2].Pos(), "%v", err)
						return
					}
					golden, err := goldenOf(pkg, call.Args[3])
					if err != nil {
						fail(call.Args[3].Pos(), "%v", err)
						return
					}
					ht.Declarations = append(ht.Declarations, Declaration{Golden: golden, Mode: mode})
				case (recv == nil || !ts[recv.Name]) && (sel.Sel.Name == "RunUntilComplete" || sel.Sel.Name == "Run"):
					scope.Runs++
				}
			})
			inspectShallow(body, ts, func(n ast.Node) {
				if call, ok := n.(*ast.CallExpr); ok {
					if fn, ok := call.Fun.(*ast.Ident); ok {
						scope.calls = append(scope.calls, fn.Name)
					}
				}
			})
			ht.Scopes = append(ht.Scopes, scope)
		})
	}
	if firstErr != nil {
		return nil, firstErr
	}
	ht.attributeHelperRuns()
	return &ht, nil
}

// attributeHelperRuns counts a call to a helper that runs the handler
// without asserting a signature as a run in the calling scope, so the
// caller is the one required to assert. A helper runs the handler when
// its body does, or when it calls another such helper, at any depth. So
// the set of such helpers is computed to a fixpoint before the callers
// are charged.
func (ht *HandlerTest) attributeHelperRuns() {
	unasserted := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, s := range ht.Scopes {
			if !s.isHelper() || s.Asserts > 0 || unasserted[s.Name] {
				continue
			}
			if s.Runs > 0 || s.callsAny(unasserted) {
				unasserted[s.Name] = true
				changed = true
			}
		}
	}
	for i := range ht.Scopes {
		for _, name := range ht.Scopes[i].calls {
			if unasserted[name] {
				ht.Scopes[i].Runs++
			}
		}
	}
}

// isHelper reports whether the scope is a package-level function other
// than a Test function. Subtests are not helpers: nothing calls them by
// name.
func (s Scope) isHelper() bool {
	return !strings.Contains(s.Name, "/") && !strings.HasPrefix(s.Name, "Test")
}

// callsAny reports whether the scope calls a function in names.
func (s Scope) callsAny(names map[string]bool) bool {
	for _, c := range s.calls {
		if names[c] {
			return true
		}
	}
	return false
}

// ModeFor returns the mode the handler test uses to compare the golden
// file at golden. It is an error when the test never asserts that file
// or asserts it with more than one mode.
func (ht *HandlerTest) ModeFor(golden string) (SignatureMode, error) {
	var modes []SignatureMode
	seen := map[SignatureMode]bool{}
	for _, d := range ht.Declarations {
		if d.Golden == golden && !seen[d.Mode] {
			seen[d.Mode] = true
			modes = append(modes, d.Mode)
		}
	}
	switch len(modes) {
	case 0:
		return 0, fmt.Errorf("no signature assertion against %s", golden)
	case 1:
		return modes[0], nil
	default:
		names := make([]string, 0, len(modes))
		for _, m := range modes {
			names = append(names, m.String())
		}
		sort.Strings(names)
		return 0, fmt.Errorf("%s is asserted with more than one mode: %s", golden, strings.Join(names, ", "))
	}
}

// Unasserted lists the Test functions and subtests that run the handler,
// directly or through a helper, without asserting a signature.
func (ht *HandlerTest) Unasserted() []string {
	var names []string
	for _, s := range ht.Scopes {
		if strings.HasPrefix(s.Name, "Test") && s.Runs > 0 && s.Asserts == 0 {
			names = append(names, s.Name)
		}
	}
	return names
}

// testingNames is the set of identifiers in scope that name a *testing.T
// value: the parameters of the enclosing function and of every enclosing
// subtest literal. A Run call on one of them is a subtest, not a handler
// run.
type testingNames map[string]bool

// testingParams returns base extended with the names of the *testing.T
// parameters of fn. testingPkg is the local name of the testing import.
func testingParams(testingPkg string, fn *ast.FuncType, base testingNames) testingNames {
	names := testingNames{}
	for k := range base {
		names[k] = true
	}
	if fn == nil || fn.Params == nil {
		return names
	}
	for _, field := range fn.Params.List {
		star, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "T" {
			continue
		}
		if recv, ok := sel.X.(*ast.Ident); !ok || recv.Name != testingPkg {
			continue
		}
		for _, name := range field.Names {
			names[name.Name] = true
		}
	}
	return names
}

// subtestLiteral returns the function literal passed to a subtest call:
// a two-argument Run call on a *testing.T value whose second argument is
// a function literal. Any other call returns false.
func subtestLiteral(call *ast.CallExpr, ts testingNames) (*ast.FuncLit, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Run" || len(call.Args) != 2 {
		return nil, false
	}
	recv, ok := sel.X.(*ast.Ident)
	if !ok || !ts[recv.Name] {
		return nil, false
	}
	lit, ok := call.Args[1].(*ast.FuncLit)
	return lit, ok
}

// walkScopes calls visit for body and for the body of every subtest
// nested in it, at any depth, naming subtests by their literal name when
// it is one. Each subtest body is visited with the *testing.T names in
// scope there.
func walkScopes(name string, body *ast.BlockStmt, testingPkg string, ts testingNames, visit func(name string, body *ast.BlockStmt, ts testingNames)) {
	visit(name, body, ts)
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		lit, ok := subtestLiteral(call, ts)
		if !ok {
			return true
		}
		sub := name + "/" + subtestName(call.Args[0])
		walkScopes(sub, lit.Body, testingPkg, testingParams(testingPkg, lit.Type, ts), visit)
		return false
	})
}

// inspectShallow visits the nodes of body, not descending into subtest
// literals, which are scopes of their own.
func inspectShallow(body *ast.BlockStmt, ts testingNames, visit func(ast.Node)) {
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if _, ok := subtestLiteral(call, ts); ok {
				return false
			}
		}
		if n != nil {
			visit(n)
		}
		return true
	})
}

func subtestName(e ast.Expr) string {
	if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		if s, err := strconv.Unquote(lit.Value); err == nil {
			return s
		}
	}
	return "<dynamic>"
}

// importName returns the local name the file imports importPath under,
// or the last element of the path when the file does not rename it or
// does not import it.
func importName(file *ast.File, importPath string) string {
	for _, imp := range file.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == importPath {
			if imp.Name != nil {
				return imp.Name.Name
			}
			break
		}
	}
	return importPath[strings.LastIndex(importPath, "/")+1:]
}

func modeOf(pkg string, e ast.Expr) (SignatureMode, error) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return 0, fmt.Errorf("mode must be an extest constant, got %T", e)
	}
	if recv, ok := sel.X.(*ast.Ident); !ok || recv.Name != pkg {
		return 0, fmt.Errorf("mode must be an extest constant")
	}
	switch sel.Sel.Name {
	case "Ordered":
		return Ordered, nil
	case "Unordered":
		return Unordered, nil
	case "Subset":
		return Subset, nil
	}
	return 0, fmt.Errorf("unknown signature mode %s.%s", pkg, sel.Sel.Name)
}

func goldenOf(pkg string, e ast.Expr) (string, error) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind == token.STRING {
			return strconv.Unquote(v.Value)
		}
	case *ast.SelectorExpr:
		if recv, ok := v.X.(*ast.Ident); ok && recv.Name == pkg {
			switch v.Sel.Name {
			case "GoldenPath":
				return GoldenPath, nil
			case "CloudGoldenPath":
				return CloudGoldenPath, nil
			}
		}
	}
	return "", fmt.Errorf("golden path must be a string literal or an extest path constant")
}

package durablelint

import (
	"fmt"
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// NondeterminismAnalyzer reports nondeterministic constructs in durable
// orchestration code outside a step body.
var NondeterminismAnalyzer = newAnalyzer("durablenondeterminism", nondeterminismDoc, runNondeterminism)

const nondeterminismDoc = `report nondeterministic constructs outside a step body

Between checkpoints a handler must be a pure function of its input and of
the checkpointed results it has consumed. The rule reports, in any function
that has a durable.Context parameter and outside any step body:

  - reading the clock: time.Now, time.Since, time.Until;
  - random numbers: the package-level functions of math/rand,
    math/rand/v2 and crypto/rand, and the constructors of
    github.com/google/uuid;
  - Context.RequestID, which changes on every invocation of an execution;
  - a range statement over a map whose body creates durable operations,
    because map iteration order is randomized.

Move the nondeterministic call into a durable.Step so its result is
checkpointed, use durable.ExecutionStartTime for a stable timestamp, and
sort map keys before iterating.

Limitations: only functions that receive a durable.Context are analysed;
a helper without a Context parameter that reads the clock is not reported
even when the handler calls it between operations. A helper literal with
a Context parameter that is declared inside a step body is treated as part
of the step and is not reported. Methods on a seeded *rand.Rand are not
reported. Nondeterminism reached through interfaces or function values is
not reported.`

// nondeterministic maps a package path to the functions in it that return a
// different value on every call. A nil set means every package-level
// function except the constructors listed in constructors.
var nondeterministic = map[string]map[string]bool{
	"time":                   {"Now": true, "Since": true, "Until": true},
	"math/rand":              nil,
	"math/rand/v2":           nil,
	"crypto/rand":            {"Read": true, "Int": true, "Prime": true, "Text": true},
	"github.com/google/uuid": {"New": true, "NewRandom": true, "NewRandomFromReader": true, "NewString": true, "NewUUID": true, "NewV6": true, "NewV7": true},
}

// constructors are the math/rand functions that build a generator from an
// explicit seed and are therefore deterministic.
var constructors = map[string]bool{
	"New": true, "NewSource": true, "NewPCG": true, "NewChaCha8": true, "NewZipf": true, "Seed": true,
}

func runNondeterminism(pass *analysis.Pass) (any, error) {
	sup := newSuppressions(pass)
	in := inspectorOf(pass)
	in.WithStack([]ast.Node{(*ast.CallExpr)(nil), (*ast.RangeStmt)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push {
			return false
		}
		switch n := n.(type) {
		case *ast.CallExpr:
			msg, ok := nondeterministicCall(pass.TypesInfo, n)
			if !ok || !inOrchestration(pass.TypesInfo, stack) {
				return true
			}
			sup.report(pass, n, msg)
		case *ast.RangeStmt:
			if _, ok := pass.TypesInfo.TypeOf(n.X).Underlying().(*types.Map); !ok {
				return true
			}
			if !inOrchestration(pass.TypesInfo, stack) {
				return true
			}
			if name, ok := firstOperation(pass.TypesInfo, n.Body); ok {
				sup.report(pass, n, fmt.Sprintf(
					"range over a map creates durable operation durable.%s in a randomized order; sort the keys and range over the slice", name))
			}
		}
		return true
	})
	return nil, nil
}

// inOrchestration reports whether the innermost node of stack is in
// orchestration code: inside a function with a durable.Context parameter
// and not inside a step body. The walk continues outward past a helper
// literal that has a Context parameter, because a step body that encloses
// the helper still checkpoints everything inside it.
func inOrchestration(info *types.Info, stack []ast.Node) bool {
	orchestration := false
	for i := len(stack) - 2; i >= 0; i-- {
		switch stack[i].(type) {
		case *ast.FuncLit:
			if roleOf(info, stack, i) == roleStepBody {
				return false
			}
			if hasContextParam(info, stack[i]) {
				orchestration = true
			}
		case *ast.FuncDecl:
			return orchestration || hasContextParam(info, stack[i])
		}
	}
	return orchestration
}

// nondeterministicCall reports whether call invokes a nondeterministic
// function and returns the diagnostic message.
func nondeterministicCall(info *types.Info, call *ast.CallExpr) (string, bool) {
	var id *ast.Ident
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		id = fun
	case *ast.SelectorExpr:
		id = fun.Sel
	case *ast.IndexExpr:
		// Explicit instantiation: randv2.N[int](10). The type argument
		// carries no information about determinism, so unwrap it.
		return nondeterministicCall(info, &ast.CallExpr{Fun: fun.X})
	case *ast.IndexListExpr:
		return nondeterministicCall(info, &ast.CallExpr{Fun: fun.X})
	default:
		return "", false
	}
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok {
		return "", false
	}
	if sig.Recv() != nil {
		if fn.Pkg().Path() == sdkPath && fn.Name() == "RequestID" && isSDKType(sig.Recv().Type(), "Context") {
			return "Context.RequestID changes on every invocation of an execution; read it only inside a step body", true
		}
		return "", false
	}
	names, ok := nondeterministic[fn.Pkg().Path()]
	if !ok {
		return "", false
	}
	if names == nil {
		if constructors[fn.Name()] {
			return "", false
		}
	} else if !names[fn.Name()] {
		return "", false
	}
	what := callName(call)
	if fn.Pkg().Path() == "time" {
		return what + " is nondeterministic and is called outside a step body; move it into a durable.Step or use durable.ExecutionStartTime", true
	}
	return what + " is nondeterministic and is called outside a step body; move it into a durable.Step so the value is checkpointed", true
}

// firstOperation returns the name of the first durable operation called
// anywhere inside node.
func firstOperation(info *types.Info, node ast.Node) (string, bool) {
	var found string
	ast.Inspect(node, func(n ast.Node) bool {
		if found != "" {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok {
			if name, ok := operationCall(info, call); ok {
				found = name
				return false
			}
		}
		return true
	})
	return found, found != ""
}

package catalogprogram

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/internal/engine"
)

const enginePath = "github.com/well-prado/new-blok/internal/engine"

// allowedImports is everything Run needs. Anything else, above all the
// inspection contract, a journal, an event hub or a store, would let this
// package observe or persist a run whose literal calls the engine sees as
// taking the workflow input (#260).
var allowedImports = map[string]bool{
	"context":       true,
	"encoding/json": true,
	"fmt":           true,
	"reflect":       true,
	"github.com/well-prado/new-blok/contract": true,
	enginePath: true,
	"github.com/well-prado/new-blok/internal/lowering": true,
	"github.com/well-prado/new-blok/node":              true,
}

// The engine is used in exactly one way: one engine.New(...).WithMaxSteps(...).Run(...)
// chain, in (*Program).Run. This test reads syntax, not types. It is red on:
//   - any other engine method (WithObserver, RunObserved,
//     RunObservedPending, RunJournaled, EmitRunTerminal, RunControl, or one
//     added later) as a selector, called or taken as a method value, or as
//     a whole string literal;
//   - any engine package selector but New, and any reference to engine.New
//     except as the callee that roots the one chain;
//   - any reflect selector but reflect.DeepEqual, so a method cannot be
//     reached by a computed name;
//   - any import outside allowedImports, and dot or blank imports;
//   - an import path imported twice in a file, or under two local names
//     across the package, and reflect or the engine imported under any
//     name but its default. Each allowed package is imported once,
//     unaliased; every local name is still tracked, as defense in depth.
//
// Out of scope: unsafe or reflect used from another package of the module
// on Program's unexported fields (this package's own unsafe import is
// refused), edits to this test, and code-generation tricks.
func TestSourceRunsTheEngineOnlyUnobserved(t *testing.T) {
	allowedMethods := map[string]bool{"WithMaxSteps": true, "Run": true}
	forbidden := map[string]bool{}
	engineType := reflect.TypeOf(&engine.Engine{})
	for i := range engineType.NumMethod() {
		if name := engineType.Method(i).Name; !allowedMethods[name] {
			forbidden[name] = true
		}
	}
	if !forbidden["WithObserver"] || !forbidden["RunObserved"] || !forbidden["RunJournaled"] {
		t.Fatalf("engine method set changed; forbidden set %v is missing an observer or journal entry point", forbidden)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	runs, news, checked := 0, 0, 0
	// localNames is every local name each import path takes, across the
	// package: one path, one name.
	localNames := map[string]map[string]bool{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		// Every local name the engine and reflect take in this file. The
		// rules below require one, the default; tracking all of them keeps
		// a second name from hiding the first.
		engineNames, reflectNames := map[string]bool{}, map[string]bool{}
		imported := map[string]int{}
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if !allowedImports[path] {
				t.Errorf("%s imports %s; this package may not observe, journal or persist a run", name, path)
			}
			if imported[path]++; imported[path] > 1 {
				t.Errorf("%s imports %s more than once; each allowed package is imported once", name, path)
			}
			local := defaultName(path)
			if spec.Name != nil {
				local = spec.Name.Name
			}
			if localNames[path] == nil {
				localNames[path] = map[string]bool{}
			}
			localNames[path][local] = true
			switch path {
			case enginePath:
				engineNames[local] = true
			case "reflect":
				reflectNames[local] = true
			}
			if (path == enginePath || path == "reflect") && spec.Name != nil {
				t.Errorf("%s imports %s as %q; it must be imported unaliased", name, path, spec.Name.Name)
			}
			if spec.Name != nil && (spec.Name.Name == "." || spec.Name.Name == "_") {
				t.Errorf("%s imports %s as %q", name, path, spec.Name.Name)
			}
		}
		// chainNew holds the engine.New selector of each counted run chain:
		// the only place engine.New may appear.
		chainNew := map[*ast.SelectorExpr]bool{}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			inRun := fn.Name.Name == "Run" && fn.Recv != nil && len(fn.Recv.List) == 1 && receiverIs(fn.Recv.List[0].Type, "Program")
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if isEngineNew(call, engineNames) {
					news++
					if !inRun {
						t.Errorf("%s: engine.New outside (*Program).Run", fset.Position(call.Pos()))
					}
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Run" && rootedAtEngineNew(sel.X, engineNames) {
					runs++
					chainNew[chainRoot(sel.X)] = true
					if !inRun {
						t.Errorf("%s: engine run outside (*Program).Run", fset.Position(call.Pos()))
					}
				}
				return true
			})
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					if text, err := strconv.Unquote(x.Value); err == nil && forbidden[text] {
						t.Errorf("%s: string %q names a forbidden engine method", fset.Position(x.Pos()), text)
					}
				}
			case *ast.SelectorExpr:
				if forbidden[x.Sel.Name] {
					t.Errorf("%s: uses engine method %s; a catalog program runs only through plain Run", fset.Position(x.Pos()), x.Sel.Name)
				}
				if id, ok := x.X.(*ast.Ident); ok && engineNames[id.Name] {
					switch {
					case x.Sel.Name != "New":
						t.Errorf("%s: uses engine.%s; only engine.New is allowed", fset.Position(x.Pos()), x.Sel.Name)
					case !chainNew[x]:
						// engine.New as a value (reflect.ValueOf(engine.New),
						// a function variable) escapes the chain check.
						t.Errorf("%s: references engine.New outside the one engine.New(...).WithMaxSteps(...).Run(...) chain", fset.Position(x.Pos()))
					}
				}
				// reflect can call any engine method by a computed name, so
				// only reflect.DeepEqual (Equal) is allowed.
				if id, ok := x.X.(*ast.Ident); ok && reflectNames[id.Name] && x.Sel.Name != "DeepEqual" {
					t.Errorf("%s: uses reflect.%s; only reflect.DeepEqual is allowed", fset.Position(x.Pos()), x.Sel.Name)
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no source files checked")
	}
	for path, names := range localNames {
		if len(names) > 1 {
			t.Errorf("%s is imported under %d local names %v; each import path takes one name across the package", path, len(names), names)
		}
	}
	if news != 1 || runs != 1 {
		t.Errorf("found %d engine.New calls and %d engine.New(...)…Run(...) chains; want exactly one of each, in (*Program).Run", news, runs)
	}
}

func receiverIs(expr ast.Expr, name string) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

// chainRoot returns the engine.New selector a rootedAtEngineNew chain
// starts from.
func chainRoot(expr ast.Expr) *ast.SelectorExpr {
	call := expr.(*ast.CallExpr)
	sel := call.Fun.(*ast.SelectorExpr)
	if sel.Sel.Name == "New" {
		return sel
	}
	return chainRoot(sel.X)
}

// defaultName is the name an unaliased import of path binds. Every allowed
// package's name is the last element of its path.
func defaultName(path string) string {
	return path[strings.LastIndex(path, "/")+1:]
}

func isEngineNew(call *ast.CallExpr, engineNames map[string]bool) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "New" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && engineNames[id.Name]
}

// rootedAtEngineNew reports whether expr is engine.New(...) followed only
// by WithMaxSteps(...) calls.
func rootedAtEngineNew(expr ast.Expr, engineNames map[string]bool) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	if isEngineNew(call, engineNames) {
		return true
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "WithMaxSteps" && rootedAtEngineNew(sel.X, engineNames)
}

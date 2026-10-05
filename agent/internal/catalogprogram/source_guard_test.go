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
// chain, in (*Program).Run. Any other engine method (WithObserver,
// RunObserved, RunObservedPending, RunJournaled, EmitRunTerminal,
// RunControl, or one added later) — called, taken as a method value, or
// named in a string for reflection — turns this test red, whatever the
// exported surface looks like.
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
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		engineName := ""
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if !allowedImports[path] {
				t.Errorf("%s imports %s; this package may not observe, journal or persist a run", name, path)
			}
			if path == enginePath {
				engineName = "engine"
				if spec.Name != nil {
					engineName = spec.Name.Name
				}
			}
			if spec.Name != nil && (spec.Name.Name == "." || spec.Name.Name == "_") {
				t.Errorf("%s imports %s as %q", name, path, spec.Name.Name)
			}
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
				if id, ok := x.X.(*ast.Ident); ok && engineName != "" && id.Name == engineName && x.Sel.Name != "New" {
					t.Errorf("%s: uses engine.%s; only engine.New is allowed", fset.Position(x.Pos()), x.Sel.Name)
				}
			}
			return true
		})
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
				if isEngineNew(call, engineName) {
					news++
					if !inRun {
						t.Errorf("%s: engine.New outside (*Program).Run", fset.Position(call.Pos()))
					}
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Run" && rootedAtEngineNew(sel.X, engineName) {
					runs++
					if !inRun {
						t.Errorf("%s: engine run outside (*Program).Run", fset.Position(call.Pos()))
					}
				}
				return true
			})
		}
	}
	if checked == 0 {
		t.Fatal("no source files checked")
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

func isEngineNew(call *ast.CallExpr, engineName string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "New" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && engineName != "" && id.Name == engineName
}

// rootedAtEngineNew reports whether expr is engine.New(...) followed only
// by WithMaxSteps(...) calls.
func rootedAtEngineNew(expr ast.Expr, engineName string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	if isEngineNew(call, engineName) {
		return true
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "WithMaxSteps" && rootedAtEngineNew(sel.X, engineName)
}

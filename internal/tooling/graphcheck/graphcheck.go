// Package graphcheck inspects Go import graphs without loading or executing
// packages. It is intentionally a source tool, not a runtime discovery path.
package graphcheck

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/internal/tooling/layout"
)

type Diagnostic struct {
	Code    string `json:"code"`
	Package string `json:"package"`
	Import  string `json:"import,omitempty"`
	Message string `json:"message"`
}

func (d Diagnostic) Error() string { return fmt.Sprintf("%s: %s: %s", d.Code, d.Package, d.Message) }

type Package struct {
	ImportPath string
	Dir        string
	Imports    []string
	// Source reports whether the directory has non-test Go files.
	Source bool
	// Declaration reports a top-level variable named Declaration of type
	// <module>/trigger.Declaration.
	Declaration bool
	// RunsTriggerConformance reports whether a test file calls
	// contract/conformance.RunTrigger.
	RunsTriggerConformance bool
}

type Graph struct {
	Module   string
	Packages map[string]Package
	// Root is the directory Analyze read; node ownership is anchored here.
	Root string
}

func Analyze(root string) (Graph, error) {
	module, err := modulePath(filepath.Join(root, "go.mod"))
	if err != nil {
		return Graph{}, err
	}
	g := Graph{Module: module, Packages: map[string]Package{}, Root: root}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			// testdata is ignored by the go tool; fixture modules inside it
			// are analyzed only when passed as the root.
			if path != root && (info.Name() == "vendor" || info.Name() == "testdata" || strings.HasPrefix(info.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkgPath := module
		if rel != "." {
			pkgPath += "/" + filepath.ToSlash(rel)
		}
		p := g.Packages[pkgPath]
		p.ImportPath, p.Dir = pkgPath, filepath.Dir(path)
		if strings.HasSuffix(path, "_test.go") {
			p.RunsTriggerConformance = p.RunsTriggerConformance || callsRunTrigger(file, module+"/contract/conformance")
			g.Packages[pkgPath] = p
			return nil
		}
		p.Source = true
		for _, spec := range file.Imports {
			p.Imports = append(p.Imports, strings.Trim(spec.Path.Value, `"`))
		}
		p.Declaration = p.Declaration || declaresContract(file, module+"/trigger")
		g.Packages[pkgPath] = p
		return nil
	})
	if err != nil {
		return Graph{}, err
	}
	for path, p := range g.Packages {
		sort.Strings(p.Imports)
		g.Packages[path] = p
	}
	return g, nil
}

func Check(root string) ([]Diagnostic, error) {
	g, err := Analyze(root)
	if err != nil {
		return nil, err
	}
	var diagnostics []Diagnostic
	for path, p := range g.Packages {
		if isEngine(path, p.Dir) {
			for _, imported := range p.Imports {
				if forbiddenEngineImport(imported) {
					diagnostics = append(diagnostics, Diagnostic{Code: "engine_import_forbidden", Package: path, Import: imported, Message: "engine package imports an adapter, store, provider or product package"})
				}
				if external(g.Module, imported) {
					diagnostics = append(diagnostics, Diagnostic{Code: "engine_external_import_forbidden", Package: path, Import: imported, Message: "engine packages import only the standard library and this module; broker, store and provider clients belong in adapters"})
				}
				if reachesForbidden(g, imported, map[string]bool{}) {
					diagnostics = append(diagnostics, Diagnostic{Code: "engine_transitive_import_forbidden", Package: path, Import: imported, Message: "engine dependency transitively reaches a forbidden package"})
				}
			}
		}
		if isTrigger(g.Module, path) {
			for _, imported := range p.Imports {
				if forbiddenTriggerImport(g.Module, imported) {
					diagnostics = append(diagnostics, Diagnostic{Code: "trigger_import_forbidden", Package: path, Import: imported, Message: "trigger adapter imports the interpreter, compiler or journal; adapters dispatch through an injected handler"})
				} else if reachesForbiddenTrigger(g, imported, map[string]bool{}) {
					diagnostics = append(diagnostics, Diagnostic{Code: "trigger_transitive_import_forbidden", Package: path, Import: imported, Message: "trigger adapter dependency transitively reaches the interpreter, compiler or journal"})
				}
			}
		}
		if p.Source && isTriggerAdapter(g.Module, path) {
			if !p.Declaration {
				diagnostics = append(diagnostics, Diagnostic{Code: "trigger_declaration_missing", Package: path, Message: "trigger adapter must publish var Declaration of type trigger.Declaration stating its completion and disconnect behavior"})
			}
			if !p.RunsTriggerConformance {
				diagnostics = append(diagnostics, Diagnostic{Code: "trigger_conformance_missing", Package: path, Message: "trigger adapter tests must run contract/conformance.RunTrigger"})
			}
		}
		if nodeRoot := nodeOwnerRoot(g, p.Dir); nodeRoot != "" {
			for _, imported := range p.Imports {
				if importedRoot, ok := packageNodeRoot(g, imported); ok && importedRoot != nodeRoot {
					diagnostics = append(diagnostics, Diagnostic{Code: "node_import_forbidden", Package: path, Import: imported, Message: "a node cannot import another node; workflows own composition"})
				}
			}
		}
	}
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Code != diagnostics[j].Code {
			return diagnostics[i].Code < diagnostics[j].Code
		}
		if diagnostics[i].Package != diagnostics[j].Package {
			return diagnostics[i].Package < diagnostics[j].Package
		}
		return diagnostics[i].Import < diagnostics[j].Import
	})
	return diagnostics, nil
}

func modulePath(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 2 && fields[0] == "module" {
			return fields[1], nil
		}
	}
	if err := s.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("go.mod has no module directive")
}

func isEngine(path, dir string) bool {
	return path == "engine" || strings.HasSuffix(path, "/engine") || filepath.Base(dir) == "engine"
}

// external reports an import outside the standard library and this module.
// Standard library paths have no dot in their first element.
func external(module, path string) bool {
	if path == module || strings.HasPrefix(path, module+"/") {
		return false
	}
	first, _, _ := strings.Cut(path, "/")
	return strings.Contains(first, ".")
}

func forbiddenEngineImport(path string) bool {
	parts := strings.Split(path, "/")
	for _, part := range parts {
		switch part {
		case "trigger", "triggers", "store", "stores", "provider", "providers", "ui", "frontend":
			return true
		}
	}
	return strings.HasPrefix(path, "database/sql") || strings.HasPrefix(path, "net/http")
}

func reachesForbidden(g Graph, path string, seen map[string]bool) bool {
	if forbiddenEngineImport(path) || external(g.Module, path) {
		return true
	}
	if seen[path] {
		return false
	}
	seen[path] = true
	p, ok := g.Packages[path]
	if !ok {
		return false
	}
	for _, imported := range p.Imports {
		if reachesForbidden(g, imported, seen) {
			return true
		}
	}
	return false
}

func isTrigger(module, path string) bool {
	rel := strings.TrimPrefix(path, module+"/")
	return rel != path && (rel == "trigger" || strings.HasPrefix(rel, "trigger/"))
}

// isTriggerAdapter matches every package under trigger/ except the contract
// package itself and internal helpers: each one is an adapter and must
// declare its behavior and run conformance.
func isTriggerAdapter(module, path string) bool {
	rel := strings.TrimPrefix(path, module+"/")
	if rel == path || !strings.HasPrefix(rel, "trigger/") {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "internal" {
			return false
		}
	}
	return true
}

// importName returns the name file uses for importPath: its alias, "." for a
// dot import, the default last element, or "" when it is not imported.
func importName(file *ast.File, importPath string) string {
	for _, spec := range file.Imports {
		if strings.Trim(spec.Path.Value, `"`) != importPath {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return importPath[strings.LastIndex(importPath, "/")+1:]
	}
	return ""
}

// refers reports whether expr names symbol from the package imported as name.
func refers(expr ast.Expr, name, symbol string) bool {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := e.X.(*ast.Ident)
		return ok && ident.Name == name && e.Sel.Name == symbol
	case *ast.Ident:
		return name == "." && e.Name == symbol
	}
	return false
}

// declaresContract reports a top-level `var Declaration` whose declared type
// or composite literal is the trigger package's Declaration.
func declaresContract(file *ast.File, triggerPath string) bool {
	name := importName(file, triggerPath)
	if name == "" {
		return false
	}
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			for index, ident := range value.Names {
				if ident.Name != "Declaration" {
					continue
				}
				if value.Type != nil && refers(value.Type, name, "Declaration") {
					return true
				}
				if index < len(value.Values) {
					if literal, ok := value.Values[index].(*ast.CompositeLit); ok && refers(literal.Type, name, "Declaration") {
						return true
					}
				}
			}
		}
	}
	return false
}

func forbiddenTriggerImport(module, path string) bool {
	for _, owned := range []string{"/internal/engine", "/internal/compile", "/internal/program", "/internal/journal", "/flowtest"} {
		if path == module+owned || strings.HasPrefix(path, module+owned+"/") {
			return true
		}
	}
	return false
}

func reachesForbiddenTrigger(g Graph, path string, seen map[string]bool) bool {
	if forbiddenTriggerImport(g.Module, path) {
		return true
	}
	if seen[path] {
		return false
	}
	seen[path] = true
	p, ok := g.Packages[path]
	if !ok {
		return false
	}
	for _, imported := range p.Imports {
		if reachesForbiddenTrigger(g, imported, seen) {
			return true
		}
	}
	return false
}

// callsRunTrigger reports whether file imports conformancePath and calls its
// RunTrigger function. Merely referring to the function does not count.
func callsRunTrigger(file *ast.File, conformancePath string) bool {
	name := importName(file, conformancePath)
	if name == "" {
		return false
	}
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && refers(call.Fun, name, "RunTrigger") {
			found = true
		}
		return !found
	})
	return found
}

// nodeOwnerRoot is the node directory that owns dir, relative to the
// analyzed root, under either layout (internal/tooling/layout.NodeRoot).
// Every package nested below a node directory belongs to that node.
func nodeOwnerRoot(g Graph, dir string) string {
	rel, err := filepath.Rel(g.Root, dir)
	if err != nil {
		return ""
	}
	root, _ := layout.NodeRoot(filepath.ToSlash(rel))
	return root
}

func packageNodeRoot(g Graph, importPath string) (string, bool) {
	p, ok := g.Packages[importPath]
	if !ok {
		return "", false
	}
	root := nodeOwnerRoot(g, p.Dir)
	return root, root != ""
}

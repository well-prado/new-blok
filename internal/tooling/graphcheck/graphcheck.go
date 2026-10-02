// Package graphcheck inspects Go import graphs without loading or executing
// packages. It is intentionally a source tool, not a runtime discovery path.
package graphcheck

import (
	"bufio"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
}

type Graph struct {
	Module   string
	Packages map[string]Package
}

func Analyze(root string) (Graph, error) {
	module, err := modulePath(filepath.Join(root, "go.mod"))
	if err != nil {
		return Graph{}, err
	}
	g := Graph{Module: module, Packages: map[string]Package{}}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			if path != root && (info.Name() == "vendor" || strings.HasPrefix(info.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
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
		for _, spec := range file.Imports {
			p.Imports = append(p.Imports, strings.Trim(spec.Path.Value, `"`))
		}
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
				if reachesForbidden(g, imported, map[string]bool{}) {
					diagnostics = append(diagnostics, Diagnostic{Code: "engine_transitive_import_forbidden", Package: path, Import: imported, Message: "engine dependency transitively reaches a forbidden package"})
				}
			}
		}
		if nodeRoot := nodeOwnerRoot(p.Dir); nodeRoot != "" {
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
	if forbiddenEngineImport(path) {
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

func nodeOwnerRoot(dir string) string {
	parts := strings.Split(filepath.ToSlash(dir), "/")
	for i, part := range parts {
		if part != "nodes" || i+1 >= len(parts) {
			continue
		}
		// Unified: nodes/<runtime>/<node>; classic: runtimes/<runtime>/nodes/<node>.
		if i+2 < len(parts) {
			return strings.Join(parts[:i+3], "/")
		}
		return strings.Join(parts[:i+2], "/")
	}
	return ""
}

func packageNodeRoot(g Graph, importPath string) (string, bool) {
	p, ok := g.Packages[importPath]
	if !ok {
		return "", false
	}
	root := nodeOwnerRoot(p.Dir)
	return root, root != ""
}

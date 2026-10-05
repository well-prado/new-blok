package ownership

import (
	"bufio"
	"bytes"
	"errors"
	"go/parser"
	"go/scanner"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// goAdapter resolves Go imports with go/parser and module-path resolution,
// as internal/tooling/graphcheck does, anchored at the project root with
// layout.NodeRoot. A unit is a package directory. An import path inside the
// application module (or a go.mod replace directive pointing at a local
// directory) resolves to that directory; the standard library and other
// modules are external.
type goAdapter struct {
	files    *projectFiles
	module   string
	replaces []replacement // longest prefix first
	modErr   *diagnostic.Diagnostic
}

type replacement struct {
	module, dir string
	outside     bool
}

func newGoAdapter(files *projectFiles, project *layout.Project) *goAdapter {
	a := &goAdapter{files: files, module: project.Module}
	if data, problem := files.read("go.mod"); problem == nil {
		a.replaces, a.modErr = localReplacements(data)
	}
	return a
}

func (a *goAdapter) Runtime() string { return layout.GoRuntime }

// Roots are the node's package directories: each directory holding a
// non-test Go file the node owns.
func (a *goAdapter) Roots(node layout.Node) []string {
	seen := map[string]bool{}
	var roots []string
	for _, file := range node.Files {
		if !goSourceFile(path.Base(file)) {
			continue
		}
		dir := path.Dir(path.Join(node.Dir, file))
		if !seen[dir] {
			seen[dir] = true
			roots = append(roots, dir)
		}
	}
	sort.Strings(roots)
	return roots
}

func goSourceFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") && !strings.HasPrefix(name, ".") && !strings.HasPrefix(name, "_")
}

// Analyze parses every non-test Go file of the package directory. Imports
// in build-constrained files count like any other (ADR 0023).
func (a *goAdapter) Analyze(dir string) Analysis {
	var analysis Analysis
	if a.modErr != nil {
		analysis.Findings = append(analysis.Findings, *a.modErr)
	}
	l := a.files.list(dir)
	names := make([]string, 0, len(l.entries))
	for name, mode := range l.entries {
		if mode.IsRegular() && goSourceFile(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		rel := path.Join(dir, name)
		data, problem := a.files.read(rel)
		if problem != nil {
			analysis.Findings = append(analysis.Findings, *problem)
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, rel, data, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			line := 0
			var list scanner.ErrorList
			if errors.As(err, &list) && len(list) > 0 {
				line = list[0].Pos.Line
			}
			analysis.Findings = append(analysis.Findings, finding(CodeParseFailed, at(rel, line), "", "Go syntax error; the file's imports cannot be read"))
			continue
		}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if strings.HasPrefix(comment.Text, "//go:linkname") {
					analysis.Findings = append(analysis.Findings, finding(CodeUnsupportedForm, at(rel, fileSet.Position(comment.Pos()).Line), "//go:linkname", "//go:linkname binds a symbol of any linked package without an import; it cannot be verified"))
				}
			}
		}
		if len(file.Imports) > maxEdgesPerFile {
			analysis.Findings = append(analysis.Findings, finding(CodeLimitExceeded, rel, strconv.Itoa(len(file.Imports)), "the file has more imports than the ownership check resolves"))
			continue
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			source := at(rel, fileSet.Position(spec.Pos()).Line)
			if importPath == "plugin" {
				analysis.Findings = append(analysis.Findings, finding(CodeUnsupportedForm, source, "plugin", "package plugin loads code by a run-time path; it cannot be verified"))
				continue
			}
			if edge, ok := a.resolve(importPath, source); ok {
				analysis.Edges = append(analysis.Edges, edge.edge)
				analysis.Findings = append(analysis.Findings, edge.findings...)
			}
		}
	}
	return analysis
}

type goEdge struct {
	edge     Edge
	findings []diagnostic.Diagnostic
}

// resolve maps an import path to a project directory, or reports false for
// the standard library, cgo and other modules.
func (a *goAdapter) resolve(importPath, source string) (goEdge, bool) {
	dir, base, local, outside := a.localDir(importPath)
	if !local {
		return goEdge{}, false
	}
	out := goEdge{edge: Edge{Source: source, Specifier: importPath}}
	if outside {
		out.findings = append(out.findings, finding(CodeImportOutsideRoot, source, importPath, "a go.mod replace directive maps this import outside the project root"))
		return out, true
	}
	var r resolved
	e := a.files.lookup(dir)
	switch {
	case e.kind == kindDir && !e.caseMismatch:
		if !a.hasGoFiles(e.path) {
			out.findings = append(out.findings, finding(CodeImportUnresolved, source, importPath, "the import names a directory with no non-test Go files"))
			break
		}
		if nested := a.nestedModule(base, e.path); nested != "" {
			out.findings = append(out.findings, finding(CodeImportUnresolved, source, importPath, "the directory belongs to the nested module at "+nested+", not to the application module"))
			break
		}
		r.target(e.path, false)
	case r.accept(e, source):
	default:
		out.findings = append(out.findings, finding(CodeImportUnresolved, source, importPath, "no package directory in the project matches this module import path"))
	}
	out.edge.Targets = r.targets
	out.findings = append(out.findings, r.findings...)
	return out, true
}

// localDir maps an import path to a project-relative directory when it
// belongs to the application module or a local replacement; base is that
// module's root directory.
func (a *goAdapter) localDir(importPath string) (dir, base string, local, outside bool) {
	for _, replace := range a.replaces {
		if rest, ok := strings.CutPrefix(importPath, replace.module); ok && (rest == "" || strings.HasPrefix(rest, "/")) {
			if replace.outside {
				return "", "", true, true
			}
			dir, inside := cleanJoin(replace.dir, strings.TrimPrefix(rest, "/"))
			return dir, replace.dir, true, !inside
		}
	}
	if a.module == "" {
		return "", "", false, false
	}
	if importPath == a.module {
		return "", "", true, false
	}
	if rest, ok := strings.CutPrefix(importPath, a.module+"/"); ok {
		return rest, "", true, false
	}
	return "", "", false, false
}

func (a *goAdapter) hasGoFiles(dir string) bool {
	for name, mode := range a.files.list(dir).entries {
		if mode.IsRegular() && goSourceFile(name) {
			return true
		}
	}
	return false
}

// nestedModule returns the directory of a go.mod between the module root
// base (exclusive) and dir (inclusive), or "".
func (a *goAdapter) nestedModule(base, dir string) string {
	parts := strings.Split(dir, "/")
	for i := len(parts); i >= 1; i-- {
		candidate := strings.Join(parts[:i], "/")
		if candidate == base || !strings.HasPrefix(candidate+"/", base+"/") && base != "" {
			break
		}
		if mode, ok := a.files.list(candidate).entries["go.mod"]; ok && mode.IsRegular() {
			return candidate
		}
	}
	return ""
}

// localReplacements reads go.mod replace directives whose target is a
// local directory ("./x" or "../x"). Module-to-module replacements stay
// external.
func localReplacements(data []byte) ([]replacement, *diagnostic.Diagnostic) {
	var out []replacement
	inBlock := false
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if comment := strings.Index(text, "//"); comment >= 0 {
			text = strings.TrimSpace(text[:comment])
		}
		switch {
		case text == "replace (" || text == "replace(":
			inBlock = true
			continue
		case inBlock && text == ")":
			inBlock = false
			continue
		case strings.HasPrefix(text, "replace ") || strings.HasPrefix(text, "replace\t"):
			text = strings.TrimSpace(text[len("replace"):])
		case !inBlock:
			continue
		}
		left, right, found := strings.Cut(text, "=>")
		if !found {
			continue
		}
		oldFields, newFields := strings.Fields(left), strings.Fields(right)
		if len(oldFields) == 0 || len(newFields) == 0 {
			d := finding(CodeConfigInvalid, at("go.mod", line), text, "go.mod replace directive is malformed")
			return nil, &d
		}
		target := unquote(newFields[0])
		if !strings.HasPrefix(target, "./") && !strings.HasPrefix(target, "../") && target != "." && target != ".." {
			continue
		}
		dir, inside := cleanJoin("", target)
		out = append(out, replacement{module: unquote(oldFields[0]), dir: dir, outside: !inside})
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].module) > len(out[j].module) })
	return out, nil
}

func unquote(s string) string {
	if value, err := strconv.Unquote(s); err == nil {
		return value
	}
	return s
}

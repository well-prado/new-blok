package layout

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
)

// definition is one static node.Define or flow.Define call.
type definition struct {
	name, version string
	source        string // project-relative file:line
}

// goSource is the syntax discovery reads from one Go package directory's
// files. Nothing here type-checks, builds or runs the package.
type goSource struct {
	fileSet *token.FileSet
	files   map[string]*ast.File // project-relative file path → syntax
}

func newGoSource() goSource {
	return goSource{fileSet: token.NewFileSet(), files: map[string]*ast.File{}}
}

// parse adds one file's syntax. SkipObjectResolution: discovery needs
// syntax only, never types or package loading.
func (s goSource) parse(rel string, data []byte) error {
	file, err := parser.ParseFile(s.fileSet, rel, data, parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	s.files[rel] = file
	return nil
}

// definitions finds every static node.Define/MustDefine and
// flow.Define/MustDefine call in the files. A call whose identity is not a
// string literal or a package-level string constant is reported, not
// guessed: guessing would make identity depend on something other than the
// descriptor.
func (s goSource) definitions(c *collector) (nodes, flows []definition) {
	byDir := map[string][]string{}
	for rel := range s.files {
		byDir[path.Dir(rel)] = append(byDir[path.Dir(rel)], rel)
	}
	dirs := make([]string, 0, len(byDir))
	for dir := range byDir {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		rels := byDir[dir]
		sort.Strings(rels)
		consts := map[string]string{}
		for _, rel := range rels {
			packageConstants(s.files[rel], consts)
		}
		for _, rel := range rels {
			file := s.files[rel]
			nodeName, flowName := importName(file, nodePackage), importName(file, flowPackage)
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				fun := call.Fun
				switch indexed := fun.(type) {
				case *ast.IndexExpr:
					fun = indexed.X
				case *ast.IndexListExpr:
					fun = indexed.X
				}
				at := fmt.Sprintf("%s:%d", rel, s.fileSet.Position(call.Pos()).Line)
				switch {
				case nodeName != "" && (refers(fun, nodeName, "Define") || refers(fun, nodeName, "MustDefine")):
					if len(call.Args) < 2 {
						c.add(CodeDescriptorNotStatic, at, "name and version arguments", "", "node.Define call has no name and version")
						return true
					}
					name, okName := staticString(call.Args[0], consts)
					version, okVersion := staticString(call.Args[1], consts)
					if !okName || !okVersion {
						c.add(CodeDescriptorNotStatic, at, "string literal or package-level string constant", "", "node identity is computed at run time; discovery cannot read it without executing source")
						return true
					}
					nodes = append(nodes, definition{name: name, version: version, source: at})
				case flowName != "" && (refers(fun, flowName, "Define") || refers(fun, flowName, "MustDefine")):
					name, version, ok := flowSpec(call, flowName, consts)
					if !ok {
						c.add(CodeDescriptorNotStatic, at, "flow.Spec literal with static Name and Version", "", "workflow identity is computed at run time; discovery cannot read it without executing source")
						return true
					}
					flows = append(flows, definition{name: name, version: version, source: at})
				}
				return true
			})
		}
	}
	return nodes, flows
}

func packageConstants(file *ast.File, consts map[string]string) {
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			if len(value.Values) != len(value.Names) {
				continue
			}
			for i, ident := range value.Names {
				if literal, ok := value.Values[i].(*ast.BasicLit); ok && literal.Kind == token.STRING {
					if text, err := strconv.Unquote(literal.Value); err == nil {
						consts[ident.Name] = text
					}
				}
			}
		}
	}
}

func staticString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		text, err := strconv.Unquote(e.Value)
		return text, err == nil
	case *ast.Ident:
		text, ok := consts[e.Name]
		return text, ok
	case *ast.ParenExpr:
		return staticString(e.X, consts)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, okLeft := staticString(e.X, consts)
		right, okRight := staticString(e.Y, consts)
		return left + right, okLeft && okRight
	}
	return "", false
}

// flowSpec reads Name and Version from a flow.Spec composite literal, keyed
// or positional, passed as the call's first argument.
func flowSpec(call *ast.CallExpr, flowName string, consts map[string]string) (string, string, bool) {
	if len(call.Args) == 0 {
		return "", "", false
	}
	literal, ok := call.Args[0].(*ast.CompositeLit)
	if !ok || !refers(literal.Type, flowName, "Spec") {
		return "", "", false
	}
	var name, version string
	var okName, okVersion bool
	for index, element := range literal.Elts {
		if pair, keyed := element.(*ast.KeyValueExpr); keyed {
			key, _ := pair.Key.(*ast.Ident)
			switch {
			case key != nil && key.Name == "Name":
				name, okName = staticString(pair.Value, consts)
			case key != nil && key.Name == "Version":
				version, okVersion = staticString(pair.Value, consts)
			}
			continue
		}
		switch index {
		case 0:
			name, okName = staticString(element, consts)
		case 1:
			version, okVersion = staticString(element, consts)
		}
	}
	return name, version, okName && okVersion
}

// importName returns the name file uses for importPath: its alias, "." for
// a dot import, the default last element, or "" when it is not imported or
// imported blank.
func importName(file *ast.File, importPath string) string {
	for _, spec := range file.Imports {
		if strings.Trim(spec.Path.Value, `"`) != importPath {
			continue
		}
		if spec.Name != nil {
			if spec.Name.Name == "_" {
				continue // a blank import names nothing; another spec may
			}
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

func imports(file *ast.File) []string {
	out := make([]string, 0, len(file.Imports))
	for _, spec := range file.Imports {
		if value, err := strconv.Unquote(spec.Path.Value); err == nil {
			out = append(out, value)
		}
	}
	return out
}

// foreignDescriptor is the identity part of a foreign node's node.json. The
// rest of the node.Descriptor document (schemas, effects) is validated by
// the runtime catalog, not by discovery.
type foreignDescriptor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Runtime string `json:"runtime,omitempty"`
}

func parseForeignDescriptor(data []byte) (foreignDescriptor, error) {
	var descriptor foreignDescriptor
	if err := json.Unmarshal(data, &descriptor); err != nil {
		return foreignDescriptor{}, err
	}
	return descriptor, nil
}

package layout

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
)

// definition is one accepted static node.Define or flow.Define call.
type definition struct {
	name, version string
	source        string // project-relative file:line
}

// part is one operand of a static string: a literal, or a package-level
// constant to resolve once the whole package's constants are known.
type part struct {
	literal string
	ident   string
}

// static is a string expression discovery can evaluate without running
// anything: a string literal, a package-level constant, or a "+" of those.
// ok is false for anything else.
type static struct {
	parts []part
	ok    bool
}

// call is one node.Define/MustDefine or flow.Define/MustDefine call site.
type call struct {
	flow     bool
	line     int
	accepted bool // in an accepted position (see acceptedCalls)
	inFunc   bool // a constructor's direct return: literals only
	name     static
	version  static
}

// fileSummary is everything discovery keeps from one Go file. The syntax
// tree is discarded as soon as the summary is taken, so memory held across
// a discovery is proportional to declarations found, not to source size.
type fileSummary struct {
	pkg         string
	constrained string // why the file is build-constrained, or ""
	imports     []string
	constants   map[string]static
	calls       []call
}

// goSource summarizes the Go files of node directories and workflow paths.
// Nothing here type-checks, builds or runs a package.
type goSource struct {
	files map[string]*fileSummary // project-relative path → summary
}

func newGoSource() goSource { return goSource{files: map[string]*fileSummary{}} }

// errSyntax is a parse failure. Its text names only a line: go/parser
// errors quote the offending token, which would echo file content into a
// diagnostic.
type errSyntax struct{ line int }

func (e errSyntax) Error() string {
	if e.line > 0 {
		return fmt.Sprintf("Go syntax error at line %d", e.line)
	}
	return "Go syntax error"
}

// parse summarizes one file. SkipObjectResolution: discovery needs syntax
// only, never types or package loading.
func (s goSource) parse(rel string, data []byte) error {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, rel, data, parser.SkipObjectResolution|parser.ParseComments)
	if err != nil {
		var list scanner.ErrorList
		if errors.As(err, &list) && len(list) > 0 {
			return errSyntax{line: list[0].Pos.Line}
		}
		return errSyntax{}
	}
	summary := &fileSummary{pkg: file.Name.Name, constrained: constraint(rel, file), constants: map[string]static{}}
	for _, spec := range file.Imports {
		if value, err := strconv.Unquote(spec.Path.Value); err == nil {
			summary.imports = append(summary.imports, value)
			if value == "C" && summary.constrained == "" {
				summary.constrained = "cgo"
			}
		}
	}
	packageConstants(file, summary)
	summary.calls = calls(file, fileSet)
	s.files[rel] = summary
	return nil
}

// imports returns each file's imports.
func (s goSource) imports() map[string][]string {
	out := make(map[string][]string, len(s.files))
	for rel, summary := range s.files {
		out[rel] = summary.imports
	}
	return out
}

// definitions resolves every Define call in the summarized files. Only an
// accepted call in an unconstrained file of a single-package directory, with
// a statically resolvable identity, becomes a definition; every other call
// is reported, never guessed, because a guess could make discovery's
// identity differ from the one the built program registers.
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
		packages := map[string][]string{}
		for _, rel := range rels {
			if s.files[rel].constrained == "" {
				packages[s.files[rel].pkg] = append(packages[s.files[rel].pkg], rel)
			}
		}
		if len(packages) > 1 {
			names := make([]string, 0, len(packages))
			for name := range packages {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, rel := range rels {
				if summary := s.files[rel]; summary.constrained == "" && hasCalls(summary) {
					c.add(CodePackageMismatch, rel, "one package clause per directory", strings.Join(names, ","), "the directory's files declare different packages; the go tool would refuse to build it")
				}
			}
			continue
		}
		// Constants: declared once, in an unconstrained file.
		declared := map[string]int{}
		constrainedConst := map[string]bool{}
		values := map[string]static{}
		for _, rel := range rels {
			summary := s.files[rel]
			for name, value := range summary.constants {
				if summary.constrained != "" {
					constrainedConst[name] = true
					continue
				}
				declared[name]++
				values[name] = value
			}
		}
		resolve := func(value static, literalsOnly bool) (string, string) {
			if !value.ok {
				return "", "not a string literal or package-level string constant"
			}
			var out strings.Builder
			for _, p := range value.parts {
				if p.ident == "" {
					out.WriteString(p.literal)
					continue
				}
				switch {
				case literalsOnly:
					return "", "a constructor's Define must use string literals: " + p.ident + " may be a parameter or local"
				case constrainedConst[p.ident]:
					return "", "constant " + p.ident + " is declared in a build-constrained file; its value depends on the build"
				case declared[p.ident] > 1:
					return "", fmt.Sprintf("constant %s is declared %d times", p.ident, declared[p.ident])
				case declared[p.ident] == 0:
					return "", p.ident + " is not a package-level string constant"
				}
				inner := values[p.ident]
				for _, q := range inner.parts {
					if q.ident != "" || !inner.ok {
						return "", "constant " + p.ident + " is not a plain string literal"
					}
					out.WriteString(q.literal)
				}
			}
			return out.String(), ""
		}
		for _, rel := range rels {
			summary := s.files[rel]
			for _, call := range summary.calls {
				at := fmt.Sprintf("%s:%d", rel, call.line)
				kind := "node"
				if call.flow {
					kind = "workflow"
				}
				switch {
				case summary.constrained != "":
					c.add(CodeDescriptorConstrained, at, "an unconstrained file", summary.constrained, "a "+kind+" is defined in a build-constrained file; which identity exists depends on the build")
					continue
				case !call.accepted:
					c.add(CodeDescriptorNotStatic, at, "package-level var X = Define(...) or a constructor's direct return", "", "a "+kind+" Define call sits where discovery cannot know it runs, or runs once")
					continue
				}
				name, whyName := resolve(call.name, call.inFunc)
				version, whyVersion := resolve(call.version, call.inFunc)
				if why := whyName + whyVersion; why != "" {
					c.add(CodeDescriptorNotStatic, at, "string literal or package-level string constant", "", kind+" identity is not static: "+why)
					continue
				}
				found := definition{name: name, version: version, source: at}
				if call.flow {
					flows = append(flows, found)
				} else {
					nodes = append(nodes, found)
				}
			}
		}
	}
	return nodes, flows
}

func hasCalls(summary *fileSummary) bool { return len(summary.calls) > 0 }

// goosList and goarchList mirror go/build's known operating systems and
// architectures (Go 1.27), for file-name build constraints.
var (
	goosList   = setOf("aix android darwin dragonfly freebsd hurd illumos ios js linux nacl netbsd openbsd plan9 solaris wasip1 windows zos")
	goarchList = setOf("386 amd64 amd64p32 arm armbe arm64 arm64be loong64 mips mipsle mips64 mips64le mips64p32 mips64p32le ppc ppc64 ppc64le riscv riscv64 s390 s390x sparc sparc64 wasm")
)

func setOf(words string) map[string]bool {
	out := map[string]bool{}
	for _, word := range strings.Fields(words) {
		out[word] = true
	}
	return out
}

// constraint reports why a file is included only in some builds: a
// //go:build or // +build line before the package clause, or a
// _GOOS/_GOARCH file-name suffix, following go/build's rules.
func constraint(rel string, file *ast.File) string {
	for _, group := range file.Comments {
		if group.End() >= file.Package {
			break
		}
		for _, comment := range group.List {
			if strings.HasPrefix(comment.Text, "//go:build") || strings.HasPrefix(comment.Text, "// +build") {
				return "build constraint " + strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
			}
		}
	}
	name := strings.TrimSuffix(path.Base(rel), ".go")
	index := strings.Index(name, "_")
	if index < 0 {
		return ""
	}
	elements := strings.Split(name[index:], "_")
	if n := len(elements); n >= 2 && goosList[elements[n-2]] && goarchList[elements[n-1]] {
		return "file name " + elements[n-2] + "_" + elements[n-1]
	}
	if last := elements[len(elements)-1]; goosList[last] || goarchList[last] {
		return "file name _" + last
	}
	return ""
}

// acceptedCalls finds the two call positions whose execution is certain and
// single: the whole initializer of a package-level var, and the result of a
// top-level function's direct return statement (the constructor form blok
// new writes).
func acceptedCalls(file *ast.File) (packageVars, constructors map[*ast.CallExpr]bool) {
	packageVars, constructors = map[*ast.CallExpr]bool{}, map[*ast.CallExpr]bool{}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if d.Tok != token.VAR {
				continue
			}
			for _, spec := range d.Specs {
				for _, value := range spec.(*ast.ValueSpec).Values {
					if call, ok := value.(*ast.CallExpr); ok {
						packageVars[call] = true
					}
				}
			}
		case *ast.FuncDecl:
			if d.Recv != nil || d.Body == nil {
				continue
			}
			for _, statement := range d.Body.List {
				if ret, ok := statement.(*ast.ReturnStmt); ok && len(ret.Results) > 0 {
					if call, ok := ret.Results[0].(*ast.CallExpr); ok {
						constructors[call] = true
					}
				}
			}
		}
	}
	return packageVars, constructors
}

func calls(file *ast.File, fileSet *token.FileSet) []call {
	nodeName, flowName := importName(file, nodePackage), importName(file, flowPackage)
	if nodeName == "" && flowName == "" {
		return nil
	}
	packageVars, constructors := acceptedCalls(file)
	var out []call
	ast.Inspect(file, func(n ast.Node) bool {
		expr, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		fun := expr.Fun
		switch indexed := fun.(type) {
		case *ast.IndexExpr:
			fun = indexed.X
		case *ast.IndexListExpr:
			fun = indexed.X
		}
		found := call{line: fileSet.Position(expr.Pos()).Line, accepted: packageVars[expr] || constructors[expr], inFunc: constructors[expr]}
		switch {
		case nodeName != "" && (refers(fun, nodeName, "Define") || refers(fun, nodeName, "MustDefine")):
			if len(expr.Args) >= 2 {
				found.name, found.version = staticOf(expr.Args[0]), staticOf(expr.Args[1])
			}
		case flowName != "" && (refers(fun, flowName, "Define") || refers(fun, flowName, "MustDefine")):
			found.flow = true
			found.name, found.version = flowSpec(expr, flowName)
		default:
			return true
		}
		out = append(out, found)
		return true
	})
	return out
}

func packageConstants(file *ast.File, summary *fileSummary) {
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			for i, ident := range value.Names {
				if ident.Name == "_" {
					continue
				}
				evaluated := static{}
				if len(value.Values) == len(value.Names) {
					evaluated = staticOf(value.Values[i])
				}
				if _, again := summary.constants[ident.Name]; again {
					evaluated = static{} // redeclared: refuse rather than pick one
				}
				summary.constants[ident.Name] = evaluated
			}
		}
	}
}

func staticOf(expr ast.Expr) static {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return static{}
		}
		text, err := strconv.Unquote(e.Value)
		if err != nil {
			return static{}
		}
		return static{parts: []part{{literal: text}}, ok: true}
	case *ast.Ident:
		return static{parts: []part{{ident: e.Name}}, ok: true}
	case *ast.ParenExpr:
		return staticOf(e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return static{}
		}
		left, right := staticOf(e.X), staticOf(e.Y)
		if !left.ok || !right.ok {
			return static{}
		}
		return static{parts: append(left.parts, right.parts...), ok: true}
	}
	return static{}
}

// flowSpec reads Name and Version from a flow.Spec composite literal, keyed
// or positional, passed as the call's first argument.
func flowSpec(expr *ast.CallExpr, flowName string) (static, static) {
	if len(expr.Args) == 0 {
		return static{}, static{}
	}
	literal, ok := expr.Args[0].(*ast.CompositeLit)
	if !ok || !refers(literal.Type, flowName, "Spec") {
		return static{}, static{}
	}
	var name, version static
	for index, element := range literal.Elts {
		if pair, keyed := element.(*ast.KeyValueExpr); keyed {
			key, _ := pair.Key.(*ast.Ident)
			switch {
			case key != nil && key.Name == "Name":
				name = staticOf(pair.Value)
			case key != nil && key.Name == "Version":
				version = staticOf(pair.Value)
			}
			continue
		}
		switch index {
		case 0:
			name = staticOf(element)
		case 1:
			version = staticOf(element)
		}
	}
	return name, version
}

// importName returns the name file uses for importPath: its alias, "." for
// a dot import, or the default last element; "" when it is not imported or
// imported only blank.
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
		return foreignDescriptor{}, errors.New("node.json is not valid JSON")
	}
	return descriptor, nil
}

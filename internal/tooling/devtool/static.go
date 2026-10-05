package devtool

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/scaffold"
)

// Framework packages whose calls and literals the static reader recognises.
const (
	nodeImport = scaffold.FrameworkModule + "/node"
	flowImport = scaffold.FrameworkModule + "/flow"
	appImport  = scaffold.FrameworkModule + "/app"
	httpImport = "net/http"
)

// stepKinds maps each flow builder that takes a step id as its second
// argument to the instruction kind it records.
var stepKinds = map[string]string{
	"Call": "call", "ArmCall": "call", "If": "if", "Choose": "choose", "Each": "each",
	"Parallel": "parallel", "TryFinally": "try-finally", "Child": "child",
	"Compare": "compare", "Default": "default", "Template": "template",
}

// httpMethods are net/http's method constants.
var httpMethods = map[string]string{
	"MethodGet": "GET", "MethodHead": "HEAD", "MethodPost": "POST", "MethodPut": "PUT",
	"MethodPatch": "PATCH", "MethodDelete": "DELETE", "MethodConnect": "CONNECT",
	"MethodOptions": "OPTIONS", "MethodTrace": "TRACE",
}

// source is a parsed workspace. Parsing reads files; it runs nothing.
type source struct {
	workspace Workspace
	fset      *token.FileSet
	// files maps a relative path to its syntax; unparsable files are absent.
	files       map[string]*ast.File
	parseErrors []diagnostic.Diagnostic
}

func parseWorkspace(ctx context.Context, workspace Workspace) (*source, error) {
	parsed := &source{workspace: workspace, fset: token.NewFileSet(), files: map[string]*ast.File{}}
	for _, item := range workspace.Packages {
		for _, name := range append(append([]string(nil), item.Files...), item.TestFiles...) {
			if err := ctx.Err(); err != nil {
				return parsed, err
			}
			data, err := readBounded(filepath.Join(workspace.Root, filepath.FromSlash(name)), MaxSourceFileBytes)
			if err != nil {
				parsed.parseErrors = append(parsed.parseErrors, diagnostic.Diagnostic{Code: "source_unreadable", Source: name, Actual: errorText(err), Expected: "a readable Go file of at most 8 MiB", Remediation: "make the file readable or move it out of the project", Message: "a Go source file could not be read"})
				continue
			}
			file, err := parser.ParseFile(parsed.fset, name, data, parser.SkipObjectResolution)
			if err != nil {
				position, message := name, err.Error()
				if list, ok := err.(interface{ Unwrap() []error }); ok && len(list.Unwrap()) > 0 {
					message = list.Unwrap()[0].Error()
				}
				if before, after, ok := strings.Cut(message, ": "); ok && strings.HasPrefix(before, name) {
					position, message = before, after
				}
				parsed.parseErrors = append(parsed.parseErrors, diagnostic.Diagnostic{Code: "source_parse_error", Source: position, Actual: message, Expected: "valid Go syntax", Remediation: "fix the Go syntax error at this position", Message: "a Go source file does not parse"})
				continue
			}
			parsed.files[name] = file
		}
	}
	return parsed, nil
}

func (s *source) position(pos token.Pos) string {
	at := s.fset.Position(pos)
	return fmt.Sprintf("%s:%d:%d", at.Filename, at.Line, at.Column)
}

// importName is the name file refers to importPath by, "" when it does not
// import it.
func importName(file *ast.File, importPath string) string {
	for _, spec := range file.Imports {
		if path, err := strconv.Unquote(spec.Path.Value); err != nil || path != importPath {
			continue
		}
		if spec.Name != nil {
			if spec.Name.Name == "_" {
				return ""
			}
			return spec.Name.Name
		}
		return importPath[strings.LastIndex(importPath, "/")+1:]
	}
	return ""
}

// selects reports whether expr names a symbol of the package imported as
// name, and which one. A dot import matches a bare identifier.
func selects(expr ast.Expr, name string) (string, bool) {
	if name == "" {
		return "", false
	}
	switch e := expr.(type) {
	case *ast.IndexExpr:
		return selects(e.X, name)
	case *ast.IndexListExpr:
		return selects(e.X, name)
	case *ast.SelectorExpr:
		if ident, ok := e.X.(*ast.Ident); ok && ident.Name == name {
			return e.Sel.Name, true
		}
	case *ast.Ident:
		if name == "." {
			return e.Name, true
		}
	}
	return "", false
}

// stringLiteral is the value of a Go string literal expression.
func stringLiteral(expr ast.Expr) (string, bool) {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(literal.Value)
	return value, err == nil
}

// staticNode is a node.Define or node.MustDefine call.
type staticNode struct {
	pkg           Package
	file          string
	pos           token.Pos
	id, version   string
	description   string
	deterministic bool
	remote        bool
	effects       []string
	capabilities  []string
	input, output string
	hasSchemas    bool
	unresolved    []string
}

// staticWorkflow is a flow.Define or flow.MustDefine call.
type staticWorkflow struct {
	pkg           Package
	file          string
	pos           token.Pos
	name, version string
	durability    string
	steps         []staticStep
	unresolved    []string
}

type staticStep struct {
	id, kind string
	pos      token.Pos
	// conditional marks a builder call under Go control flow, which may run
	// zero or several times; duplicate detection ignores it.
	conditional bool
}

// staticRoute is an app.Route literal.
type staticRoute struct {
	file                   string
	pos                    token.Pos
	method, path, workflow string
	unresolved             []string
}

type staticTest struct {
	name, source string
}

type extraction struct {
	nodes     []staticNode
	workflows []staticWorkflow
	routes    []staticRoute
	// tests and examples by package directory.
	tests, examples map[string][]staticTest
	// importers maps an import path to the package directories that import it.
	importers map[string]map[string]bool
}

// extract reads every recognised declaration from the parsed workspace.
func (s *source) extract() extraction {
	result := extraction{tests: map[string][]staticTest{}, examples: map[string][]staticTest{}, importers: map[string]map[string]bool{}}
	for _, item := range s.workspace.Packages {
		for _, name := range append(append([]string(nil), item.Files...), item.TestFiles...) {
			file := s.files[name]
			if file == nil {
				continue
			}
			for _, spec := range file.Imports {
				if path, err := strconv.Unquote(spec.Path.Value); err == nil {
					if result.importers[path] == nil {
						result.importers[path] = map[string]bool{}
					}
					result.importers[path][item.Dir] = true
				}
			}
			if strings.HasSuffix(name, "_test.go") {
				s.collectTests(file, item.Dir, &result)
			}
			s.collectDeclarations(file, item, name, &result)
		}
	}
	return result
}

func (s *source) collectTests(file *ast.File, dir string, result *extraction) {
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Recv != nil {
			continue
		}
		name := function.Name.Name
		ref := staticTest{name: name, source: s.position(function.Name.Pos())}
		switch {
		case testName(name, "Test"):
			result.tests[dir] = append(result.tests[dir], ref)
		case testName(name, "Example"):
			result.examples[dir] = append(result.examples[dir], ref)
		}
	}
}

// testName applies go test's rule: the prefix alone, or the prefix followed
// by a character that is not a lower-case letter.
func testName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	rest := name[len(prefix):]
	return rest == "" || rest[0] < 'a' || rest[0] > 'z'
}

func (s *source) collectDeclarations(file *ast.File, item Package, name string, result *extraction) {
	nodeName, flowName, appName, httpName := importName(file, nodeImport), importName(file, flowImport), importName(file, appImport), importName(file, httpImport)
	if nodeName == "" && flowName == "" && appName == "" {
		return
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch expr := n.(type) {
		case *ast.CallExpr:
			if symbol, ok := selects(expr.Fun, nodeName); ok && (symbol == "Define" || symbol == "MustDefine") {
				result.nodes = append(result.nodes, s.node(expr, nodeName, item, name))
			}
			if symbol, ok := selects(expr.Fun, flowName); ok && (symbol == "Define" || symbol == "MustDefine") {
				result.workflows = append(result.workflows, s.workflow(expr, flowName, item, name))
			}
		case *ast.CompositeLit:
			if symbol, ok := selects(expr.Type, appName); ok && symbol == "Route" {
				result.routes = append(result.routes, s.route(expr, name, httpName))
			}
			if array, ok := expr.Type.(*ast.ArrayType); ok {
				if symbol, ok := selects(array.Elt, appName); ok && symbol == "Route" {
					for _, element := range expr.Elts {
						if literal, ok := element.(*ast.CompositeLit); ok && literal.Type == nil {
							result.routes = append(result.routes, s.route(literal, name, httpName))
						}
					}
				}
			}
		}
		return true
	})
}

func (s *source) node(call *ast.CallExpr, nodeName string, item Package, file string) staticNode {
	found := staticNode{pkg: item, file: file, pos: call.Pos()}
	if len(call.Args) < 3 {
		found.unresolved = append(found.unresolved, "node.Define needs a name, a version and a handler")
		return found
	}
	var ok bool
	if found.id, ok = stringLiteral(call.Args[0]); !ok {
		found.unresolved = append(found.unresolved, "the node name is not a string literal")
	}
	if found.version, ok = stringLiteral(call.Args[1]); !ok {
		found.unresolved = append(found.unresolved, "the node version is not a string literal")
	}
	for _, option := range call.Args[3:] {
		optionCall, ok := option.(*ast.CallExpr)
		symbol, selected := "", false
		if ok {
			symbol, selected = selects(optionCall.Fun, nodeName)
		}
		if !selected {
			found.unresolved = append(found.unresolved, "an option is not a direct node option call")
			continue
		}
		switch symbol {
		case "Description":
			if len(optionCall.Args) != 1 {
				found.unresolved = append(found.unresolved, "node.Description takes one argument")
			} else if text, ok := stringLiteral(optionCall.Args[0]); ok {
				found.description = text
			} else {
				found.unresolved = append(found.unresolved, "the description is not a string literal")
			}
		case "Pure":
			found.deterministic, found.effects = true, nil
		case "RemoteBoundary":
			found.remote = true
		case "Effects", "RequiredCapabilities":
			var values []string
			for _, argument := range optionCall.Args {
				if text, ok := stringLiteral(argument); ok {
					values = append(values, text)
				} else {
					found.unresolved = append(found.unresolved, "a node."+symbol+" value is not a string literal")
				}
			}
			if symbol == "Effects" {
				found.effects, found.deterministic = append(found.effects, values...), false
			} else {
				found.capabilities = append(found.capabilities, values...)
			}
		case "Schemas":
			if len(optionCall.Args) == 2 {
				input, inputOK := s.bytesLiteral(optionCall.Args[0], file)
				output, outputOK := s.bytesLiteral(optionCall.Args[1], file)
				found.hasSchemas = true
				found.input, found.output = input, output
				if !inputOK || !outputOK {
					found.unresolved = append(found.unresolved, "a node schema is not a []byte string literal")
				}
			}
		default:
			found.unresolved = append(found.unresolved, "node."+symbol+" is not a recognised node option")
		}
	}
	return found
}

// bytesLiteral resolves []byte("...") or a package-level variable of that
// form declared in the same file.
func (s *source) bytesLiteral(expr ast.Expr, name string) (string, bool) {
	if conversion, ok := expr.(*ast.CallExpr); ok && len(conversion.Args) == 1 {
		if array, ok := conversion.Fun.(*ast.ArrayType); ok && array.Len == nil {
			if ident, ok := array.Elt.(*ast.Ident); ok && ident.Name == "byte" {
				return stringLiteral(conversion.Args[0])
			}
		}
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return "", false
	}
	file := s.files[name]
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			for index, declared := range value.Names {
				if declared.Name == ident.Name && index < len(value.Values) {
					return s.bytesLiteral(value.Values[index], name)
				}
			}
		}
	}
	return "", false
}

func (s *source) workflow(call *ast.CallExpr, flowName string, item Package, file string) staticWorkflow {
	found := staticWorkflow{pkg: item, file: file, pos: call.Pos()}
	if len(call.Args) != 2 {
		found.unresolved = append(found.unresolved, "flow.Define needs a spec and a build function")
		return found
	}
	spec, ok := call.Args[0].(*ast.CompositeLit)
	if symbol, selected := selects(specType(spec), flowName); !ok || !selected || symbol != "Spec" {
		found.unresolved = append(found.unresolved, "the workflow spec is not a flow.Spec literal")
	} else {
		for _, element := range spec.Elts {
			key, value, ok := keyed(element)
			if !ok {
				found.unresolved = append(found.unresolved, "the workflow spec has a positional field")
				continue
			}
			switch key {
			case "Name", "Version":
				text, ok := stringLiteral(value)
				if !ok {
					found.unresolved = append(found.unresolved, "the workflow "+strings.ToLower(key)+" is not a string literal")
				} else if key == "Name" {
					found.name = text
				} else {
					found.version = text
				}
			case "Durability":
				if symbol, ok := selects(value, flowName); ok && symbol == "Memory" {
					found.durability = string(flow.Memory)
				} else if text, ok := stringLiteral(value); ok {
					found.durability = text
				} else {
					found.unresolved = append(found.unresolved, "the workflow durability is not a flow constant")
				}
			}
		}
	}
	build, ok := call.Args[1].(*ast.FuncLit)
	if !ok {
		found.unresolved = append(found.unresolved, "the build function is not a function literal, so its steps are not read")
		return found
	}
	s.steps(build.Body, flowName, false, &found)
	return found
}

// keyed splits a keyed composite-literal element.
func keyed(element ast.Expr) (string, ast.Expr, bool) {
	field, ok := element.(*ast.KeyValueExpr)
	if !ok {
		return "", nil, false
	}
	key, ok := field.Key.(*ast.Ident)
	if !ok {
		return "", nil, false
	}
	return key.Name, field.Value, true
}

func specType(spec *ast.CompositeLit) ast.Expr {
	if spec == nil {
		return nil
	}
	return spec.Type
}

// steps records the flow builder calls in body. Builders run every arm they
// are given exactly once, so function literals passed to a builder are read
// as part of the same id namespace; any other function literal may never
// run and is not read.
func (s *source) steps(body ast.Node, flowName string, conditional bool, workflow *staticWorkflow) {
	var stack []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if literal, ok := n.(*ast.FuncLit); ok && len(stack) > 0 {
			// An arm is read by its builder call below. Any other literal
			// may never run; if it builds steps, say they were not read.
			if !s.isArm(stack, flowName) && s.buildsSteps(literal, flowName) {
				workflow.unresolved = append(workflow.unresolved, "a function literal not passed to a flow builder builds steps, which are not read")
			}
			return false
		}
		stack = append(stack, n)
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		symbol, selected := selects(call.Fun, flowName)
		kind, isStep := stepKinds[symbol]
		if !selected || !isStep {
			return true
		}
		underControl := conditional || controlled(stack[:len(stack)-1])
		if len(call.Args) < 2 {
			workflow.unresolved = append(workflow.unresolved, "flow."+symbol+" has no step id")
		} else if id, ok := stringLiteral(call.Args[1]); ok {
			workflow.steps = append(workflow.steps, staticStep{id: id, kind: kind, pos: call.Pos(), conditional: underControl})
		} else {
			workflow.unresolved = append(workflow.unresolved, "a flow."+symbol+" step id is not a string literal")
		}
		for _, argument := range call.Args {
			for _, arm := range armLiterals(argument) {
				s.steps(arm.Body, flowName, underControl, workflow)
			}
		}
		return true
	})
}

// isArm reports whether the function literal on top of stack is an argument
// (directly or inside a composite literal) of a flow builder call.
func (s *source) isArm(stack []ast.Node, flowName string) bool {
	for index := len(stack) - 1; index >= 0; index-- {
		switch n := stack[index].(type) {
		case *ast.CompositeLit, *ast.KeyValueExpr:
			continue
		case *ast.CallExpr:
			symbol, selected := selects(n.Fun, flowName)
			_, isStep := stepKinds[symbol]
			return selected && isStep
		default:
			return false
		}
	}
	return false
}

func (s *source) buildsSteps(literal *ast.FuncLit, flowName string) bool {
	found := false
	ast.Inspect(literal.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if symbol, selected := selects(call.Fun, flowName); selected && stepKinds[symbol] != "" {
				found = true
			}
		}
		return !found
	})
	return found
}

// controlled reports whether a node sits under Go control flow inside the
// build function.
func controlled(stack []ast.Node) bool {
	for _, n := range stack {
		switch n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt:
			return true
		}
	}
	return false
}

// armLiterals are the function literals an argument holds directly or in a
// composite literal, such as Choose's case map.
func armLiterals(argument ast.Expr) []*ast.FuncLit {
	var arms []*ast.FuncLit
	ast.Inspect(argument, func(n ast.Node) bool {
		if literal, ok := n.(*ast.FuncLit); ok {
			arms = append(arms, literal)
			return false
		}
		return true
	})
	return arms
}

func (s *source) route(literal *ast.CompositeLit, file, httpName string) staticRoute {
	found := staticRoute{file: file, pos: literal.Pos()}
	for _, element := range literal.Elts {
		key, value, ok := keyed(element)
		if !ok {
			found.unresolved = append(found.unresolved, "the route has a positional field")
			continue
		}
		text, isString := stringLiteral(value)
		switch key {
		case "Method":
			if symbol, ok := selects(value, httpName); ok && httpMethods[symbol] != "" {
				found.method = httpMethods[symbol]
			} else if isString {
				found.method = text
			} else {
				found.unresolved = append(found.unresolved, "the route method is not a literal or net/http constant")
			}
		case "Path", "Workflow":
			if !isString {
				found.unresolved = append(found.unresolved, "the route "+strings.ToLower(key)+" is not a string literal")
			} else if key == "Path" {
				found.path = text
			} else {
				found.workflow = text
			}
		}
	}
	return found
}

// stepDiagnostics are the builder rules flow.Define enforces that a static
// read can decide: the id grammar, the reserved output id and duplicates
// among builder calls that run exactly once.
func (s *source) stepDiagnostics(workflow staticWorkflow) []diagnostic.Diagnostic {
	var found []diagnostic.Diagnostic
	seen := map[string]staticStep{}
	label := workflow.name
	for _, step := range workflow.steps {
		switch {
		case !contract.ValidID(step.id):
			found = append(found, diagnostic.Diagnostic{Code: "workflow_step_id_invalid", Source: s.position(step.pos), Step: step.id, Field: "id", Expected: contract.IDPattern, Actual: step.id, Remediation: "rename the step to match " + contract.IDPattern, Message: "workflow " + label + " has a step id outside the id grammar"})
			continue
		case step.id == flow.OutputID:
			found = append(found, diagnostic.Diagnostic{Code: "workflow_step_id_reserved", Source: s.position(step.pos), Step: step.id, Field: "id", Expected: "an id other than " + flow.OutputID, Actual: step.id, Remediation: "rename the step; " + flow.OutputID + " is the instruction Lower appends for the workflow output", Message: "workflow " + label + " uses the reserved step id " + flow.OutputID})
			continue
		}
		if step.conditional {
			continue
		}
		if first, ok := seen[step.id]; ok {
			found = append(found, diagnostic.Diagnostic{Code: "workflow_step_id_duplicate", Source: s.position(step.pos), Step: step.id, Field: "id", Expected: "a step id unique in the workflow", Actual: "also used at " + s.position(first.pos), Remediation: "rename one of the steps; step ids are one namespace across every arm", Message: "workflow " + label + " uses step id " + step.id + " twice"})
			continue
		}
		seen[step.id] = step
	}
	return found
}

// testsFor are the test and example references for a component in pkg: the
// tests of pkg itself and of every package that imports it directly. They
// are references, not coverage.
func (e extraction) testsFor(pkg Package) (tests, examples []staticTest) {
	dirs := map[string]bool{pkg.Dir: true}
	for dir := range e.importers[pkg.ImportPath] {
		dirs[dir] = true
	}
	ordered := make([]string, 0, len(dirs))
	for dir := range dirs {
		ordered = append(ordered, dir)
	}
	sort.Strings(ordered)
	for _, dir := range ordered {
		tests = append(tests, e.tests[dir]...)
		examples = append(examples, e.examples[dir]...)
	}
	return tests, examples
}

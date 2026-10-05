// Package ownership enforces node independence across every language's
// import graph (architecture §3, ADR 0025): a node may import its own
// files, shared helper and domain code, and external packages, but never
// another node or a workflow, directly or through a chain of shared
// helpers. Workflows, which compose nodes, may import as many nodes as
// they like.
//
// Each runtime supplies an Adapter that parses its source and resolves every
// import to project files with the same rules its toolchain uses. Nothing is
// built, loaded or run. A node is "verified" only when every import in its
// reachable graph was parsed and resolved by a checked adapter; a computed
// import, an evaluating form, an unresolved specifier, a link or a case
// mismatch leaves it "unverified", and a crossing into another node or a
// workflow, or an import cycle, is a "violation".
package ownership

import (
	"fmt"
	"os"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// Diagnostic codes (ADR 0025). Tools match on Code and Source; message
// wording may improve. Link diagnostics reuse ADR 0023's layout_symlink_*
// codes.
const (
	CodeNodeImportsNode      = "ownership_node_imports_node"
	CodeTransitiveNodeImport = "ownership_transitive_node_import"
	CodeNodeImportsWorkflow  = "ownership_node_imports_workflow"
	CodeImportCycle          = "ownership_import_cycle"
	CodeImportUnresolved     = "ownership_import_unresolved"
	CodeImportOutsideRoot    = "ownership_import_outside_root"
	CodeImportCaseMismatch   = "ownership_import_case_mismatch"
	CodeDynamicImport        = "ownership_dynamic_import"
	CodeUnsupportedForm      = "ownership_unsupported_form"
	CodeSourceUnsupported    = "ownership_source_unsupported"
	CodeParseFailed          = "ownership_parse_failed"
	CodeAmbiguousSyntax      = "ownership_ambiguous_syntax"
	CodeConfigInvalid        = "ownership_config_invalid"
	CodeRuntimeUnsupported   = "ownership_runtime_unsupported"
	CodeLimitExceeded        = "ownership_limit_exceeded"
	StatusVerified           = "verified"
	StatusUnverified         = "unverified"
	StatusViolation          = "violation"
	ClassViolation           = "violation"
	ClassUnverified          = "unverified"
	maxDiagnostics           = 256
	maxUnits                 = layout.MaxFiles
	maxEdgesPerFile          = 4096
	maxConfigDepth           = 8
	maxResolutionDepth       = 16
)

var remediations = map[string]string{
	CodeNodeImportsNode:        "nodes never import nodes; compose them in a workflow, or move the shared code to a helper or domain package that imports no node",
	CodeTransitiveNodeImport:   "a shared helper this node imports reaches another node; remove that import from the helper or stop importing the helper",
	CodeNodeImportsWorkflow:    "nodes never import workflows; workflows compose nodes",
	CodeImportCycle:            "break the import cycle; a node's dependency graph must be acyclic",
	CodeImportUnresolved:       "make the import resolvable from source (relative path, tsconfig paths, package.json exports/imports or a declared dependency) so ownership can be checked",
	CodeImportOutsideRoot:      "import project files by a path inside the project root",
	CodeImportCaseMismatch:     "spell the import exactly as the file is named; it resolves differently on case-sensitive and case-insensitive file systems",
	CodeDynamicImport:          "use a string-literal specifier; a computed import cannot be verified",
	CodeUnsupportedForm:        "remove the evaluating or loader form; ownership can verify only static imports",
	CodeSourceUnsupported:      "this file type has no checked ownership adapter; keep node source in a supported language form",
	CodeParseFailed:            "fix the syntax error; ownership reads source without running it",
	CodeAmbiguousSyntax:        "a \"/\" here reads both as division and as a regular expression; parenthesize or end the statement with a semicolon so the file can be verified",
	CodeConfigInvalid:          "fix the tsconfig.json, package.json or go.mod that import resolution reads",
	CodeRuntimeUnsupported:     "this runtime has no checked ownership adapter yet; its nodes cannot be verified",
	CodeLimitExceeded:          "reduce the import graph below the documented ownership bounds",
	layout.CodeSymlinkEscape:   "replace the link with the files it points to; resolution never leaves the project root",
	layout.CodeSymlinkAlias:    "import the file by its real path; a link is never followed",
	layout.CodeSymlinkDangling: "remove the link or restore its target",
	layout.CodeSymlinkLoop:     "remove the link cycle",
}

var classes = map[string]string{
	CodeNodeImportsNode:        ClassViolation,
	CodeTransitiveNodeImport:   ClassViolation,
	CodeNodeImportsWorkflow:    ClassViolation,
	CodeImportCycle:            ClassViolation,
	CodeImportUnresolved:       ClassUnverified,
	CodeImportOutsideRoot:      ClassUnverified,
	CodeImportCaseMismatch:     ClassUnverified,
	CodeDynamicImport:          ClassUnverified,
	CodeUnsupportedForm:        ClassUnverified,
	CodeSourceUnsupported:      ClassUnverified,
	CodeParseFailed:            ClassUnverified,
	CodeAmbiguousSyntax:        ClassUnverified,
	CodeConfigInvalid:          ClassUnverified,
	CodeRuntimeUnsupported:     ClassUnverified,
	CodeLimitExceeded:          ClassUnverified,
	layout.CodeSymlinkEscape:   ClassUnverified,
	layout.CodeSymlinkAlias:    ClassUnverified,
	layout.CodeSymlinkDangling: ClassUnverified,
	layout.CodeSymlinkLoop:     ClassUnverified,
}

// Class reports whether a diagnostic code is a "violation" (the graph
// crosses a node boundary or is cyclic) or "unverified" (part of the graph
// could not be checked), or "" for a code this package does not emit.
func Class(code string) string { return classes[code] }

// Adapter resolves one runtime's import graph. Units are project-relative,
// slash-separated paths: a file for file-based module systems (Node.js), a
// package directory for Go. An adapter must parse source with a real
// grammar and resolve each import with its toolchain's rules; it reports
// anything it cannot resolve rather than guessing.
type Adapter interface {
	// Runtime is the runtime directory name the adapter serves.
	Runtime() string
	// Roots returns the units a node's analysis starts from, given the node
	// as discovered by layout.
	Roots(node layout.Node) []string
	// Analyze parses one unit and resolves its imports.
	Analyze(unit string) Analysis
}

// Analysis is one unit's resolved imports and the problems found in it.
type Analysis struct {
	Edges    []Edge
	Findings []diagnostic.Diagnostic
}

// Edge is one import statement and every unit it may resolve to.
type Edge struct {
	// Source is the importing file and line, file:line.
	Source    string
	Specifier string
	Targets   []Target
}

// Target is a unit an import resolves to.
type Target struct {
	Unit string
	// NoFollow marks a target whose ownership is checked but whose own
	// imports are not traversed: one reached through a link or a case
	// mismatch, or a package directory rather than a source unit.
	NoFollow bool
}

// NodeResult is one node's verdict.
type NodeResult struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Runtime string `json:"runtime"`
	Dir     string `json:"dir"`
	Status  string `json:"status"`
	// Units is how many units the node's analysis read: its own and the
	// shared code it reaches.
	Units int `json:"units"`
}

// Report is the result of an ownership check. Nodes are sorted by Dir and
// Diagnostics like layout's, so the same project yields the same bytes on
// every machine; no record holds an absolute path.
type Report struct {
	Nodes       []NodeResult            `json:"nodes"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics"`
}

// Verified reports whether every node is verified and nothing was found.
func (r *Report) Verified() bool {
	if len(r.Diagnostics) > 0 {
		return false
	}
	for _, node := range r.Nodes {
		if node.Status != StatusVerified {
			return false
		}
	}
	return true
}

// Error is a failed ownership check carrying every diagnostic.
type Error struct{ Diagnostics []diagnostic.Diagnostic }

func (e *Error) Error() string {
	if len(e.Diagnostics) == 0 {
		return "ownership: check failed"
	}
	first := e.Diagnostics[0]
	text := "ownership: " + first.Code
	if first.Source != "" {
		text += " " + first.Source
	}
	text += ": " + first.Message
	if more := len(e.Diagnostics) - 1; more > 0 {
		text += fmt.Sprintf(" (and %d more)", more)
	}
	return text
}

// Codes lists the diagnostics' codes in order.
func (e *Error) Codes() []string {
	codes := make([]string, len(e.Diagnostics))
	for i, d := range e.Diagnostics {
		codes[i] = d.Code
	}
	return codes
}

// Violations returns the violation-class diagnostics: a node reaching
// another node or a workflow, or an import cycle. These are errors.
func (r *Report) Violations() []diagnostic.Diagnostic { return r.byClass(ClassViolation) }

// Warnings returns the unverified-class diagnostics: parts of a graph the
// check could not verify. They are warnings, not errors: an unverified node
// was not shown to break independence (ADR 0025).
func (r *Report) Warnings() []diagnostic.Diagnostic { return r.byClass(ClassUnverified) }

// Unverified returns the nodes whose status is unverified.
func (r *Report) Unverified() []NodeResult {
	var out []NodeResult
	for _, node := range r.Nodes {
		if node.Status == StatusUnverified {
			out = append(out, node)
		}
	}
	return out
}

func (r *Report) byClass(class string) []diagnostic.Diagnostic {
	var out []diagnostic.Diagnostic
	for _, d := range r.Diagnostics {
		if Class(d.Code) == class {
			out = append(out, d)
		}
	}
	return out
}

// Err returns an *Error carrying the violations only, or nil when there is
// none. Unverified nodes do not make it fail: they are warnings, read with
// Warnings and Unverified. Verified reports the stricter "everything was
// verified".
func (r *Report) Err() error {
	if violations := r.Violations(); len(violations) > 0 {
		return &Error{Diagnostics: violations}
	}
	return nil
}

// CheckDir discovers the project at root (ADR 0023) and checks it. A failed
// discovery returns its *layout.Error.
func CheckDir(root string) (*Report, error) {
	project, err := layout.Discover(root)
	if err != nil {
		return nil, err
	}
	return Check(project)
}

// Check analyzes every discovered node's import graph. It returns an error
// only when the project root cannot be opened; findings are in the Report.
func Check(project *layout.Project) (*Report, error) {
	root, err := os.OpenRoot(project.Root)
	if err != nil {
		return nil, fmt.Errorf("ownership: open project root: %w", err)
	}
	defer root.Close()
	c := &checker{
		project:  project,
		files:    newProjectFiles(root, project.Root),
		cache:    map[string]Analysis{},
		graph:    map[string][]string{},
		reachers: map[string][]int{},
		seen:     map[string]bool{},
	}
	c.adapters = map[string]Adapter{}
	for _, adapter := range []Adapter{newGoAdapter(c.files, project), newNodeAdapter(c.files, project)} {
		c.adapters[adapter.Runtime()] = adapter
	}
	return c.run(), nil
}

type checker struct {
	project  *layout.Project
	files    *projectFiles
	adapters map[string]Adapter
	cache    map[string]Analysis // runtime + "\x00" + unit
	graph    map[string][]string // followed edges, for cycle detection
	reachers map[string][]int    // unit → indexes of nodes whose analysis reached it
	results  []NodeResult
	diags    []diagnostic.Diagnostic
	seen     map[string]bool
	units    int
	limited  bool
}

func (c *checker) add(d diagnostic.Diagnostic) {
	if d.Remediation == "" {
		d.Remediation = remediations[d.Code]
	}
	key := strings.Join([]string{d.Code, d.Source, d.Field, d.Expected, d.Actual, d.Message}, "\x00")
	if c.seen[key] {
		return
	}
	c.seen[key] = true
	c.diags = append(c.diags, d)
}

// owner classifies a unit: the node directory that owns it, or the
// workflow path, or neither (shared code).
func (c *checker) owner(unit string) (node, workflow string) {
	if root, ok := layout.NodeRoot(unit); ok {
		return root, ""
	}
	for _, declared := range c.project.Manifest.WorkflowPaths() {
		if unit == declared || strings.HasPrefix(unit, declared+"/") {
			return "", declared
		}
	}
	return "", ""
}

func (c *checker) analyze(adapter Adapter, unit string) Analysis {
	key := adapter.Runtime() + "\x00" + unit
	if analysis, ok := c.cache[key]; ok {
		return analysis
	}
	c.units++
	var analysis Analysis
	if c.units > maxUnits {
		if !c.limited {
			c.limited = true
			analysis.Findings = []diagnostic.Diagnostic{{Code: CodeLimitExceeded, Source: unit, Expected: fmt.Sprint(maxUnits), Message: "the import graph has more units than the ownership check analyzes"}}
		}
	} else {
		analysis = adapter.Analyze(unit)
	}
	c.cache[key] = analysis
	return analysis
}

// step is how a unit was first reached in one node's traversal.
type step struct {
	from string // unit, or "" for a root
	edge string // the import's file:line
}

func (c *checker) run() *Report {
	nodes := append([]layout.Node(nil), c.project.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Dir < nodes[j].Dir })
	nodeFindings := make([][]diagnostic.Diagnostic, len(nodes))
	for index, node := range nodes {
		result := NodeResult{Name: node.Name, Version: node.Version, Runtime: node.Runtime, Dir: node.Dir}
		adapter, ok := c.adapters[node.Runtime]
		if !ok {
			nodeFindings[index] = []diagnostic.Diagnostic{{Code: CodeRuntimeUnsupported, Source: node.Dir, Actual: node.Runtime, Message: "no checked ownership adapter exists for the " + node.Runtime + " runtime; its imports cannot be verified"}}
			c.results = append(c.results, result)
			continue
		}
		nodeFindings[index], result.Units = c.traverse(index, node, adapter)
		c.results = append(c.results, result)
	}
	cycles := c.cycles()
	for index := range nodes {
		findings := nodeFindings[index]
		for _, cycle := range cycles {
			for _, member := range cycle.members {
				if slices.Contains(c.reachers[member], index) {
					findings = append(findings, cycle.diagnostic)
					break
				}
			}
		}
		status := StatusVerified
		for _, d := range findings {
			c.add(d)
			if Class(d.Code) == ClassViolation {
				status = StatusViolation
			} else if status == StatusVerified {
				status = StatusUnverified
			}
		}
		c.results[index].Status = status
	}
	if len(c.diags) > maxDiagnostics {
		c.diags = append(c.diags[:maxDiagnostics], diagnostic.Diagnostic{Code: CodeLimitExceeded, Message: fmt.Sprintf("more than %d diagnostics; the rest were not reported", maxDiagnostics), Remediation: remediations[CodeLimitExceeded]})
	}
	sortDiagnostics(c.diags)
	if c.diags == nil {
		c.diags = []diagnostic.Diagnostic{}
	}
	if c.results == nil {
		c.results = []NodeResult{}
	}
	return &Report{Nodes: c.results, Diagnostics: c.diags}
}

// traverse walks one node's graph breadth-first from its roots. It stops at
// another node or a workflow (a violation) and never traverses a NoFollow
// target. Every finding in a reached unit is the node's.
func (c *checker) traverse(index int, node layout.Node, adapter Adapter) ([]diagnostic.Diagnostic, int) {
	var findings []diagnostic.Diagnostic
	reached := map[string]step{}
	var queue []string
	for _, root := range adapter.Roots(node) {
		if _, ok := reached[root]; !ok {
			reached[root] = step{}
			queue = append(queue, root)
		}
	}
	reported := map[string]bool{}
	for len(queue) > 0 {
		unit := queue[0]
		queue = queue[1:]
		c.reachers[unit] = append(c.reachers[unit], index)
		analysis := c.analyze(adapter, unit)
		findings = append(findings, analysis.Findings...)
		for _, edge := range analysis.Edges {
			for _, target := range edge.Targets {
				targetNode, workflow := c.owner(target.Unit)
				switch {
				case targetNode != "" && targetNode != node.Dir:
					entry, chain := c.chain(reached, unit, edge.Source, target.Unit)
					key := entry + "\x00" + targetNode
					if reported[key] {
						continue
					}
					reported[key] = true
					code := CodeNodeImportsNode
					if len(chain) > 2 {
						code = CodeTransitiveNodeImport
					}
					findings = append(findings, diagnostic.Diagnostic{Code: code, Source: entry, Expected: node.Dir + ", shared code or an external package", Actual: target.Unit, Message: fmt.Sprintf("node %s reaches node %s: %s", node.Dir, targetNode, strings.Join(chain, " -> "))})
				case workflow != "":
					entry, chain := c.chain(reached, unit, edge.Source, target.Unit)
					key := entry + "\x00" + workflow
					if reported[key] {
						continue
					}
					reported[key] = true
					findings = append(findings, diagnostic.Diagnostic{Code: CodeNodeImportsWorkflow, Source: entry, Expected: node.Dir + ", shared code or an external package", Actual: target.Unit, Message: fmt.Sprintf("node %s reaches workflow path %s: %s", node.Dir, workflow, strings.Join(chain, " -> "))})
				case target.NoFollow:
				default:
					c.graph[unit] = appendUnique(c.graph[unit], target.Unit)
					if _, ok := reached[target.Unit]; !ok {
						reached[target.Unit] = step{from: unit, edge: edge.Source}
						queue = append(queue, target.Unit)
					}
				}
			}
		}
	}
	return findings, len(reached)
}

// chain returns the node file:line the path starts at and the path from it,
// as edge sources followed by the offending target.
func (c *checker) chain(reached map[string]step, unit, source, target string) (string, []string) {
	path := []string{target, source}
	for current := unit; ; {
		previous := reached[current]
		if previous.from == "" {
			break
		}
		path = append(path, previous.edge)
		current = previous.from
	}
	slices.Reverse(path)
	return path[0], path
}

func appendUnique(list []string, value string) []string {
	if slices.Contains(list, value) {
		return list
	}
	return append(list, value)
}

type cycle struct {
	members    []string
	diagnostic diagnostic.Diagnostic
}

// cycles finds every strongly connected component of more than one unit,
// or a unit importing itself, in the followed graph (Tarjan).
func (c *checker) cycles() []cycle {
	units := make([]string, 0, len(c.graph))
	for unit := range c.graph {
		units = append(units, unit)
	}
	sort.Strings(units)
	index, low := map[string]int{}, map[string]int{}
	onStack := map[string]bool{}
	var stack []string
	var out []cycle
	next := 0
	var connect func(string)
	connect = func(unit string) {
		index[unit], low[unit] = next, next
		next++
		stack = append(stack, unit)
		onStack[unit] = true
		targets := append([]string(nil), c.graph[unit]...)
		sort.Strings(targets)
		for _, target := range targets {
			if _, visited := index[target]; !visited {
				connect(target)
				low[unit] = min(low[unit], low[target])
			} else if onStack[target] {
				low[unit] = min(low[unit], index[target])
			}
		}
		if low[unit] != index[unit] {
			return
		}
		var members []string
		for {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[top] = false
			members = append(members, top)
			if top == unit {
				break
			}
		}
		if len(members) == 1 && !slices.Contains(c.graph[unit], unit) {
			return
		}
		sort.Strings(members)
		out = append(out, cycle{members: members, diagnostic: diagnostic.Diagnostic{Code: CodeImportCycle, Source: members[0], Actual: strings.Join(members, ", "), Message: fmt.Sprintf("%d units import each other in a cycle", len(members))}})
	}
	for _, unit := range units {
		if _, visited := index[unit]; !visited {
			connect(unit)
		}
	}
	return out
}

func sortDiagnostics(items []diagnostic.Diagnostic) {
	key := func(d diagnostic.Diagnostic) []string {
		return []string{d.Code, d.Source, d.Step, d.Field, d.Expected, d.Actual, d.Message}
	}
	sort.SliceStable(items, func(i, j int) bool { return slices.Compare(key(items[i]), key(items[j])) < 0 })
}

// finding builds an adapter diagnostic.
func finding(code, source, actual, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Code: code, Source: source, Actual: actual, Message: message, Remediation: remediations[code]}
}

func at(file string, line int) string { return fmt.Sprintf("%s:%d", file, line) }

// cleanJoin joins a project-relative directory and a relative path and
// reports whether the result stays inside the root.
func cleanJoin(dir, rel string) (string, bool) {
	joined := path.Clean(path.Join(dir, rel))
	if joined == ".." || strings.HasPrefix(joined, "../") || path.IsAbs(joined) {
		return "", false
	}
	if joined == "." {
		return "", true
	}
	return joined, true
}

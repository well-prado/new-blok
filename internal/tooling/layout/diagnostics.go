package layout

import (
	"fmt"
	"slices"
	"sort"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// Diagnostic codes. These strings are the stable contract (ADR 0023): tools
// and fixtures match on Code and Source; Message wording may improve.
const (
	CodeManifestMissing     = "layout_manifest_missing"
	CodeManifestInvalid     = "layout_manifest_invalid"
	CodeModuleMissing       = "layout_module_missing"
	CodePathOutsideRoot     = "layout_path_outside_root"
	CodeMixedLayout         = "layout_mixed"
	CodeInvalidRuntime      = "layout_invalid_runtime"
	CodeFileUnowned         = "layout_file_unowned"
	CodeFileUnsupported     = "layout_file_unsupported"
	CodeParseFailed         = "layout_parse_failed"
	CodeDescriptorMissing   = "layout_descriptor_missing"
	CodeDescriptorMultiple  = "layout_descriptor_multiple"
	CodeDescriptorNotStatic = "layout_descriptor_not_static"
	CodeDescriptorInvalid   = "layout_descriptor_invalid"
	CodeDescriptorMisplaced = "layout_descriptor_misplaced"
	CodeRuntimeMismatch     = "layout_runtime_mismatch"
	CodeDuplicateIdentity   = "layout_duplicate_identity"
	CodeDuplicateVersion    = "layout_duplicate_version"
	CodePathCollision       = "layout_path_collision"
	CodeOwnershipOverlap    = "layout_ownership_overlap"
	CodeWorkflowPathMissing = "layout_workflow_path_missing"
	CodeNodeImportsNode     = "layout_node_imports_node"
	CodeNodeImportsWorkflow = "layout_node_imports_workflow"
	CodeSymlinkEscape       = "layout_symlink_escape"
	CodeSymlinkAlias        = "layout_symlink_alias"
	CodeSymlinkDangling     = "layout_symlink_dangling"
	CodeSymlinkLoop         = "layout_symlink_loop"
	CodeLimitExceeded       = "layout_limit_exceeded"
)

// Error is a failed discovery. Diagnostics are sorted by code, then source,
// and carry project-relative slash paths only, so the same project yields
// the same bytes on every machine.
type Error struct {
	Diagnostics []diagnostic.Diagnostic
}

func (e *Error) Error() string {
	if len(e.Diagnostics) == 0 {
		return "layout: discovery failed"
	}
	first := e.Diagnostics[0]
	text := "layout: " + first.Code
	if first.Source != "" {
		text += " " + first.Source
	}
	text += ": " + first.Message
	if more := len(e.Diagnostics) - 1; more > 0 {
		text += fmt.Sprintf(" (and %d more)", more)
	}
	return text
}

// Codes lists the diagnostics' codes in order, for tests and tools.
func (e *Error) Codes() []string {
	codes := make([]string, len(e.Diagnostics))
	for i, d := range e.Diagnostics {
		codes[i] = d.Code
	}
	return codes
}

var remediations = map[string]string{
	CodeManifestMissing:     "create blok.json at the project root (blok new writes one)",
	CodeModuleMissing:       "add a go.mod with a module directive at the project root",
	CodeMixedLayout:         "keep every node under the layout blok.json declares, or change its layout",
	CodeInvalidRuntime:      "name the runtime directory with lower-case letters and digits, such as go or nodejs",
	CodeFileUnowned:         "move the file into a node directory or out of the runtime directory",
	CodeFileUnsupported:     "replace the special file with a regular file or remove it",
	CodeParseFailed:         "fix the syntax error; discovery reads source without building it",
	CodeDescriptorMissing:   "declare the node with node.Define in Go, or a node.json descriptor for a foreign runtime",
	CodeDescriptorMultiple:  "keep one node per directory; move the other definition into its own node directory",
	CodeDescriptorNotStatic: "pass the name and version as string literals or package-level string constants",
	CodeDescriptorInvalid:   "use a namespaced lower-case name and a major.minor.patch version",
	CodeDescriptorMisplaced: "define nodes under a node directory and workflows under a declared workflow path",
	CodeRuntimeMismatch:     "move the node under the runtime directory its descriptor uses",
	CodeDuplicateIdentity:   "give each node and workflow a unique name; identity comes from the descriptor, not the directory",
	CodeDuplicateVersion:    "keep one source version per name; older versions are retained artifacts or packages, not parallel source",
	CodePathCollision:       "rename one path; they differ only by letter case and collide on case-insensitive file systems",
	CodeOwnershipOverlap:    "keep workflow paths outside node directories",
	CodeWorkflowPathMissing: "create the declared workflow path or remove it from blok.json workflows",
	CodeNodeImportsNode:     "nodes never import nodes; compose them in a workflow, or move shared code to a domain package",
	CodeNodeImportsWorkflow: "nodes never import workflows; workflows compose nodes",
	CodeSymlinkEscape:       "replace the link with the files it points to; discovery never leaves the project root",
	CodeSymlinkAlias:        "remove the link; a second path to the same source would give it two owners",
	CodeSymlinkDangling:     "remove the link or restore its target",
	CodeSymlinkLoop:         "remove the link cycle",
	CodeLimitExceeded:       "split the project or reduce its size below the documented discovery bounds",
}

// collector accumulates diagnostics up to MaxDiagnostics.
type collector struct {
	items     []diagnostic.Diagnostic
	truncated bool
}

func (c *collector) add(code, source, expected, actual, message string) {
	if len(c.items) >= MaxDiagnostics {
		c.truncated = true
		return
	}
	remediation := remediations[code]
	if remediation == "" {
		remediation = "see ADR 0023 for the discovery contract"
	}
	c.items = append(c.items, diagnostic.Diagnostic{Code: code, Source: source, Expected: expected, Actual: actual, Message: message, Remediation: remediation})
}

func (c *collector) addAll(items []diagnostic.Diagnostic) {
	for _, item := range items {
		if len(c.items) >= MaxDiagnostics {
			c.truncated = true
			return
		}
		c.items = append(c.items, item)
	}
}

func (c *collector) err() error {
	if len(c.items) == 0 {
		return nil
	}
	if c.truncated {
		c.items = append(c.items, diagnostic.Diagnostic{Code: CodeLimitExceeded, Message: fmt.Sprintf("more than %d diagnostics; the rest were not reported", MaxDiagnostics), Remediation: remediations[CodeLimitExceeded]})
	}
	sortDiagnostics(c.items)
	return &Error{Diagnostics: c.items}
}

func sortDiagnostics(items []diagnostic.Diagnostic) {
	key := func(d diagnostic.Diagnostic) []string {
		return []string{d.Code, d.Source, d.Step, d.Field, d.Expected, d.Actual, d.Message}
	}
	sort.SliceStable(items, func(i, j int) bool { return slices.Compare(key(items[i]), key(items[j])) < 0 })
}

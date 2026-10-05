// Package layout discovers an application's nodes and workflows from its
// manifest and descriptors, for both supported layouts:
//
//	classic: runtimes/<runtime>/nodes/<node>/
//	unified: nodes/<runtime>/<node>/
//
// Discovery reads blok.json, go.mod, Go source syntax and foreign node.json
// descriptors. It never builds, loads or runs application code: no go run,
// no go list, no plugin loading and no package initialization. A node's
// identity is its descriptor's name and version, never its directory, so the
// same application discovered in either layout yields the same canonical
// catalog and digest. Every failure is a stable diagnostic (ADR 0023).
package layout

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

const (
	Classic = "classic"
	Unified = "unified"

	// ManifestFile is the application manifest at the project root.
	ManifestFile = "blok.json"
	// DescriptorFile declares a foreign-runtime node's identity. Go nodes
	// are identified by their node.Define call instead.
	DescriptorFile = "node.json"
	// DefaultWorkflowPath is where workflows live when the manifest names
	// no explicit workflow paths (architecture §3).
	DefaultWorkflowPath = "workflows"
	// GoRuntime is the runtime whose descriptors are Go source.
	GoRuntime = "go"

	nodePackage = "github.com/well-prado/new-blok/node"
	flowPackage = "github.com/well-prado/new-blok/flow"
)

// Bounds. Discovery of a project beyond them fails with
// layout_limit_exceeded instead of reading without end.
const (
	MaxNodes        = 1024 // matches contract/runtime.MaxCatalogNodes
	MaxWorkflows    = 1024
	MaxFiles        = 10000
	MaxDepth        = 32
	MaxFileBytes    = 1 << 20
	MaxDiagnostics  = 256
	maxManifestPath = 4096
)

var (
	identityGrammar = regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`)
	versionGrammar  = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	runtimeGrammar  = regexp.MustCompile(`^[a-z][a-z0-9]{0,31}$`)
)

// Manifest is blok.json. Field order is the scaffold's byte-exact output
// order; Workflows is optional and omitted when empty, so existing manifests
// round-trip unchanged.
type Manifest struct {
	Name     string   `json:"name"`
	Module   string   `json:"module"`
	Runtime  string   `json:"runtime"`
	Layout   string   `json:"layout"`
	Triggers []string `json:"triggers"`
	// Types is the Go source blok generate reads by default; its bindings
	// are written beside it.
	Types string `json:"types"`
	// Workflows lists the project-relative directories (or .go files) that
	// hold workflow definitions. Empty means DefaultWorkflowPath.
	Workflows []string `json:"workflows,omitempty"`
}

// WorkflowPaths is the explicit workflow paths, or the default.
func (m Manifest) WorkflowPaths() []string {
	if len(m.Workflows) == 0 {
		return []string{DefaultWorkflowPath}
	}
	return append([]string(nil), m.Workflows...)
}

// NodeDir is where a layout keeps a node's directory (architecture §3).
func NodeDir(layout, runtime, node string) string {
	if layout == Unified {
		return "nodes/" + runtime + "/" + node
	}
	return "runtimes/" + runtime + "/nodes/" + node
}

// NodeRoot returns the node directory that owns a project-relative,
// slash-separated path under either layout, and whether one does. Only paths
// anchored at the project root count: a "nodes" element deeper in the tree
// is an ordinary package name. Every path below a node root, however deeply
// nested, belongs to that one node.
func NodeRoot(rel string) (string, bool) {
	parts := strings.Split(path.Clean(rel), "/")
	switch {
	case len(parts) >= 3 && parts[0] == "nodes":
		return strings.Join(parts[:3], "/"), true
	case len(parts) >= 4 && parts[0] == "runtimes" && parts[2] == "nodes":
		return strings.Join(parts[:4], "/"), true
	}
	return "", false
}

// layoutOf reports which layout a node root belongs to.
func layoutOf(root string) string {
	if strings.HasPrefix(root, "nodes/") {
		return Unified
	}
	return Classic
}

// ParseManifest decodes and validates blok.json. Unknown fields, trailing
// data, an unsupported layout and any path that is absolute, unclean or
// leaves the project root fail closed.
func ParseManifest(data []byte) (Manifest, []diagnostic.Diagnostic) {
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, []diagnostic.Diagnostic{manifestDiag("", "", err.Error(), "blok.json is not a valid manifest: "+err.Error())}
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Manifest{}, []diagnostic.Diagnostic{manifestDiag("", "", "trailing data", "blok.json has data after its object")}
	}
	var diagnostics []diagnostic.Diagnostic
	if manifest.Layout != Classic && manifest.Layout != Unified {
		diagnostics = append(diagnostics, manifestDiag("layout", "classic|unified", manifest.Layout, "blok.json layout must be classic or unified"))
	}
	if manifest.Types != "" && !validRel(manifest.Types) {
		diagnostics = append(diagnostics, outsideRoot("types", manifest.Types))
	}
	seen := map[string]bool{}
	for index, workflow := range manifest.Workflows {
		field := fmt.Sprintf("workflows[%d]", index)
		if !validRel(workflow) {
			diagnostics = append(diagnostics, outsideRoot(field, workflow))
			continue
		}
		for other := range seen {
			if workflow == other || strings.HasPrefix(workflow, other+"/") || strings.HasPrefix(other, workflow+"/") {
				diagnostics = append(diagnostics, manifestDiag(field, "disjoint workflow paths", workflow, fmt.Sprintf("workflow path %q overlaps %q; one file would have two owners", workflow, other)))
			}
		}
		seen[workflow] = true
	}
	return manifest, diagnostics
}

// validRel reports a clean, relative, slash-separated path inside the root.
func validRel(rel string) bool {
	if rel == "" || len(rel) > maxManifestPath || strings.ContainsAny(rel, "\\:\x00") || path.IsAbs(rel) {
		return false
	}
	return path.Clean(rel) == rel && rel != "." && rel != ".." && !strings.HasPrefix(rel, "../")
}

func manifestDiag(field, expected, actual, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Code: CodeManifestInvalid, Source: ManifestFile, Field: field, Expected: expected, Actual: actual, Message: message, Remediation: "fix blok.json: known fields only, layout classic or unified, project-relative paths"}
}

func outsideRoot(field, value string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Code: CodePathOutsideRoot, Source: ManifestFile, Field: field, Expected: "clean project-relative path", Actual: value, Message: fmt.Sprintf("blok.json %s %q is not a clean path inside the project root", field, value), Remediation: "use a relative path with forward slashes and no '..' or '.' elements"}
}

package ownership

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/internal/scaffold"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// requireVerified fails unless every node is verified with a non-empty
// graph: a green check over zero units is not evidence.
func requireVerified(t *testing.T, root string, wantNodes int) *Report {
	t.Helper()
	report, err := CheckDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified() || report.Err() != nil {
		t.Fatalf("false positive: %+v", report.Diagnostics)
	}
	if len(report.Nodes) != wantNodes {
		t.Fatalf("nodes=%+v; want %d", report.Nodes, wantNodes)
	}
	for _, node := range report.Nodes {
		if node.Status != StatusVerified || node.Units == 0 {
			t.Fatalf("node %+v is not verified over a non-empty graph", node)
		}
	}
	return report
}

// TestNativeStartersVerify: the real `blok new` starter, in both layouts,
// has no false positive. Its workflow and composition root import the
// node, which is how nodes are composed.
func TestNativeStartersVerify(t *testing.T) {
	for _, name := range []string{layout.Classic, layout.Unified} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "shop")
			if _, err := scaffold.Create(scaffold.Options{Directory: root, Module: "example.com/shop", Layout: name, Framework: scaffold.Framework{Version: "v0.1.0"}}); err != nil {
				t.Fatal(err)
			}
			report := requireVerified(t, root, 1)
			if report.Nodes[0].Runtime != layout.GoRuntime || report.Nodes[0].Dir != layout.NodeDir(name, layout.GoRuntime, "quote") {
				t.Fatalf("node=%+v", report.Nodes[0])
			}
		})
	}
}

// TestNodeExampleVerifies adds Node.js nodes to the starter using the
// repository's real Node.js SDK source and its real worker fixture node
// file (unchanged import text in the unified layout). Every node is
// verified: the SDK is shared code, and the composition root imports both
// Node.js nodes.
func TestNodeExampleVerifies(t *testing.T) {
	repository := filepath.Join("..", "..", "..")
	read := func(rel string) string {
		data, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	for _, name := range []string{layout.Classic, layout.Unified} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "shop")
			if _, err := scaffold.Create(scaffold.Options{Directory: root, Module: "example.com/shop", Layout: name, Framework: scaffold.Framework{Version: "v0.1.0"}}); err != nil {
				t.Fatal(err)
			}
			fixtures := layout.NodeDir(name, "nodejs", "fixtures")
			format := layout.NodeDir(name, "nodejs", "format")
			sdkImport := "../../../sdk/nodejs/index.js"
			nodes, types := read("testdata/worker/nodejs/nodes.ts"), read("testdata/worker/nodejs/types.ts")
			if !strings.Contains(nodes, sdkImport) {
				t.Fatalf("the worker fixture no longer imports %s", sdkImport)
			}
			if name == layout.Classic { // one directory deeper
				nodes = strings.ReplaceAll(nodes, sdkImport, "../"+sdkImport)
				types = strings.ReplaceAll(types, sdkImport, "../"+sdkImport)
			}
			files := map[string]string{
				"package.json":            `{"private":true,"type":"module","workspaces":["sdk/*"],"devDependencies":{"typescript":"5.9.3","@types/node":"22.18.10"}}`,
				"tsconfig.json":           read("runtime/nodejs/tsconfig.json"),
				"sdk/nodejs/package.json": read("sdk/nodejs/package.json"),
				"sdk/nodejs/index.ts":     read("sdk/nodejs/index.ts"),
				"sdk/nodejs/json.ts":      read("sdk/nodejs/json.ts"),
				"sdk/nodejs/schema.ts":    read("sdk/nodejs/schema.ts"),
				"sdk/nodejs/blob.ts":      read("sdk/nodejs/blob.ts"),
				fixtures + "/node.json":   `{"name":"fixture/quote","version":"1.0.0","runtime":"nodejs"}`,
				fixtures + "/index.ts":    nodes,
				fixtures + "/types.ts":    types,
				format + "/node.json":     `{"name":"shop/format-receipt","version":"1.0.0","runtime":"nodejs"}`,
				format + "/index.ts":      "import { defineNode } from \"@blok/nodejs-sdk\";\nimport { cents } from \"./money.js\";\nexport const format = defineNode<{ total: number }, { text: string }, null>({ name: \"shop/format-receipt\", version: \"1.0.0\", description: \"Formats a receipt\", input: { type: \"object\" }, output: { type: \"object\" }, dependencies: null, execute: (_ctx, input) => ({ text: cents(input.total) }) });\n",
				format + "/money.ts":      "export const cents = (total: number): string => (total / 100).toFixed(2);\n",
				"app/worker/nodes.ts":     "import { nodes } from \"../../" + fixtures + "/index.js\";\nimport { format } from \"../../" + format + "/index.js\";\nexport const all = [...nodes, format];\n",
			}
			writeFiles(t, root, files)
			report := requireVerified(t, root, 3)
			for _, node := range report.Nodes {
				if node.Runtime == "nodejs" && node.Units < 2 {
					t.Fatalf("node %+v did not reach the SDK", node)
				}
			}
		})
	}
}

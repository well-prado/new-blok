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

// TestNodeExample adds Node.js nodes to the starter using the repository's
// real Node.js SDK source and its real worker fixture node file (unchanged
// import text in the unified layout), plus a node written only in the
// allowlist's safe forms. Nothing is a violation, and the composition root
// imports every node. The allowlist (ADR 0025) fails closed on the SDK's
// own code — computed keys such as text[at] and Object.prototype — so the
// nodes that reach the SDK are unverified, and every finding lies in the
// SDK's files, not in a node; the safe-form node is verified.
func TestNodeExample(t *testing.T) {
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
			clean := layout.NodeDir(name, "nodejs", "greet")
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
				"app/worker/nodes.ts":     "import { nodes } from \"../../" + fixtures + "/index.js\";\nimport { format } from \"../../" + format + "/index.js\";\nimport { greet } from \"../../" + clean + "/index.js\";\nexport const all = [...nodes, format, greet];\n",
				clean + "/node.json":      `{"name":"shop/greet","version":"1.0.0","runtime":"nodejs"}`,
				clean + "/index.ts":       "import { title } from \"./title.js\";\nexport const greet = (names: string[]): string => `hello ${title(names[0] ?? \"\")} and ${names.length - 1} more`;\n",
				clean + "/title.ts":       "export const title = (s: string): string => s.slice(0, 1).toUpperCase() + s.slice(1);\n",
			}
			writeFiles(t, root, files)
			report, err := CheckDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Nodes) != 4 {
				t.Fatalf("nodes=%+v", report.Nodes)
			}
			for _, node := range report.Nodes {
				want := StatusUnverified
				if node.Runtime == layout.GoRuntime || node.Dir == clean {
					want = StatusVerified
				}
				if node.Status != want || node.Units == 0 {
					t.Fatalf("node %+v; want %s over a non-empty graph", node, want)
				}
			}
			if len(report.Diagnostics) == 0 {
				t.Fatal("the SDK's computed keys must keep the nodes that reach it unverified")
			}
			for _, d := range report.Diagnostics {
				if Class(d.Code) != ClassUnverified || !strings.HasPrefix(d.Source, "sdk/nodejs/") {
					t.Fatalf("a finding outside the SDK, or a violation: %+v", d)
				}
			}
		})
	}
}

// TestNodeStarterWithDeclaredSDK is the real shape of a Node.js node in an
// application: the starter plus a node that depends on @blok/nodejs-sdk as
// a declared npm dependency (not installed: declared third-party packages
// are external, ADR 0025) and is written in the safe forms. It verifies,
// in both layouts, alongside the native node.
func TestNodeStarterWithDeclaredSDK(t *testing.T) {
	for _, name := range []string{layout.Classic, layout.Unified} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "shop")
			if _, err := scaffold.Create(scaffold.Options{Directory: root, Module: "example.com/shop", Layout: name, Framework: scaffold.Framework{Version: "v0.1.0"}}); err != nil {
				t.Fatal(err)
			}
			dir := layout.NodeDir(name, "nodejs", "format-receipt")
			writeFiles(t, root, map[string]string{
				"package.json":        `{"name":"shop","private":true,"type":"module","dependencies":{"@blok/nodejs-sdk":"0.1.0"},"devDependencies":{"typescript":"5.9.3","@types/node":"22.18.10"}}`,
				"tsconfig.json":       `{"compilerOptions":{"target":"ES2023","module":"NodeNext","moduleResolution":"NodeNext","strict":true}}`,
				dir + "/node.json":    `{"name":"shop/format-receipt","version":"1.0.0","runtime":"nodejs"}`,
				dir + "/index.ts":     "import { defineNode } from \"@blok/nodejs-sdk\";\nimport { cents } from \"./money.js\";\n\nconst amount = { type: \"object\", properties: { total: { type: \"integer\" } }, required: [\"total\"] } as const;\nconst text = { type: \"object\", properties: { text: { type: \"string\" } }, required: [\"text\"] } as const;\n\nexport const formatReceipt = defineNode<{ total: number }, { text: string }, null>({\n  name: \"shop/format-receipt\", version: \"1.0.0\", description: \"Formats a receipt\",\n  input: amount, output: text, dependencies: null, deterministic: true,\n  execute: (_ctx, input) => ({ text: cents(input.total) }),\n});\n",
				dir + "/money.ts":     "export const cents = (total: number): string => `${(total / 100).toFixed(2)} USD`;\n",
				"app/worker/nodes.ts": "import { formatReceipt } from \"../../" + dir + "/index.js\";\nexport const nodes = [formatReceipt];\n",
			})
			report := requireVerified(t, root, 2)
			for _, node := range report.Nodes {
				if node.Dir == dir && node.Units != 2 {
					t.Fatalf("node %+v; want its two files (the SDK is an external package)", node)
				}
			}
		})
	}
}

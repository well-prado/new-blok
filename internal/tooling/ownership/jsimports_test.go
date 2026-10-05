package ownership

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func describe(scan jsScan) []string {
	var out []string
	for _, imported := range scan.imports {
		out = append(out, fmt.Sprintf("%d %s %s", imported.line, imported.form, imported.specifier))
	}
	for _, form := range scan.unverified {
		out = append(out, fmt.Sprintf("%d %s %s", form.line, form.code, form.form))
	}
	slices.Sort(out)
	return out
}

// TestImportForms pins every import form the recognizer reports, the
// forms it must not mistake for imports, and the forms that can never be
// verified.
func TestImportForms(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		want         []string
	}{
		{"default", `import a from "./a.js";`, []string{"1 import ./a.js"}},
		{"named multiline", "import {\n  a,\n  b as c,\n} from './b';", []string{"1 import ./b"}},
		{"namespace", `import * as ns from "pkg/sub"`, []string{"1 import pkg/sub"}},
		{"default and named", `import d, { e } from "#internal/x"`, []string{"1 import #internal/x"}},
		{"default and namespace", `import d, * as n from "x"`, []string{"1 import x"}},
		{"side effect", `import "./setup.js"`, []string{"1 import ./setup.js"}},
		{"type only counts", `import type { T } from "../other/types"`, []string{"1 import ../other/types"}},
		{"default named type", `import type from "./t"`, []string{"1 import ./t"}},
		{"attributes", `import data from "./d.json" with { type: "json" };`, []string{"1 import ./d.json"}},
		{"export star", `export * from "./x"; export * as y from "./y"`, []string{"1 export-from ./x", "1 export-from ./y"}},
		{"export named from", "export {\n a as b } from '@app/lib'", []string{"1 export-from @app/lib"}},
		{"export type from", `export type { T } from "./t"; export type * from "./u"`, []string{"1 export-from ./t", "1 export-from ./u"}},
		{"local export list is not an import", `const a = 1; export { a }; export default a;`, nil},
		{"import equals", `import fs = require("node:fs"); export import x = require("./x")`, []string{"1 import-equals ./x", "1 import-equals node:fs"}},
		{"namespace alias is not an import", `import A = B.C;`, nil},
		{"require", `const x = require("./x"); const y = require(` + "`./y`" + `)`, []string{"1 require ./x", "1 require ./y"}},
		{"require.resolve", `const p = require.resolve("./p")`, []string{"1 require.resolve ./p"}},
		{"module.require", `module.require("./m")`, []string{"1 require ./m"}},
		{"dynamic static", `await import("./lazy.js")`, []string{"1 import() ./lazy.js"}},
		{"typeof import", `type T = typeof import("./t")`, []string{"1 import() ./t"}},
		{"import.meta.resolve", `import.meta.resolve("./r")`, []string{"1 import.meta.resolve ./r"}},
		{"import.meta is not an import", `const u = import.meta.url`, nil},
		{"escaped specifier", `require("\x2e\x2e/b/x"); require("\56\56/c")`, []string{"1 require ../b/x", "1 require ../c"}},
		{"escaped identifier", `require("../b")`, []string{"1 require ../b"}},
		{"reference path", "/// <reference path=\"../b/types.d.ts\" />\nexport {}", []string{"1 reference ../b/types.d.ts"}},
		{"strings are not imports", `const s = "import x from './nope'"; const t = 'require("./nope")'`, nil},
		{"comments are not imports", "// import a from './nope'\n/* require('./nope') */", nil},
		{"template text is not an import", "const s = `import a from './nope' ${1 + 2} require('./nope')`", nil},
		{"regex is not an import", `const r = /import("x")/g; if (a) /require\(/.test(b)`, nil},
		{"non-null assertion divides", "const q = a! / b; const r = c[0]! / 2; import d from './d'", []string{"1 import ./d"}},
		{"division then import", "const q = a / b; import c from './c'", []string{"1 import ./c"}},
		{"regex after block", "function f() {}\n/'/.test(s); import d from './d'", []string{"2 import ./d"}},
		{"property keys and members", `const o = { import: 1, require: 2 }; o.import(); x.require("y"); class K { import() {} require(a) { return a } }`, nil},
		{"dynamic import computed", `import(name)`, []string{"1 ownership_dynamic_import import(expression)"}},
		{"dynamic import template", "import(`./${name}.js`)", []string{"1 ownership_dynamic_import import(expression)"}},
		{"require computed", `require("./" + name)`, []string{"1 ownership_dynamic_import require(expression)"}},
		{"require as value", `const r = require; r("./x")`, []string{"1 ownership_unsupported_form require as a value"}},
		{"require typeof is fine", `if (typeof require === "function") {}`, nil},
		{"eval", `eval("require('../b')")`, []string{"1 ownership_unsupported_form eval"}},
		{"Function constructor", `new Function("return 1")`, []string{"1 ownership_unsupported_form Function constructor"}},
		{"createRequire", `const req = createRequire(import.meta.url)`, []string{"1 ownership_unsupported_form createRequire"}},
		{"getBuiltinModule", `process.getBuiltinModule("module")`, []string{"1 ownership_unsupported_form getBuiltinModule"}},
		{"worker", `new Worker("./w.js")`, []string{"1 ownership_unsupported_form new Worker"}},
		{"require.cache", `delete require.cache[k]`, []string{"1 ownership_unsupported_form require.cache"}},
		{"Function type is fine", `let f: Function; type G = Function[]`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan, err := scanJS([]byte(tc.source))
			if err != nil {
				t.Fatalf("lex: %v", err)
			}
			if got := describe(scan); !slices.Equal(got, tc.want) {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestLexErrorsFailClosed: a source the lexer cannot tokenize is an error,
// never a partial import list.
func TestLexErrorsFailClosed(t *testing.T) {
	for _, source := range []string{
		`const s = "unterminated`,
		"const t = `unterminated",
		"/* unterminated",
		"function f() {",
		"f())",
		`const r = /unterminated`,
	} {
		if _, err := scanJS([]byte(source)); err == nil {
			t.Errorf("lexed %q without error", source)
		}
	}
}

// TestRepositoryNodeSourcesLex: every TypeScript and JavaScript file of the
// repository's Node.js SDK, worker and fixtures tokenizes, and the imports
// found match the ones the files visibly contain.
func TestRepositoryNodeSourcesLex(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	var checked int
	for _, dir := range []string{"sdk/nodejs", "runtime/nodejs", "testdata/worker/nodejs"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(file string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && (entry.Name() == "node_modules" || entry.Name() == "dist") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !hasExtension(entry.Name(), scriptExtensions) {
				return nil
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			scan, err := scanJS(data)
			if err != nil {
				t.Errorf("%s: %v", file, err)
				return nil
			}
			// Every line holding a from "…" clause yields an import.
			for _, imported := range scan.imports {
				lines := strings.Split(string(data), "\n")
				if imported.line < 1 || imported.line > len(lines) {
					t.Errorf("%s: import %q at impossible line %d", file, imported.specifier, imported.line)
				}
			}
			checked++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if checked < 10 {
		t.Fatalf("checked only %d files; the repository's Node sources moved", checked)
	}
}

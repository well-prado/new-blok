package ownership

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestCheckNeverExecutesSource is structural evidence that the ownership
// check only parses: its own source imports nothing that builds, loads or
// runs code, names no process, write or environment function, and reads
// files only through an os.Root (directly or via layout.OpenRegular). Every
// fixture also carries source that would fail loudly if run; nothing runs
// it. This is not a sandbox.
func TestCheckNeverExecutesSource(t *testing.T) {
	forbiddenImports := map[string]bool{
		"os/exec": true, "plugin": true, "go/build": true, "go/importer": true, "go/types": true,
		"reflect": true, "syscall": true, "unsafe": true, "net": true, "net/http": true,
		"golang.org/x/tools/go/packages": true,
	}
	forbiddenNames := map[string]bool{
		"StartProcess": true, "FindProcess": true, "Command": true, "CommandContext": true, "Exec": true,
		"WriteFile": true, "Create": true, "Remove": true, "RemoveAll": true, "Rename": true,
		"Mkdir": true, "MkdirAll": true, "Symlink": true, "Link": true, "Chmod": true, "Chdir": true,
		"Setenv": true, "Getenv": true, "LookupEnv": true, "Pipe": true,
	}
	// Package-level os functions that would bypass the os.Root.
	forbiddenOS := map[string]bool{"Open": true, "OpenFile": true, "ReadFile": true, "ReadDir": true, "Stat": true, "Lstat": true, "Readlink": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range parsed.Imports {
			if value, _ := strconv.Unquote(spec.Path.Value); forbiddenImports[value] {
				t.Errorf("%s imports %s", file, value)
			}
		}
		for _, group := range parsed.Comments {
			for _, comment := range group.List {
				if strings.HasPrefix(comment.Text, "//go:linkname") {
					t.Errorf("%s uses //go:linkname", file)
				}
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			selector, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if forbiddenNames[selector.Sel.Name] {
				t.Errorf("%s names %s", file, selector.Sel.Name)
			}
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == "os" && forbiddenOS[selector.Sel.Name] {
				t.Errorf("%s calls os.%s instead of reading through the os.Root", file, selector.Sel.Name)
			}
			return true
		})
	}
	if checked < 5 {
		t.Fatalf("checked %d files; the package moved", checked)
	}
	if _, err := os.Stat("ownership.go"); err != nil {
		t.Fatal(err)
	}
}

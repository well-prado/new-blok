package agent

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

// #260: a workflow tool's lowered program has no literal form. A call that
// takes a literal lowers with no references, so an engine running that
// program on its own hands the call the workflow input, and an observer or
// journal records the workflow input as that step's input. Only catalog
// dispatch substitutes the literal. These guards make "run the stored
// program outside dispatch" something package agent cannot express.

// The overlay file does what the #257 review did. On a catalog that keeps
// the raw program in package agent it compiles and shows the hazard; once
// the program is held by agent/internal/catalogprogram it must not compile,
// and the compiler must name that type as the reason.
func TestStoredCatalogProgramCannotRunOutsideDispatch(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH: the compile-time guard cannot be checked here")
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": {
		filepath.Join(dir, "zz_escape260_test.go"): filepath.Join(dir, "testdata", "escape260_test.go.txt"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(t.TempDir(), "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(goTool, "test", "-overlay", overlayPath, "-count=1", "-run", "^TestEscape260RunStoredProgramObserved$", ".")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err == nil {
		t.Fatalf("the stored program ran outside dispatch and the overlay test passed:\n%s", text)
	}
	if strings.Contains(text, "HAZARD #260") {
		t.Fatalf("the stored catalog program is reachable and runs wrongly outside dispatch:\n%s", text)
	}
	for _, want := range []string{"cannot use *b.program", "catalogprogram.Program", "as contract.InternalProgram value"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the overlay failed, but not on the guard (want %q in the compiler output):\n%s", want, text)
		}
	}
}

// No value package agent can reach holds a runnable contract.InternalProgram:
// not a field of an agent type, not an exported field or method result of
// another package's type, not the argument of a callback such a method
// takes. The program lives in an unexported field of a type defined in
// another package, which agent code cannot name.
func TestCatalogProgramIsUnreachableFromPackageAgent(t *testing.T) {
	target := reflect.TypeOf(contract.InternalProgram{})
	agentPath := reflect.TypeOf(binding{}).PkgPath()
	seen := map[reflect.Type]bool{}
	var walk func(typ reflect.Type, path string) string
	walk = func(typ reflect.Type, path string) string {
		if typ == target {
			return path
		}
		if seen[typ] {
			return ""
		}
		seen[typ] = true
		local := typ.PkgPath() == agentPath
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			if found := walk(typ.Elem(), path+"[]"); found != "" {
				return found
			}
		case reflect.Map:
			if found := walk(typ.Key(), path+"{key}"); found != "" {
				return found
			}
			if found := walk(typ.Elem(), path+"{}"); found != "" {
				return found
			}
		case reflect.Struct:
			for i := range typ.NumField() {
				field := typ.Field(i)
				if !local && !field.IsExported() {
					continue
				}
				if found := walk(field.Type, path+"."+field.Name); found != "" {
					return found
				}
			}
		case reflect.Func:
			for i := range typ.NumOut() {
				if found := walk(typ.Out(i), path+"()"); found != "" {
					return found
				}
			}
			// A callback parameter receives values from the callee.
			for i := range typ.NumIn() {
				if in := typ.In(i); in.Kind() == reflect.Func {
					for j := range in.NumIn() {
						if found := walk(in.In(j), path+"(callback)"); found != "" {
							return found
						}
					}
				}
			}
		}
		for _, owner := range []reflect.Type{typ, reflect.PointerTo(typ)} {
			for i := range owner.NumMethod() {
				method := owner.Method(i)
				if found := walk(method.Type, path+"."+method.Name); found != "" {
					return found
				}
			}
		}
		return ""
	}
	for _, root := range []reflect.Type{reflect.TypeOf(binding{}), reflect.TypeOf(Catalog{}), reflect.TypeOf(Listing{})} {
		if path := walk(root, root.Name()); path != "" {
			t.Errorf("package agent reaches a runnable program at %s", path)
		}
	}
}

// Package agent cannot construct an engine, so it cannot attach an observer
// or a journal to a catalog run, and it cannot lower a workflow with
// literals itself: the only literal-aware lowering and the only engine run
// sit behind agent/internal/catalogprogram.Program.
func TestCatalogDoesNotImportTheEngineOrTheLowering(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, spec := range parsed.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			switch path {
			case "github.com/well-prado/new-blok/internal/engine", "github.com/well-prado/new-blok/internal/lowering":
				t.Errorf("%s imports %s; a catalog program runs only through catalogprogram.Program.Run", name, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no package agent source files checked")
	}
}

package catalogprogram

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

func literalProgram(t *testing.T) *Program {
	t.Helper()
	p, err := Lower("conf/workflow", "1.0.0", []Instruction{
		{Kind: "call", ID: "reserve", Node: "conf/reserve", Input: "$input"},
		{Kind: "call", ID: "commit", Node: "conf/commit", Input: "$literal", Literal: []byte(`{"sku":"tea","quantity":1}`)},
		{Kind: "call", ID: "audit", Node: "conf/audit", Input: "$step.reserve"},
	}, "$step.commit")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Run is the only way to execute the program, and it hands each literal
// call its literal bytes, unchanged, and each other call what the engine
// resolved. Nothing else of the workflow input reaches a literal call.
func TestRunSubstitutesEveryLiteral(t *testing.T) {
	p := literalProgram(t)
	var mu sync.Mutex
	got := map[string]string{}
	output, err := p.Run(context.Background(), map[string]any{"sku": "SECRET", "quantity": 2}, 8,
		func(string) []string { return []string{"db:conf"} },
		func(_ context.Context, id string, input []byte) (any, error) {
			mu.Lock()
			got[id] = string(input)
			mu.Unlock()
			return map[string]any{"from": id}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"reserve": `{"quantity":2,"sku":"SECRET"}`,
		"commit":  `{"sku":"tea","quantity":1}`,
		"audit":   `{"from":"reserve"}`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatched %v; want %v", got, want)
	}
	if raw, _ := json.Marshal(output); string(raw) != `{"from":"commit"}` {
		t.Fatalf("output %s", raw)
	}
}

func TestLiteralsAreCopiesAndProgramIsComparable(t *testing.T) {
	p := literalProgram(t)
	literals := p.Literals()
	literals["commit"][0] = 'X'
	delete(literals, "commit")
	if again := p.Literals(); string(again["commit"]) != `{"sku":"tea","quantity":1}` || len(again) != 1 {
		t.Fatalf("stored literals changed through a copy: %q", again)
	}
	want := contract.InternalProgram{WorkflowID: "conf/workflow", Version: "1.0.0", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "reserve", Kind: "call", Node: "conf/reserve"},
		{Index: 1, ID: "commit", Kind: "call", Node: "conf/commit"},
		{Index: 2, ID: "audit", Kind: "call", Node: "conf/audit", References: []contract.Reference{{Step: "reserve"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "commit"}}},
	}}
	if !p.Equal(want) {
		t.Fatalf("program %#v; want %#v", p, want)
	}
	if !strings.Contains(p.GoString(), `ID:"commit"`) {
		t.Fatalf("GoString %s", p.GoString())
	}
	if none, err := Lower("conf/workflow", "1.0.0", []Instruction{{Kind: "call", ID: "reserve", Node: "conf/reserve", Input: "$input"}}, "$step.reserve"); err != nil || none.Literals() != nil {
		t.Fatalf("no-literal program: %v %v", none, err)
	}
}

// The package's surface is exactly what dispatch needs. A method or
// function that returned the raw program, or a run that accepted an
// observer, a journal or an engine, would let a caller run the program
// without the literal substitution; adding one turns this test red.
func TestSurfaceCannotHandOutOrObserveTheProgram(t *testing.T) {
	typ := reflect.TypeOf(&Program{})
	var methods []string
	for i := range typ.NumMethod() {
		methods = append(methods, typ.Method(i).Name+" "+typ.Method(i).Type.String())
	}
	want := []string{
		"Equal func(*catalogprogram.Program, contract.InternalProgram) bool",
		"GoString func(*catalogprogram.Program) string",
		"Literals func(*catalogprogram.Program) map[string][]uint8",
		"Run func(*catalogprogram.Program, context.Context, interface {}, int, func(string) []string, catalogprogram.Dispatch) (interface {}, error)",
	}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("Program methods:\n%s\nwant:\n%s", strings.Join(methods, "\n"), strings.Join(want, "\n"))
	}
	if typ.Elem().NumField() != 2 || typ.Elem().Field(0).IsExported() || typ.Elem().Field(1).IsExported() {
		t.Fatal("Program must keep its program and literals in unexported fields")
	}
	exported := exportedDecls(t)
	if wantDecls := []string{"Dispatch", "Instruction", "Lower", "Program"}; !reflect.DeepEqual(exported, wantDecls) {
		t.Fatalf("exported identifiers %v; want %v", exported, wantDecls)
	}
}

func exportedDecls(t *testing.T) []string {
	t.Helper()
	var names []string
	files, _ := filepath.Glob("*.go")
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					names = append(names, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							names = append(names, s.Name.Name)
						}
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if n.IsExported() {
								names = append(names, n.Name)
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

// The literal side map comes only from lowering with Options.Literals. Any
// package that imports the lowering could build a program whose literal
// calls take the workflow input, so only flow (which never sets the option)
// and this package may import it.
func TestOnlyFlowAndCatalogProgramImportTheLowering(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"flow": true, filepath.Join("agent", "internal", "catalogprogram"): true, filepath.Join("internal", "lowering"): true}
	importers := map[string]bool{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == "testdata" || entry.Name() == "node_modules" || strings.HasPrefix(entry.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			if p, _ := strconv.Unquote(spec.Path.Value); p == "github.com/well-prado/new-blok/internal/lowering" {
				rel, _ := filepath.Rel(root, filepath.Dir(path))
				importers[rel] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !importers["flow"] || !importers[filepath.Join("agent", "internal", "catalogprogram")] {
		t.Fatalf("walk did not find the known importers: %v", importers)
	}
	for dir := range importers {
		if !allowed[dir] {
			t.Errorf("%s imports internal/lowering; a literal-bearing program must stay behind catalogprogram.Program", dir)
		}
	}
}

package event

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/internal/tooling/graphcheck"
)

// closure returns every import reachable from root, standard library
// included, from the source import graph.
func closure(graph graphcheck.Graph, root string) map[string]bool {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(path string) {
		if seen[path] {
			return
		}
		seen[path] = true
		for _, imported := range graph.Packages[path].Imports {
			walk(imported)
		}
	}
	walk(root)
	return seen
}

// TestNoStreamOrUIDependencyEntersTheEngine pins the dependency direction:
// the interpreter and the inspection contract reach neither this hub, the
// SSE adapter in inspect, net/http, nor any UI or trigger package; the hub
// itself depends on the standard library only.
func TestNoStreamOrUIDependencyEntersTheEngine(t *testing.T) {
	graph, err := graphcheck.Analyze(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	module := graph.Module
	if _, ok := graph.Packages[module+"/internal/engine"]; !ok {
		t.Fatal("graph does not contain the engine; the check would be vacuous")
	}
	if !closure(graph, module+"/inspect")[module+"/observe/event"] {
		t.Fatal("inspect does not reach observe/event; the check is not seeing the stream")
	}
	forbidden := func(path string) bool {
		// observe/* holds observation adapters (this hub, exporters); the
		// engine reaches only the narrow contract/observe port (ADR 0020).
		if path == module+"/observe" || strings.HasPrefix(path, module+"/observe/") || path == module+"/inspect" || strings.HasPrefix(path, "net/http") {
			return true
		}
		for _, part := range strings.Split(strings.TrimPrefix(path, module+"/"), "/") {
			switch part {
			case "ui", "frontend", "studio", "trigger":
				return true
			}
		}
		return false
	}
	for _, root := range []string{module + "/internal/engine", module + "/contract/inspection", module + "/execution"} {
		for path := range closure(graph, root) {
			if forbidden(path) {
				t.Errorf("%s reaches %s", root, path)
			}
		}
	}
	for _, imported := range graph.Packages[module+"/observe/event"].Imports {
		first, _, _ := strings.Cut(imported, "/")
		if strings.Contains(first, ".") || strings.HasPrefix(imported, "net") || strings.HasPrefix(imported, "os") {
			t.Errorf("observe/event imports %s; it must stay transport- and store-free", imported)
		}
	}
}

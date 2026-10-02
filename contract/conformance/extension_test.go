package conformance

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestExtensionModuleRunsConformanceWithoutEngineInternals builds a separate
// module containing a third-party adapter, runs the trigger corpus against it
// and proves that its dependency closure contains no New Blok internal
// package. The compiler refuses the same module importing the engine.
func TestExtensionModuleRunsConformanceWithoutEngineInternals(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		goTool = filepath.Join(runtime.GOROOT(), "bin", "go")
		if _, statErr := os.Stat(goTool); statErr != nil {
			t.Fatalf("the go tool is required to build the extension module: %v", statErr)
		}
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join("testdata", "extension")
	dir := t.TempDir()
	template, err := os.ReadFile(filepath.Join(fixture, "go.mod.fixture"))
	if err != nil {
		t.Fatal(err)
	}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(dir, "go.mod"), []byte(strings.ReplaceAll(string(template), "REPOSITORY_ROOT", filepath.ToSlash(root))))
	sum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(dir, "go.sum"), sum)
	for _, name := range []string{"loopback.go", "loopback_test.go"} {
		data, err := os.ReadFile(filepath.Join(fixture, "loopback", name))
		if err != nil {
			t.Fatal(err)
		}
		write(filepath.Join(dir, "loopback", name), data)
	}
	run := func(args ...string) (string, error) {
		command := exec.Command(goTool, args...)
		command.Dir = dir
		command.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off", "GOWORK=off")
		output, err := command.CombinedOutput()
		return string(output), err
	}

	deps, err := run("list", "-deps", "-test", "-f", "{{.ImportPath}}", "./...")
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, deps)
	}
	if !strings.Contains(deps, "github.com/well-prado/new-blok/contract/conformance") {
		t.Fatalf("extension does not link the conformance harness:\n%s", deps)
	}
	for _, line := range strings.Split(deps, "\n") {
		if strings.HasPrefix(line, "github.com/well-prado/new-blok/internal/") || strings.HasPrefix(line, "github.com/well-prado/new-blok/flowtest") {
			t.Fatalf("extension conformance links engine internals: %s", line)
		}
	}
	if output, err := run("vet", "./..."); err != nil {
		t.Fatalf("go vet: %v\n%s", err, output)
	}
	if output, err := run("test", "-count=1", "./..."); err != nil {
		t.Fatalf("extension conformance failed: %v\n%s", err, output)
	}

	write(filepath.Join(dir, "loopback", "engine.go"), []byte("package loopback\n\nimport _ \"github.com/well-prado/new-blok/internal/engine\"\n"))
	output, err := run("build", "./...")
	if err == nil || !strings.Contains(output, "use of internal package") {
		t.Fatalf("extension importing the engine built: err=%v\n%s", err, output)
	}
}

package observe_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func goTool(t *testing.T) string {
	t.Helper()
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	path := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the go tool is required: %v", err)
	}
	return path
}

// TestUnselectedExporterAddsNoNetworkListenerOrProvider builds a Go-only
// application that selects an observer and full tracing but no exporter,
// and requires its linked packages to contain no network, TLS, listener,
// gRPC or OpenTelemetry package (ADR 0020). It also pins that the root
// module does not require OpenTelemetry at all: the exporter is a separate
// module, so its dependency versions never enter an application that does
// not import it.
func TestUnselectedExporterAddsNoNetworkListenerOrProvider(t *testing.T) {
	output, err := exec.Command(goTool(t), "list", "-deps", "./testdata/footprint").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, output)
	}
	deps := strings.Fields(string(output))
	linked := map[string]bool{}
	for _, path := range deps {
		linked[path] = true
	}
	if !linked["github.com/well-prado/new-blok/contract/observe"] || !linked["github.com/well-prado/new-blok/internal/engine"] {
		t.Fatal("fixture does not reach the engine and the port; the check would be vacuous")
	}
	for _, path := range deps {
		if path == "net" || strings.HasPrefix(path, "net/") || strings.HasPrefix(path, "crypto/tls") || strings.HasPrefix(path, "go.opentelemetry.io/") || strings.HasPrefix(path, "google.golang.org/grpc") || strings.HasPrefix(path, "github.com/well-prado/new-blok/observe/") {
			t.Errorf("no-exporter application links %s", path)
		}
	}
	mod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "go.opentelemetry.io") {
		t.Fatal("the root module requires OpenTelemetry; the exporter must stay in observe/otel's own module")
	}
	binary := filepath.Join(t.TempDir(), "footprint")
	build := exec.Command(goTool(t), "build", "-trimpath", "-o", binary, "./testdata/footprint")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}
	info, err := os.Stat(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("no-exporter footprint: %d linked packages, %d-byte binary", len(deps), info.Size())
}

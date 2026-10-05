package otel_test

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

func linkedPackages(t *testing.T, target string) map[string]bool {
	t.Helper()
	output, err := exec.Command(goTool(t), "list", "-deps", target).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", target, err, output)
	}
	linked := map[string]bool{}
	for _, path := range strings.Fields(string(output)) {
		linked[path] = true
	}
	return linked
}

// TestSelectedExporterFootprint is the counterpart of contract/observe's
// no-exporter check: the same application with the OTLP/HTTP exporters
// selected does link the SDK and net/http, so that check can fail. The
// bridge package itself links no exporter and no gRPC: the application
// chooses its transport.
func TestSelectedExporterFootprint(t *testing.T) {
	selected := linkedPackages(t, "./testdata/footprint")
	for _, want := range []string{"go.opentelemetry.io/otel/sdk/trace", "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp", "net/http", "github.com/well-prado/new-blok/observe/otel"} {
		if !selected[want] {
			t.Errorf("selected-exporter application does not link %s", want)
		}
	}
	for path := range linkedPackages(t, ".") {
		if strings.HasPrefix(path, "go.opentelemetry.io/otel/exporters/") || strings.HasPrefix(path, "google.golang.org/grpc") {
			t.Errorf("the bridge links %s; exporters are the application's choice", path)
		}
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
	t.Logf("selected-exporter footprint: %d linked packages, %d-byte binary", len(selected), info.Size())
}

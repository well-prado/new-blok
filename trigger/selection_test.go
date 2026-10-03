package trigger_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

const module = "github.com/well-prado/new-blok"

func goTool(t *testing.T) string {
	t.Helper()
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	path := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the go tool is required to build selection binaries: %v", err)
	}
	return path
}

// dependencies returns every package linked into a selection binary and the
// number of embedded files each carries.
func dependencies(t *testing.T, name string) map[string]int {
	t.Helper()
	command := exec.Command(goTool(t), "list", "-deps", "-f", "{{.ImportPath}} {{len .EmbedFiles}}", "./testdata/selection/"+name)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", name, err, output)
	}
	deps := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		var path string
		var embeds int
		if _, err := fmt.Sscan(line, &path, &embeds); err != nil {
			t.Fatalf("go list line %q: %v", line, err)
		}
		deps[path] = embeds
	}
	return deps
}

func build(t *testing.T, name string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	if output, err := exec.Command(goTool(t), "build", "-o", binary, "./testdata/selection/"+name).CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", name, err, output)
	}
	return binary
}

// linked returns linked packages at or under prefix, excluding the selection
// fixtures themselves.
func linked(deps map[string]int, prefix string) []string {
	var found []string
	for path := range deps {
		if strings.HasPrefix(path, module+"/trigger/testdata/") {
			continue
		}
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			found = append(found, path)
		}
	}
	return found
}

func noEmbeddedAssets(t *testing.T, deps map[string]int) {
	t.Helper()
	for path, embeds := range deps {
		if embeds != 0 {
			t.Fatalf("%s embeds %d files into the binary", path, embeds)
		}
	}
}

func TestBinaryWithoutTriggerLinksNoAdapterListenerOrStore(t *testing.T) {
	deps := dependencies(t, "none")
	for _, forbidden := range []string{module + "/trigger", "net", "net/http", "database/sql", module + "/store", module + "/contract/conformance", "github.com/nats-io", "github.com/coder/websocket"} {
		if found := linked(deps, forbidden); len(found) != 0 {
			t.Fatalf("binary with no trigger links %v", found)
		}
	}
	noEmbeddedAssets(t, deps)
	output, err := exec.Command(build(t, "none")).Output()
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Goroutines int `json:"goroutines"`
		Output     struct {
			TotalCents int64 `json:"totalCents"`
		} `json:"output"`
	}
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatalf("report %q: %v", output, err)
	}
	if report.Goroutines != 1 || report.Output.TotalCents != 3000 {
		t.Fatalf("report=%+v; want one goroutine and a computed quote", report)
	}
}

func TestBinaryWithHTTPSelectedServesAndDrains(t *testing.T) {
	deps := dependencies(t, "http")
	if len(linked(deps, module+"/trigger/http")) == 0 || len(linked(deps, "net/http")) == 0 {
		t.Fatal("selected HTTP trigger is not linked")
	}
	for _, forbidden := range []string{module + "/trigger/worker", module + "/trigger/pubsub", module + "/trigger/websocket", module + "/store", "database/sql", module + "/contract/conformance", "github.com/nats-io", "github.com/coder/websocket"} {
		if found := linked(deps, forbidden); len(found) != 0 {
			t.Fatalf("HTTP-only binary links unselected %v", found)
		}
	}
	noEmbeddedAssets(t, deps)
	command := exec.Command(build(t, "http"))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill() }()
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() {
		t.Fatalf("binary printed no address: %v", lines.Err())
	}
	var ready struct {
		Listening string `json:"listening"`
	}
	if err := json.Unmarshal(lines.Bytes(), &ready); err != nil || ready.Listening == "" {
		t.Fatalf("ready line %q: %v", lines.Bytes(), err)
	}
	response, err := http.Post("http://"+ready.Listening+"/quotes", "application/json", bytes.NewReader([]byte(`{"sku":"coffee","quantity":2}`)))
	if err != nil {
		t.Fatal(err)
	}
	var quote struct {
		TotalCents int64 `json:"totalCents"`
	}
	err = json.NewDecoder(response.Body).Decode(&quote)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || quote.TotalCents != 3000 {
		t.Fatalf("status=%d quote=%+v err=%v", response.StatusCode, quote, err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if !lines.Scan() || !strings.Contains(lines.Text(), `"stopped":true`) {
		t.Fatalf("binary did not drain: %q %v", lines.Text(), lines.Err())
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("binary did not exit after SIGTERM")
	}
}

func TestBinaryWithWorkerSelectedProcessesDurableJob(t *testing.T) {
	deps := dependencies(t, "worker")
	if len(linked(deps, module+"/trigger/worker")) == 0 || len(linked(deps, module+"/store/sqlite")) == 0 {
		t.Fatal("selected worker trigger or its store is not linked")
	}
	for _, forbidden := range []string{module + "/trigger/http", module + "/trigger/pubsub", "net/http", module + "/contract/conformance", "github.com/nats-io", "github.com/coder/websocket"} {
		if found := linked(deps, forbidden); len(found) != 0 {
			t.Fatalf("worker-only binary links unselected %v", found)
		}
	}
	noEmbeddedAssets(t, deps)
	output, err := exec.Command(build(t, "worker"), filepath.Join(t.TempDir(), "jobs.db")).Output()
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		State  string `json:"state"`
		Output struct {
			TotalCents int64 `json:"totalCents"`
		} `json:"output"`
	}
	if err := json.Unmarshal(output, &report); err != nil || report.State != "completed" || report.Output.TotalCents != 3000 {
		t.Fatalf("report %q err=%v", output, err)
	}
}

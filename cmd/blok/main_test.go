package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/scaffold"
)

func TestCommands(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"default help", nil, "Usage: blok <command>"},
		{"explicit help", []string{"help"}, "Usage: blok <command>"},
		{"help flag", []string{"--help"}, "Usage: blok <command>"},
		{"version", []string{"version"}, "blok 0.0.0-dev"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := run(tt.args, &out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tt.want) {
				t.Fatalf("output %q does not contain %q", out.String(), tt.want)
			}
		})
	}
}

// repoRoot is this repository, a local framework checkout for --framework.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func goTool(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("go", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s in %s: %v\n%s", strings.Join(args, " "), dir, err, output)
	}
	return string(output)
}

// TestFreshApplicationBuildsServesAndRegenerates: a fresh scaffold in a temp
// directory outside this module tidies, vets, tests and builds with no manual
// repair; its binary serves the quote workflow over the Blok HTTP trigger;
// and generating its bindings again changes nothing (#64 review P1).
func TestFreshApplicationBuildsServesAndRegenerates(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a fresh application")
	}
	for layout, node := range map[string]string{"classic": "runtimes/go/nodes/quote", "unified": "nodes/go/quote"} {
		t.Run(layout, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "shop")
			var out bytes.Buffer
			if err := run([]string{"new", directory, "--module", "example.com/shop", "--layout", layout, "--non-interactive", "--framework", repoRoot(t)}, &out); err != nil {
				t.Fatalf("new: %v\n%s", err, out.String())
			}
			goTool(t, directory, "vet", "./...")
			goTool(t, directory, "test", "./...")
			// Selecting only HTTP links only HTTP: no other trigger, store,
			// broker or foreign-runtime package reaches the application.
			deps := goTool(t, directory, "list", "-deps", "./...")
			for _, unselected := range []string{"/trigger/worker", "/trigger/cron", "/trigger/grpc", "/trigger/pubsub", "/trigger/webhook", "/trigger/sse", "/trigger/websocket", "/trigger/mcp", "/store/sqlite", "/runtime/worker", "nats-io", "modernc.org/sqlite"} {
				if strings.Contains(deps, unselected) {
					t.Fatalf("the HTTP-only starter depends on %s:\n%s", unselected, deps)
				}
			}
			binary := filepath.Join(t.TempDir(), "shop")
			if runtime.GOOS == "windows" {
				binary += ".exe"
			}
			goTool(t, directory, "build", "-o", binary, "./cmd/shop")

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			_ = listener.Close()
			server := exec.Command(binary)
			server.Env = append(os.Environ(), "ADDR="+addr)
			if err := server.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })
			post := func(body string) (int, string) {
				for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
					response, err := http.Post("http://"+addr+"/quotes", "application/json", strings.NewReader(body))
					if err == nil {
						defer response.Body.Close()
						raw, _ := io.ReadAll(response.Body)
						return response.StatusCode, string(raw)
					}
					if time.Now().After(deadline) {
						t.Fatalf("the application never answered: %v", err)
					}
				}
			}
			if status, body := post(`{"sku":"coffee","quantity":2}`); status != http.StatusOK || !strings.Contains(body, `"totalCents":3000`) {
				t.Fatalf("quote: status=%d body=%s", status, body)
			}
			if status, body := post(`{"sku":"coffee","quantity":0}`); status != http.StatusBadRequest {
				t.Fatalf("invalid quantity: status=%d body=%s", status, body)
			}

			bindings := filepath.Join(directory, filepath.FromSlash(node), "bindings_gen.go")
			before, err := os.ReadFile(bindings)
			if err != nil {
				t.Fatal(err)
			}
			t.Chdir(directory)
			for range 2 {
				out.Reset()
				if err := run([]string{"generate"}, &out); err != nil || !strings.Contains(out.String(), "(unchanged)") {
					t.Fatalf("generate: err=%v out=%q; want unchanged", err, out.String())
				}
			}
			if err := run([]string{"generate", "--check"}, &out); err != nil {
				t.Fatalf("generate --check: %v", err)
			}
			if after, _ := os.ReadFile(bindings); !bytes.Equal(before, after) {
				t.Fatal("regenerating changed the bindings")
			}
		})
	}
}

// TestInteractiveAnswersMatchFlags: piped answers, all read by one reader,
// produce exactly the files the equivalent flags do (#64 review P2).
func TestInteractiveAnswersMatchFlags(t *testing.T) {
	root := repoRoot(t)
	interactive := filepath.Join(t.TempDir(), "piped")
	answers := strings.Join([]string{interactive, "example.com/piped", "piped", "go", "unified", "http"}, "\n") + "\n"
	var out bytes.Buffer
	if err := runWithIO([]string{"new", "--interactive", "--skip-tidy", "--framework", root}, &out, strings.NewReader(answers)); err != nil {
		t.Fatalf("interactive: %v\n%s", err, out.String())
	}
	flags := filepath.Join(t.TempDir(), "piped")
	if err := run([]string{"new", flags, "--module", "example.com/piped", "--name", "piped", "--layout", "unified", "--non-interactive", "--skip-tidy", "--framework", root}, &out); err != nil {
		t.Fatal(err)
	}
	want := readTree(t, flags)
	got := readTree(t, interactive)
	if len(got) != len(want) {
		t.Fatalf("interactive wrote %d files; flags wrote %d", len(got), len(want))
	}
	for path, content := range want {
		if !bytes.Equal(got[path], content) {
			t.Fatalf("%s differs between interactive and flag scaffolds", path)
		}
	}
}

func readTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(path)
		relative, _ := filepath.Rel(root, path)
		files[filepath.ToSlash(relative)] = content
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

// TestInteractiveCancellation: ending input at the first prompt or a later
// one cancels, and nothing is created.
func TestInteractiveCancellation(t *testing.T) {
	for name, input := range map[string]string{"first prompt": "", "later prompt": "%s\nexample.com/later\n"} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "later")
			var out bytes.Buffer
			err := runWithIO([]string{"new", "--interactive", "--framework", repoRoot(t)}, &out, strings.NewReader(strings.Replace(input, "%s", directory, 1)))
			if !errors.Is(err, scaffold.ErrCancelled) {
				t.Fatalf("got %v, want cancellation", err)
			}
			if _, err := os.Stat(directory); !os.IsNotExist(err) {
				t.Fatalf("a cancelled scaffold created %s", directory)
			}
		})
	}
}

// TestFrameworkDefaultsToThisBuild: without --framework the starter depends
// on the version this blok was built from, unless that build cannot be
// fetched from the module proxy.
func TestFrameworkDefaultsToThisBuild(t *testing.T) {
	original := buildVersion
	t.Cleanup(func() { buildVersion = original })
	for version, ok := range map[string]bool{"v0.3.0": true, "v0.0.0-20261005000000-abcdefabcdef": true, "(devel)": false, "": false, "v0.0.0-20261005000000-abcdefabcdef+dirty": false} {
		buildVersion = func() string { return version }
		selected, err := frameworkFor("")
		if ok && (err != nil || selected.Version != version) {
			t.Fatalf("%q: selected=%+v err=%v", version, selected, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "--framework")) {
			t.Fatalf("%q: err=%v; want a pointer to --framework", version, err)
		}
	}
	if selected, err := frameworkFor(repoRoot(t)); err != nil || selected.Dir != repoRoot(t) {
		t.Fatalf("a directory selects a local checkout: %+v %v", selected, err)
	}
	if selected, err := frameworkFor("v1.2.3"); err != nil || selected.Version != "v1.2.3" {
		t.Fatalf("a version selects that version: %+v %v", selected, err)
	}
}

// TestReadmeQuickstartRuns: the README's blok new command, run as written
// from the repository root, succeeds (#64 review P2).
func TestReadmeQuickstartRuns(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join(repoRoot(t), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	var line string
	for _, candidate := range strings.Split(string(readme), "\n") {
		if strings.HasPrefix(candidate, "go run ./cmd/blok new ") {
			line = candidate
		}
	}
	if line == "" {
		t.Fatal("README has no blok new command")
	}
	args := strings.Fields(strings.TrimPrefix(line, "go run ./cmd/blok "))
	// Run from a stand-in checkout inside the test's temp space, so the
	// command's relative paths (../my-app) never touch the real tree.
	// --framework . only checks the checkout's go.mod; tidy is skipped here
	// and covered by TestFreshApplicationBuildsServesAndRegenerates.
	workspace := t.TempDir()
	repo := filepath.Join(workspace, "repo")
	mod, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), mod, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(repo)
	var out bytes.Buffer
	if err := run(append(args, "--skip-tidy"), &out); err != nil {
		t.Fatalf("%s: %v", line, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "my-app", "cmd", "my_app", "main.go")); err != nil {
		t.Fatalf("the README's next steps expect cmd/my_app: %v", err)
	}
}

func TestGenerateIsStableAndDoesNotExecuteSource(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "types.go")
	output := filepath.Join(directory, "bindings_gen.go")
	source := []byte("package example\nfunc init() { panic(\"must not execute\") }\ntype Item struct { ID string }\n")
	if err := os.WriteFile(input, source, 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"generate", "--input", input, "--output", output}, &out); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"generate", "--input", input, "--output", output, "--check"}, &out); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generated output changed on repeat")
	}
}

func TestUnsupportedCommandsProduceNoSuccessOutput(t *testing.T) {
	for _, args := range [][]string{{"serve"}, {"version", "unexpected"}} {
		var out bytes.Buffer
		if err := run(args, &out); err == nil || out.Len() != 0 {
			t.Fatalf("args %v: error=%v, output=%q", args, err, out.String())
		}
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestOutputFailureIsReturned(t *testing.T) {
	want := errors.New("closed output")
	if err := run([]string{"version"}, failingWriter{want}); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// TestGenerateRejectsTypeErrorsAndNeverReplacesHandWrittenFiles: a type error
// fails generation without writing, and an existing file that is not marked
// generated is never replaced.
func TestGenerateRejectsTypeErrorsAndNeverReplacesHandWrittenFiles(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "types.go")
	output := filepath.Join(directory, "bindings_gen.go")
	if err := os.WriteFile(input, []byte("package example\ntype Item struct { ID Missing }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"generate", input, output}, &out); err == nil || !strings.Contains(err.Error(), "type check") {
		t.Fatalf("type error: err=%v", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("a failed generation wrote %s", output)
	}
	if err := os.WriteFile(input, []byte("package example\ntype Item struct { ID string }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("package example\n// hand written\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"generate", input, output}, &out); err == nil || !strings.Contains(err.Error(), "refusing to replace non-generated file") {
		t.Fatalf("hand-written output: err=%v", err)
	}
	if content, _ := os.ReadFile(output); string(content) != "package example\n// hand written\n" {
		t.Fatalf("the hand-written file changed: %q", content)
	}
}

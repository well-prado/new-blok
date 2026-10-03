package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestNewCreatesConventionalApplicationAndIsNonInteractive(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "quote-app")
	var out bytes.Buffer
	if err := run([]string{"new", directory, "--module", "example.com/quote", "--name", "quote"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "blok.json", "cmd/quote/main.go", "cmd/quote/main_test.go", "internal/app/types.go"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	if err := run([]string{"new", directory, "--non-interactive"}, &out); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("expected non-empty refusal, got %v", err)
	}
}

func TestNewInteractiveCancellation(t *testing.T) {
	var out bytes.Buffer
	if err := runWithIO([]string{"new", "--interactive"}, &out, strings.NewReader("")); !errors.Is(err, scaffold.ErrCancelled) {
		t.Fatalf("got %v, want cancellation", err)
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

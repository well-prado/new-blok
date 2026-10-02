package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
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

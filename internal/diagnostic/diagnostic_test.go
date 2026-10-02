package diagnostic

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenErrorSnapshot(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("..", "..", "testdata", "diagnostics", "unknown-instruction.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := (Diagnostic{Code: "unknown_instruction", Source: "workflow.json:12:5", Step: "route", Field: "kind", Expected: "call|condition|output", Actual: "teleport", Remediation: "replace kind with a supported instruction", Message: "instruction kind is not supported"}).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(got)+"\n" != string(want) {
		t.Fatalf("snapshot=%s want=%s", got, want)
	}
}

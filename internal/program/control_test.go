package program

import (
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

// The v1 artifact holds the call instruction set only: a control program
// (format 2, or any instruction with a control body) has no v1 form and is
// refused, never encoded into v1 and decoded back without its format
// (Review R round 1 on #333 slice 1a, ADR 0028).
func TestV1ArtifactRefusesControlPrograms(t *testing.T) {
	output := contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "fan"}}}
	parallel := contract.InternalInstruction{Index: 0, ID: "fan", Kind: "parallel", Control: &contract.Control{Arms: []contract.Arm{{Name: "0"}}}}
	for name, input := range map[string]contract.InternalProgram{
		"format 2":     {WorkflowID: "w", Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{{Index: 0, ID: "fan", Kind: "parallel"}, output}},
		"control body": {WorkflowID: "w", Instructions: []contract.InternalInstruction{parallel, output}},
	} {
		if _, err := Build(input, Limits{}); err == nil || !strings.Contains(err.Error(), "unsupported_program_format") {
			t.Errorf("%s: Build err=%v; want unsupported_program_format", name, err)
		}
	}
	encoded := `{"version":1,"checkpointVersion":1,"workflowId":"w","instructions":[{"index":0,"id":"fan","kind":"parallel","control":{"arms":[{"name":"0"}]}}],"digest":"sha256:0"}`
	if _, err := Decode([]byte(encoded)); err == nil || !strings.Contains(err.Error(), "unsupported_program_format") {
		t.Errorf("Decode err=%v; want unsupported_program_format", err)
	}
}

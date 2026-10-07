package flow_test

import (
	"encoding/json"
	"testing"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/program"
)

// A call-only workflow lowers to the program it lowered to before #333, byte
// for byte, so its artifact digest is unchanged: the control instruction
// set is a new program format, never a re-encoding of the old one (ADR
// 0031). The expected encoding and digest were recorded on origin/main
// 79ee0a7.
func TestCallOnlyLoweringKeepsItsEncodingAndDigest(t *testing.T) {
	n := newConformanceNodes(t)
	lowered, err := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
	}).Lower()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(lowered)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"workflowId":"conformance","version":"1.0.0","digest":"","instructions":[{"index":0,"id":"reserve","kind":"call","node":"reserve","output":null},{"index":1,"id":"commit","kind":"call","node":"commit","references":[{"step":"reserve","path":["body"]}],"output":null},{"index":2,"id":"output","kind":"output","references":[{"step":"commit"}],"output":null}]}`
	if string(encoded) != want {
		t.Fatalf("encoding changed:\n got %s\nwant %s", encoded, want)
	}
	built, err := program.Build(lowered, program.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := built.Digest(), "sha256:c97cf42bad24f5ef9a373ec7afc46d3f18e315a0ea02211c7fcfbbd8e4803342"; got != want {
		t.Fatalf("artifact digest=%s want %s", got, want)
	}
}

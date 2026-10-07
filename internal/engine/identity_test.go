package engine

import (
	"encoding/json"
	"strings"
	"testing"
)

// The identity of a root-iteration wait step as origin/main a3d90d2 (before
// #382) derived and stored it: its operation key addresses the step and
// wait records of every suspended cluster run, and internal/cluster stores
// the identity in this JSON form.
const (
	pinnedRunID        = "run:00000000000000000000000000000382"
	pinnedPlan         = `{"name":"approval","timeoutMillis":60000}`
	pinnedOperationKey = "op:d7fd717f9a7c118f8240da06db6639b7fcaf6ac178f331ab8401731f46168c98"
	pinnedIdentityJSON = `{"RunID":"run:00000000000000000000000000000382","ArtifactDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","StepID":"approval","InputDigest":"sha256:9fa48c6496cac7520e0902a8e8defa92ad62666da18434674df1ffe193716d68","OperationKey":"op:d7fd717f9a7c118f8240da06db6639b7fcaf6ac178f331ab8401731f46168c98"}`
)

var pinnedArtifact = "sha256:" + strings.Repeat("a", 64)

// TestStepIdentityEncodingIsPinned: the operation key and the stored JSON
// of a root-iteration step are the bytes origin/main wrote, whichever way
// the root is spelled, and do not depend on StepIdentity's Go field names
// (#382: renaming a field on a3d90d2 moved both, so every suspended run
// would have failed after an upgrade). Another iteration has another key.
func TestStepIdentityEncodingIsPinned(t *testing.T) {
	for _, root := range []string{"", RootIteration} {
		identity := NewStepIdentity(pinnedRunID, pinnedArtifact, "approval", root, []byte(pinnedPlan))
		if identity.OperationKey != pinnedOperationKey {
			t.Fatalf("iteration %q: operation key %s; want %s", root, identity.OperationKey, pinnedOperationKey)
		}
		encoded, err := json.Marshal(identity.Canonical())
		if err != nil || string(encoded) != pinnedIdentityJSON {
			t.Fatalf("iteration %q: stored identity\n %s err=%v\nwant\n %s", root, encoded, err, pinnedIdentityJSON)
		}
		var stored StepIdentity
		if err := json.Unmarshal([]byte(pinnedIdentityJSON), &stored); err != nil || !stored.SameExecution(identity) || stored.Iteration() != RootIteration {
			t.Fatalf("iteration %q: a record written before #382 decodes as %+v err=%v; want the same execution", root, stored, err)
		}
	}
	second := NewStepIdentity(pinnedRunID, pinnedArtifact, "approval", "loop[1]", []byte(pinnedPlan))
	if second.OperationKey == pinnedOperationKey || second.SameExecution(NewStepIdentity(pinnedRunID, pinnedArtifact, "approval", "loop[0]", []byte(pinnedPlan))) {
		t.Fatalf("iterations share an identity: %+v", second)
	}
	// sha256 of the root encoding with ,"IterationPath":"loop[1]" before
	// its closing brace (computed outside Go).
	if want := "op:6857842b6d7eb31a84abf3277dae2a74acd8805dba2d0bfcb084f7eaadf7c184"; second.OperationKey != want {
		t.Fatalf("iteration loop[1]: operation key %s; want %s", second.OperationKey, want)
	}
}

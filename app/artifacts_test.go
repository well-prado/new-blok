package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/artifact"
)

func TestArtifactProbeChecksIdentityAndRetainedFormats(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	expected := artifact.Manifest{Name: "deploy/orders", Version: "1.0.0", WorkflowDigest: digest, NativeBinaryDigest: digest, LockDigest: digest, CompilerDigest: digest, CheckpointFormat: "v1"}
	identity, _ := expected.Digest()
	actual := expected
	checkpoint := artifact.Checkpoint{ArtifactDigest: identity, CheckpointFormat: "v1"}
	probe, err := ArtifactProbe(expected, func(context.Context) (artifact.Manifest, []artifact.Checkpoint, error) {
		return actual, []artifact.Checkpoint{checkpoint}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	actual.NativeBinaryDigest = "sha256:" + strings.Repeat("b", 64)
	if probe(context.Background()) == nil {
		t.Fatal("different binary accepted")
	}
	actual = expected
	checkpoint.CheckpointFormat = "v2"
	if probe(context.Background()) == nil {
		t.Fatal("incompatible retained format accepted")
	}
	checkpoint.CheckpointFormat = "v1"
	checkpoint.ArtifactDigest = ""
	if probe(context.Background()) == nil {
		t.Fatal("missing retained artifact accepted")
	}
	if _, err := ArtifactProbe(expected, nil); err == nil {
		t.Fatal("missing inventory accepted")
	}
	if _, err := ArtifactProbe(artifact.Manifest{}, func(context.Context) (artifact.Manifest, []artifact.Checkpoint, error) {
		return expected, nil, nil
	}); err == nil {
		t.Fatal("invalid expected manifest accepted")
	}
	probe, err = ArtifactProbe(expected, func(context.Context) (artifact.Manifest, []artifact.Checkpoint, error) {
		return artifact.Manifest{}, nil, errors.New("inventory unavailable")
	})
	if err != nil || probe(context.Background()) == nil {
		t.Fatal("unavailable inventory accepted")
	}
}

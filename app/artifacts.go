package app

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/well-prado/new-blok/contract/artifact"
)

// ArtifactProbe binds readiness to an immutable deployment identity and every
// retained checkpoint. The application supplies its actual artifact inventory;
// a missing inventory or incompatible retained run fails closed.
func ArtifactProbe(expected artifact.Manifest, inventory func(context.Context) (artifact.Manifest, []artifact.Checkpoint, error)) (func(context.Context) error, error) {
	digest, err := expected.Digest()
	if err != nil {
		return nil, err
	}
	if inventory == nil {
		return nil, errors.New("deployment: artifact inventory required")
	}
	return func(ctx context.Context) error {
		actual, checkpoints, err := inventory(ctx)
		if err != nil {
			return err
		}
		actualDigest, err := actual.Digest()
		if err != nil {
			return err
		}
		if actualDigest != digest {
			return errors.New("deployment: incompatible artifact")
		}
		for _, checkpoint := range checkpoints {
			if err := artifact.VerifyCheckpoint(actual, checkpoint); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// RetainedArtifact is the identity-only inventory supplied by the owning store.
type RetainedArtifact struct {
	ArtifactDigest     string
	ManifestJSON       json.RawMessage
	CheckpointArtifact string
	CheckpointDigest   string
	CheckpointPresent  bool
}

// RetainedArtifactProbe verifies the actual retained journal against the
// executable selected by the application. This single-version deployment
// refuses older identities, even if their manifest remains in the database.
// checkpointDigest identifies the application's supported checkpoint codec;
// it is not a digest of mutable checkpoint state.
func RetainedArtifactProbe(expected artifact.Manifest, checkpointDigest string, inventory func(context.Context, func(RetainedArtifact) error) error) (func(context.Context) error, error) {
	digest, err := expected.Digest()
	if err != nil {
		return nil, err
	}
	if checkpointDigest == "" || inventory == nil {
		return nil, errors.New("deployment: journal inventory and checkpoint codec required")
	}
	return func(ctx context.Context) error {
		return inventory(ctx, func(item RetainedArtifact) error {
			if len(item.ManifestJSON) == 0 {
				return errors.New("deployment: required retained artifact missing")
			}
			var retained artifact.Manifest
			if err := json.Unmarshal(item.ManifestJSON, &retained); err != nil {
				return err
			}
			actual, err := retained.Digest()
			if err != nil {
				return err
			}
			if actual != item.ArtifactDigest || actual != digest {
				return errors.New("deployment: retained executable unavailable or incompatible")
			}
			if item.CheckpointPresent {
				if item.CheckpointDigest != checkpointDigest {
					return errors.New("deployment: incompatible retained checkpoint codec")
				}
				return artifact.VerifyCheckpoint(retained, artifact.Checkpoint{ArtifactDigest: item.CheckpointArtifact, CheckpointFormat: expected.CheckpointFormat})
			}
			return nil
		})
	}, nil
}

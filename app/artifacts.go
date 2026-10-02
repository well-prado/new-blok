package app

import (
	"context"
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

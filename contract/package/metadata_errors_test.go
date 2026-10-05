package packagecontract

import (
	"errors"
	"testing"
)

func TestInvalidMetadataRemainsInvalidPackage(t *testing.T) {
	bundle := fixtureBundle(t)
	bundle.Manifest.Metadata.NodeDescriptor = nil
	_, err := bundle.Verify(TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment())
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing required metadata must unwrap to ErrInvalid, got %v", err)
	}
}

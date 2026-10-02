package runtime

import (
	"errors"
	"testing"

	contract "github.com/well-prado/new-blok/contract/runtime"
)

func TestTokenAuthenticationCannotWidenCapabilityScope(t *testing.T) {
	a, err := NewTokenAuthenticator([]Credential{{Token: "secret-token", Principal: "worker-1", Capabilities: []contract.Capability{"payment:read"}}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Authenticate("secret-token", []contract.Capability{"payment:read"})
	if err != nil || p.Name != "worker-1" {
		t.Fatalf("auth: %+v %v", p, err)
	}
	if _, err := a.Authenticate("secret-token", []contract.Capability{"payment:write"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("widened scope: %v", err)
	}
	if _, err := a.Authenticate("wrong", nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("spoofed token: %v", err)
	}
}

func TestBlobStoreRequiresDigestAndBoundsPayload(t *testing.T) {
	b, err := NewBlobStore(32)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("bounded blob")
	ref := contract.BlobRef{Digest: contract.CanonicalDigest(value), Size: len(value)}
	if err := b.Put(ref, value); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(ref)
	if err != nil || string(got) != string(value) {
		t.Fatalf("blob: %s %v", got, err)
	}
	bad := ref
	bad.Digest = contract.CanonicalDigest([]byte("other"))
	if err := b.Put(bad, value); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("digest mismatch: %v", err)
	}
	oversized := contract.BlobRef{Digest: contract.CanonicalDigest(make([]byte, 33)), Size: 33}
	if err := b.Put(oversized, make([]byte, 33)); !errors.Is(err, contract.ErrLimitExceeded) {
		t.Fatalf("oversized: %v", err)
	}
}

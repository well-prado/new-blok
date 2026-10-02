package runtime

import (
	"errors"
	"fmt"
	"sync"
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

func TestAuthenticationOwnsCredentialsAndNarrowsReturnedGrants(t *testing.T) {
	values := []Credential{{Token: "synthetic", Principal: "worker", Capabilities: []contract.Capability{"blob:read", "blob:write"}}}
	a, err := NewTokenAuthenticator(values)
	if err != nil {
		t.Fatal(err)
	}
	values[0].Capabilities[0] = "admin"
	p, err := a.Authenticate("synthetic", []contract.Capability{"blob:read"})
	if err != nil || len(p.Capabilities) != 1 || p.Capabilities[0] != "blob:read" {
		t.Fatalf("grant: %+v %v", p, err)
	}
	p.Capabilities[0] = "admin"
	if _, err := a.Authenticate("synthetic", []contract.Capability{"admin"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("mutation granted authority: %v", err)
	}
	p, err = a.Authenticate("synthetic", nil)
	if err != nil || len(p.Capabilities) != 0 {
		t.Fatalf("empty request widened: %+v %v", p, err)
	}
	if _, err := NewTokenAuthenticator([]Credential{{Token: "same", Principal: "a"}, {Token: "same", Principal: "b"}}); err == nil {
		t.Fatal("ambiguous credential accepted")
	}
}

func testSession(t *testing.T, a *TokenAuthenticator, token, name string, caps ...contract.Capability) *AuthenticatedSession {
	t.Helper()
	s, err := a.NewSession(token, name, caps)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionReconnectCannotSpoofRotateOrRestoreDroppedAuthority(t *testing.T) {
	a, err := NewTokenAuthenticator([]Credential{
		{Token: "one", Principal: "worker", Capabilities: []contract.Capability{"blob:read", "blob:write"}},
		{Token: "two", Principal: "worker", Capabilities: []contract.Capability{"blob:read", "blob:write"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.NewSession("one", "spoof", nil); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("principal spoof: %v", err)
	}
	s := testSession(t, a, "one", "worker", "blob:read", "blob:write")
	p := s.Principal()
	p.Capabilities[0] = "admin"
	if err := s.ValidateCall("spoof", nil); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("call spoof: %v", err)
	}
	if err := s.Reconnect("two", "worker", []contract.Capability{"blob:read"}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("rotation: %v", err)
	}
	if err := s.Reconnect("one", "spoof", nil); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("reconnect spoof: %v", err)
	}
	if err := s.Reconnect("one", "worker", []contract.Capability{"blob:read"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Reconnect("one", "worker", []contract.Capability{"blob:write"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("scope restored: %v", err)
	}
	if err := s.ValidateCall("worker", []contract.Capability{"blob:write"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("call widened: %v", err)
	}
	a.Revoke("one")
	if err := s.Authorize(nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked session: %v", err)
	}
	if _, err := a.Authenticate("one", nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked token: %v", err)
	}
	if err := s.Reconnect("one", "worker", nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked reconnect: %v", err)
	}
	if err := (&AuthenticatedSession{}).Authorize(nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero proof: %v", err)
	}
}

func TestAuthorizedBlobOwnershipAndAggregateLimits(t *testing.T) {
	caps := []contract.Capability{"blob:read", "blob:write"}
	a, err := NewTokenAuthenticator([]Credential{{Token: "a", Principal: "a", Capabilities: caps}, {Token: "b", Principal: "b", Capabilities: caps}})
	if err != nil {
		t.Fatal(err)
	}
	sa := testSession(t, a, "a", "a", caps...)
	sb := testSession(t, a, "b", "b", caps...)
	store, err := NewBlobStore(8)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("data")
	ref := contract.BlobRef{Digest: contract.CanonicalDigest(value), Size: 4}
	if err := store.PutAuthorized(sa, ref, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, err := store.GetAuthorized(sa, ref)
	if err != nil || string(got) != "data" {
		t.Fatalf("input alias: %q %v", got, err)
	}
	got[0] = 'X'
	if _, err := store.GetAuthorized(sb, ref); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("cross owner: %v", err)
	}
	if _, err := store.Get(ref); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("local namespace leaked: %v", err)
	}
	bad := ref
	bad.Size = 3
	if _, err := store.GetAuthorized(sa, bad); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("forged size: %v", err)
	}
	if _, err := store.ResolveAuthorized(sa, []contract.BlobRef{ref, ref}, 7); !errors.Is(err, contract.ErrLimitExceeded) {
		t.Fatalf("aggregate refs: %v", err)
	}
	if _, err := store.ResolveAuthorized(sa, make([]contract.BlobRef, 1025), 8); !errors.Is(err, contract.ErrLimitExceeded) {
		t.Fatalf("reference count: %v", err)
	}
	if err := store.PutAuthorized(sb, ref, []byte("data")); err != nil {
		t.Fatal(err)
	}
	other := []byte("!")
	otherRef := contract.BlobRef{Digest: contract.CanonicalDigest(other), Size: 1}
	if err := store.PutAuthorized(sa, otherRef, other); !errors.Is(err, contract.ErrLimitExceeded) {
		t.Fatalf("aggregate stored bytes: %v", err)
	}
	resolved, err := store.ResolveAuthorized(sa, []contract.BlobRef{ref, ref}, 8)
	if err != nil || string(resolved[0]) != "data" {
		t.Fatalf("resolve: %v %v", resolved, err)
	}
	resolved[0][0] = 'X'
	if string(resolved[1]) != "data" {
		t.Fatal("repeated refs share mutable output")
	}
	readOnly := testSession(t, a, "a", "a", "blob:read")
	if err := store.PutAuthorized(readOnly, ref, []byte("data")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("read only write: %v", err)
	}
	a.Revoke("a")
	if _, err := store.GetAuthorized(sa, ref); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked read: %v", err)
	}
}

func TestBlobRefMalformedAndEmptyMetadataBounds(t *testing.T) {
	b, _ := NewBlobStore(1)
	for _, ref := range []contract.BlobRef{{Digest: "bad", Size: 0}, {Digest: contract.CanonicalDigest(nil), Size: -1}, {Digest: contract.CanonicalDigest(nil), Size: 2}} {
		if err := b.Put(ref, nil); !errors.Is(err, contract.ErrLimitExceeded) {
			t.Fatalf("put invalid ref: %v", err)
		}
		if _, err := b.Get(ref); !errors.Is(err, contract.ErrLimitExceeded) {
			t.Fatalf("get invalid ref: %v", err)
		}
	}
	// Empty content has one digest, but different credentials have distinct owners.
	credentials := make([]Credential, 1025)
	for i := range credentials {
		credentials[i] = Credential{Token: fmt.Sprint(i), Principal: fmt.Sprint(i), Capabilities: []contract.Capability{"blob:write"}}
	}
	a, err := NewTokenAuthenticator(credentials)
	if err != nil {
		t.Fatal(err)
	}
	ref := contract.BlobRef{Digest: contract.CanonicalDigest(nil), Size: 0}
	for i := range credentials {
		s := testSession(t, a, fmt.Sprint(i), fmt.Sprint(i), "blob:write")
		err := b.PutAuthorized(s, ref, nil)
		if i < 1024 && err != nil {
			t.Fatal(err)
		}
		if i == 1024 && !errors.Is(err, contract.ErrLimitExceeded) {
			t.Fatalf("metadata capacity: %v", err)
		}
	}
}

func TestConcurrentSessionChecksAndReconnect(t *testing.T) {
	a, _ := NewTokenAuthenticator([]Credential{{Token: "synthetic", Principal: "worker", Capabilities: []contract.Capability{"blob:read"}}})
	s := testSession(t, a, "synthetic", "worker", "blob:read")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.Authorize(nil)
				_ = s.Principal()
				_ = s.Reconnect("synthetic", "worker", nil)
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); a.Revoke("synthetic") }()
	wg.Wait()
	if err := s.Authorize(nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revocation lost: %v", err)
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

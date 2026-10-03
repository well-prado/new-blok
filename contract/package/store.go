package packagecontract

import "sync"

// Store is an in-memory, offline package source useful to local consumers and
// conformance adapters. It never executes package contents.
type Store struct {
	mu       sync.RWMutex
	packages map[string]Bundle
}

func NewStore() *Store { return &Store{packages: make(map[string]Bundle)} }

func (s *Store) Publish(bundle Bundle, policy TrustPolicy, env Environment) (Verified, error) {
	if s == nil {
		return Verified{}, &Error{Code: "invalid_manifest", Message: "package store is nil"}
	}
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return Verified{}, err
	}
	key := packageKey(bundle.Manifest.Identity)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, exists := s.packages[key]; exists {
		oldDigest, oldErr := old.Manifest.Digest()
		if oldErr != nil || oldDigest != verified.ManifestDigest || ArtifactDigest(old.Artifact) != verified.ArtifactDigest || !sameSignature(old.Signature, bundle.Signature) {
			return Verified{}, &Error{Code: "version_conflict", Path: "identity", Message: "an immutable package version already exists with different content"}
		}
		return verified, nil
	}
	s.packages[key] = cloneBundle(bundle)
	return verified, nil
}

func (s *Store) Fetch(id Identity, policy TrustPolicy, env Environment) (Bundle, Verified, error) {
	if s == nil {
		return Bundle{}, Verified{}, ErrNotFound
	}
	if err := id.Validate(); err != nil {
		return Bundle{}, Verified{}, err
	}
	s.mu.RLock()
	bundle, ok := s.packages[packageKey(id)]
	s.mu.RUnlock()
	if !ok {
		return Bundle{}, Verified{}, ErrNotFound
	}
	bundle = cloneBundle(bundle)
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return Bundle{}, Verified{}, err
	}
	return bundle, verified, nil
}

func packageKey(id Identity) string { return id.Name + "@" + id.Version }

func cloneBundle(bundle Bundle) Bundle {
	bundle.Artifact = append([]byte(nil), bundle.Artifact...)
	if bundle.Signature != nil {
		signature := *bundle.Signature
		bundle.Signature = &signature
	}
	bundle.Manifest.Dependencies = append([]Dependency(nil), bundle.Manifest.Dependencies...)
	if bundle.Manifest.Compatibility.Runtimes != nil {
		runtimes := make(map[string]string, len(bundle.Manifest.Compatibility.Runtimes))
		for key, value := range bundle.Manifest.Compatibility.Runtimes {
			runtimes[key] = value
		}
		bundle.Manifest.Compatibility.Runtimes = runtimes
	}
	return bundle
}

func sameSignature(a, b *Signature) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

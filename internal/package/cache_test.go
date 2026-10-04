package packagemanager

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contractpackage "github.com/well-prado/new-blok/contract/package"
)

func TestCacheVerifiesExactIdentityAndLockReplay(t *testing.T) {
	cache, err := NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	policy := contractpackage.TrustPolicy{AllowUnsignedLocal: true}
	env := contractpackage.Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}
	old := cacheBundle(t, "shop/lib", "1.0.0")
	newer := cacheBundle(t, "shop/lib", "2.0.0")
	if err := cache.Put(context.Background(), old, policy, env); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(context.Background(), newer, policy, env); err != nil {
		t.Fatal(err)
	}
	verified, err := old.Verify(policy, env)
	if err != nil {
		t.Fatal(err)
	}
	lock := contractpackage.Lock{FormatVersion: 1, Roots: []contractpackage.Dependency{{Name: "shop/lib", Version: "1.0.0"}}, Packages: []contractpackage.LockedPackage{{Identity: verified.Identity, ManifestDigest: verified.ManifestDigest, ArtifactDigest: verified.ArtifactDigest, Trust: verified.Trust}}}
	source := CacheSource{Cache: cache, Policy: policy, Env: env, Offline: true}
	bundles, err := source.FetchLocked(context.Background(), lock)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundles) != 1 || bundles[0].Manifest.Identity.Version != "1.0.0" {
		t.Fatalf("locked replay selected a different version: %+v", bundles)
	}
	if _, _, err := cache.Get(context.Background(), contractpackage.Identity{Name: "shop/missing", Version: "1.0.0"}, policy, env); !errors.Is(err, contractpackage.ErrOfflineMissing) {
		t.Fatalf("offline cache miss classification=%v", err)
	}
	changed := cacheBundle(t, "shop/lib", "1.0.0")
	changed.Artifact = []byte("different content for the immutable version")
	changed.Manifest.ArtifactDigest = contractpackage.ArtifactDigest(changed.Artifact)
	if err := cache.Put(context.Background(), changed, policy, env); !errors.Is(err, contractpackage.ErrVersionConflict) {
		t.Fatalf("changed immutable version accepted: %v", err)
	}
}

func TestCacheDetectsBlobCorruptionOnEveryRead(t *testing.T) {
	cache, err := NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	bundle := cacheBundle(t, "shop/corrupt", "1.0.0")
	policy := contractpackage.TrustPolicy{AllowUnsignedLocal: true}
	env := contractpackage.Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}
	if err := cache.Put(context.Background(), bundle, policy, env); err != nil {
		t.Fatal(err)
	}
	indexData, err := os.ReadFile(cache.indexPath(bundle.Manifest.Identity))
	if err != nil {
		t.Fatal(err)
	}
	var index cacheIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(cache.root, "blobs", digestHex(index.Digest)+".bundle")
	data, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0x20
	if err := os.WriteFile(blob, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.Get(context.Background(), bundle.Manifest.Identity, policy, env); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("corrupted cache read was not rejected: %v", err)
	}
}

func TestCachePolicyIsScopedToQueriedIdentity(t *testing.T) {
	cache, err := NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	env := contractpackage.Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}
	unsigned := cacheBundle(t, "shop/local", "1.0.0")
	if err := cache.Put(context.Background(), unsigned, contractpackage.TrustPolicy{AllowUnsignedLocal: true}, env); err != nil {
		t.Fatal(err)
	}
	seed := []byte(strings.Repeat("r", ed25519.SeedSize))
	private := ed25519.NewKeyFromSeed(seed)
	trusted := cacheBundle(t, "shop/trusted", "1.0.0")
	signature, err := contractpackage.Sign(trusted.Manifest, "fixture", private)
	if err != nil {
		t.Fatal(err)
	}
	trusted.Signature = signature
	policy := contractpackage.TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{"fixture": private.Public().(ed25519.PublicKey)}}
	if err := cache.Put(context.Background(), trusted, policy, env); err != nil {
		t.Fatal(err)
	}
	source := CacheSource{Cache: cache, Policy: policy, Env: env, Offline: true}
	ids, err := source.Versions(context.Background(), "shop/trusted")
	if err != nil || len(ids) != 1 || ids[0] != trusted.Manifest.Identity {
		t.Fatalf("scoped trusted lookup failed: ids=%+v err=%v", ids, err)
	}
}

func TestCacheCapacityIsAtomicUnderConcurrentWrites(t *testing.T) {
	cache, err := NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	policy := contractpackage.TrustPolicy{AllowUnsignedLocal: true}
	env := contractpackage.Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}
	bundles := make([]contractpackage.Bundle, maxCacheEntries+1)
	for i := range bundles {
		bundles[i] = cacheBundle(t, "shop/concurrent", versionFor(i))
	}
	var wg sync.WaitGroup
	var success, full int
	var resultMu sync.Mutex
	for _, bundle := range bundles {
		bundle := bundle
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cache.Put(context.Background(), bundle, policy, env)
			resultMu.Lock()
			defer resultMu.Unlock()
			if err == nil {
				success++
			} else if strings.Contains(err.Error(), "capacity") {
				full++
			} else {
				t.Errorf("concurrent cache write: %v", err)
			}
		}()
	}
	wg.Wait()
	indexes, err := os.ReadDir(filepath.Join(cache.root, "indexes"))
	if err != nil {
		t.Fatal(err)
	}
	if success != maxCacheEntries || full != 1 || len(indexes) != maxCacheEntries {
		t.Fatalf("capacity outcomes: success=%d full=%d indexes=%d", success, full, len(indexes))
	}
}

func TestCacheLockWaitHonorsContextCancellation(t *testing.T) {
	cache, err := NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := lockCache(context.Background(), cache.root)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err = cache.Get(ctx, contractpackage.Identity{Name: "shop/blocked", Version: "1.0.0"}, contractpackage.TrustPolicy{}, contractpackage.Environment{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked read error=%v", err)
	}
	if time.Since(started) > 300*time.Millisecond {
		t.Fatalf("cache lock ignored cancellation for %s", time.Since(started))
	}
}

func TestConcurrentLockWritesRemainParseable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blok.lock")
	locks := make([]contractpackage.Lock, 8)
	for i := range locks {
		bundle := cacheBundle(t, "shop/lock", versionFor(i))
		verified, err := bundle.Verify(contractpackage.TrustPolicy{AllowUnsignedLocal: true}, contractpackage.Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}})
		if err != nil {
			t.Fatal(err)
		}
		locks[i] = contractpackage.Lock{FormatVersion: 1, Roots: []contractpackage.Dependency{{Name: verified.Identity.Name, Version: verified.Identity.Version}}, Packages: []contractpackage.LockedPackage{{Identity: verified.Identity, ManifestDigest: verified.ManifestDigest, ArtifactDigest: verified.ArtifactDigest, Trust: verified.Trust}}}
	}
	var wg sync.WaitGroup
	for _, lock := range locks {
		lock := lock
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := WriteLock(path, lock); err != nil {
				t.Errorf("write lock: %v", err)
			}
		}()
	}
	wg.Wait()
	got, digest, err := ReadLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Packages) != 1 || len(digest) != 71 {
		t.Fatalf("invalid lock after concurrent atomic writes: %+v %q", got, digest)
	}
}

func TestReadLockRejectsOversizedCollectionsBeforeFullDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.lock")
	roots := strings.TrimSuffix(strings.Repeat(`{"name":"shop/root","version":"1.0.0"},`, contractpackage.MaxResolvedPackages+1), ",")
	data := `{"formatVersion":1,"roots":[` + roots + `],"packages":[]}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadLock(path); err == nil || !strings.Contains(err.Error(), "count exceeds") {
		t.Fatalf("oversized root collection was not rejected during preflight: %v", err)
	}
}

func cacheBundle(t *testing.T, name, version string) contractpackage.Bundle {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "local-node-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var bundle contractpackage.Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		t.Fatal(err)
	}
	bundle.Manifest.Identity = contractpackage.Identity{Name: name, Version: version}
	bundle.Manifest.Dependencies = nil
	descriptor := *bundle.Manifest.Metadata.NodeDescriptor
	descriptor.Name, descriptor.Version = name, version
	bundle.Manifest.Metadata.NodeDescriptor = &descriptor
	bundle.Artifact = []byte("synthetic cache artifact:" + name + "@" + version)
	bundle.Manifest.ArtifactDigest = contractpackage.ArtifactDigest(bundle.Artifact)
	bundle.Signature = nil
	if err := bundle.Manifest.Validate(); err != nil {
		t.Fatalf("synthetic cache bundle invalid: %v", err)
	}
	return bundle
}

func versionFor(index int) string { return "1.0." + strconv.Itoa(index) }

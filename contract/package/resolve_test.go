package packagecontract

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestResolutionFixtures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "resolution-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		FormatVersion             int `json:"formatVersion"`
		ExpectedPackageExecutions int `json:"expectedPackageExecutions"`
		ExpectedExternalEffects   int `json:"expectedExternalEffects"`
		Cases                     []struct {
			Name     string       `json:"name"`
			Roots    []Dependency `json:"roots"`
			Packages []struct {
				Name         string       `json:"name"`
				Version      string       `json:"version"`
				Dependencies []Dependency `json:"dependencies"`
			} `json:"packages"`
			ExpectedPackages   int    `json:"expectedPackages"`
			ExpectedSourceGets int64  `json:"expectedSourceGets"`
			ExpectedError      string `json:"expectedError"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.FormatVersion != 1 || fixture.ExpectedPackageExecutions != 0 || fixture.ExpectedExternalEffects != 0 {
		t.Fatalf("fixture execution/effect preconditions changed: %+v", fixture)
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			source := &fixtureSource{bundles: make(map[string]Bundle)}
			for _, pkg := range tc.Packages {
				source.bundles[packageKey(Identity{Name: pkg.Name, Version: pkg.Version})] = resolutionBundle(t, pkg.Name, pkg.Version, pkg.Dependencies)
			}
			lock, err := Resolve(context.Background(), tc.Roots, source, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment(), nil)
			if got := errorCode(err); got != tc.ExpectedError {
				t.Fatalf("resolution error code=%q, want %q (err=%v)", got, tc.ExpectedError, err)
			}
			if int64(source.gets.Load()) != tc.ExpectedSourceGets {
				t.Fatalf("actual source bundle reads=%d, want %d", source.gets.Load(), tc.ExpectedSourceGets)
			}
			if tc.ExpectedError != "" {
				if err == nil {
					t.Fatal("expected failure")
				}
				if tc.ExpectedError == "resolution_cycle" && !errors.Is(err, ErrResolutionCycle) {
					t.Fatalf("cycle classification lost: %v", err)
				}
				if tc.ExpectedError == "resolution_conflict" && !errors.Is(err, ErrResolutionConflict) {
					t.Fatalf("conflict classification lost: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(lock.Packages) != tc.ExpectedPackages {
				t.Fatalf("locked packages=%d, want %d", len(lock.Packages), tc.ExpectedPackages)
			}
			if len(lock.Packages) != 2 || lock.Packages[0].Identity.Version != "2.1.0" {
				t.Fatalf("resolver did not select the highest compatible transitive version: %+v", lock.Packages)
			}
			first, err := lock.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			repeated, err := Resolve(context.Background(), tc.Roots, source, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment(), nil)
			if err != nil {
				t.Fatal(err)
			}
			second, err := repeated.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			if string(first) != string(second) {
				t.Fatalf("repeated resolution changed lock bytes\nfirst: %s\nsecond: %s", first, second)
			}
		})
	}
}

func TestLockCanonicalOrderAndRejectsUnboundArtifacts(t *testing.T) {
	root := resolutionBundle(t, "shop/root", "1.0.0", nil)
	verified, err := root.Verify(TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	lock := Lock{FormatVersion: 1, Roots: []Dependency{{Name: "shop/root", Version: "1.0.0"}}, Packages: []LockedPackage{{Identity: verified.Identity, ManifestDigest: verified.ManifestDigest, ArtifactDigest: verified.ArtifactDigest, Trust: verified.Trust}}}
	a, err := lock.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	lock.Roots[0].Version = "^1"
	if _, err := lock.Canonical(); errorCode(err) != "unsupported_version_range" {
		t.Fatalf("unsupported range was not explicit: %v", err)
	}
	lock.Roots[0].Version = "1.0.0"
	lock.Packages[0].ArtifactDigest = "sha256:" + string(make([]byte, 64))
	if _, err := lock.Canonical(); errorCode(err) != "invalid_lock" {
		t.Fatalf("malformed artifact digest accepted: %v", err)
	}
	if len(a) == 0 {
		t.Fatal("canonical lock unexpectedly empty")
	}
}

func TestResolverRejectsIncompatibleRuntimeRange(t *testing.T) {
	bundle := resolutionBundle(t, "shop/runtime-bound", "1.0.0", nil)
	bundle.Manifest.Compatibility.Runtimes["nodejs"] = ">=23.0.0"
	source := &fixtureSource{bundles: map[string]Bundle{packageKey(bundle.Manifest.Identity): bundle}}
	_, err := Resolve(context.Background(), []Dependency{{Name: "shop/runtime-bound", Version: "1.0.0"}}, source, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment(), nil)
	if errorCode(err) != "incompatible_runtime" || source.gets.Load() != 1 {
		t.Fatalf("runtime-incompatible candidate result=%v, source reads=%d", err, source.gets.Load())
	}
}

type fixtureSource struct {
	bundles map[string]Bundle
	gets    atomic.Int64
}

func (s *fixtureSource) Versions(_ context.Context, name string) ([]Identity, error) {
	var ids []Identity
	for _, bundle := range s.bundles {
		if bundle.Manifest.Identity.Name == name {
			ids = append(ids, bundle.Manifest.Identity)
		}
	}
	return ids, nil
}
func (s *fixtureSource) Get(_ context.Context, id Identity) (Bundle, error) {
	s.gets.Add(1)
	bundle, ok := s.bundles[packageKey(id)]
	if !ok {
		return Bundle{}, ErrNotFound
	}
	return cloneBundle(bundle), nil
}

func resolutionBundle(t *testing.T, name, version string, dependencies []Dependency) Bundle {
	t.Helper()
	bundle := fixtureBundle(t)
	bundle.Manifest.Identity = Identity{Name: name, Version: version}
	bundle.Manifest.Dependencies = append([]Dependency(nil), dependencies...)
	descriptor := *bundle.Manifest.Metadata.NodeDescriptor
	descriptor.Name, descriptor.Version = name, version
	bundle.Manifest.Metadata.NodeDescriptor = &descriptor
	bundle.Artifact = []byte("synthetic-package-artifact:" + name + "@" + version)
	bundle.Manifest.ArtifactDigest = ArtifactDigest(bundle.Artifact)
	bundle.Signature = nil
	if err := bundle.Manifest.Validate(); err != nil {
		t.Fatalf("invalid synthetic package: %v", err)
	}
	return bundle
}

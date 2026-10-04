package packagecontract

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPublishSupportsFullManifestAndArtifactCeilings(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "protocol-boundary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		FormatVersion            int        `json:"formatVersion"`
		ArtifactBytes            int        `json:"artifactBytes"`
		CanonicalManifestBytes   int        `json:"canonicalManifestBytes"`
		SignatureKeyIDBytes      int        `json:"signatureKeyIdBytes"`
		ExpectedRegistryRequests int32      `json:"expectedRegistryRequests"`
		ExpectedTrust            TrustLevel `json:"expectedTrust"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.FormatVersion != 1 || fixture.ArtifactBytes != MaxArtifactBytes || fixture.CanonicalManifestBytes != MaxManifestBytes || fixture.SignatureKeyIDBytes != MaxSignatureKeyIDBytes {
		t.Fatal("boundary fixture must match the independent contract ceilings")
	}
	bundle := fixtureBundle(t)
	bundle.Artifact = bytes.Repeat([]byte{'x'}, fixture.ArtifactBytes)
	bundle.Manifest.ArtifactDigest = ArtifactDigest(bundle.Artifact)
	for i := 0; i < MaxDependencies; i++ {
		prefix := fmt.Sprintf("dep-%03d", i)
		dep := Dependency{
			Name:    strings.Repeat("x", 128) + "/" + prefix + strings.Repeat("x", 128-len(prefix)),
			Version: "1.0.0" + strings.Repeat(" ", 251),
		}
		bundle.Manifest.Dependencies = append(bundle.Manifest.Dependencies, dep)
		if _, err := bundle.Manifest.Canonical(); err != nil {
			bundle.Manifest.Dependencies = bundle.Manifest.Dependencies[:len(bundle.Manifest.Dependencies)-1]
			break
		}
	}
	// Fill the remaining bytes with a valid bounded provenance URL. The
	// manifest's canonical ceiling is independent of its artifact ceiling.
	bundle.Manifest.Provenance.Source = "https://example.test/"
	canonical, err := bundle.Manifest.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	padding := MaxManifestBytes - len(canonical)
	if padding < 0 || len(bundle.Manifest.Provenance.Source)+padding > 2048 {
		t.Fatalf("boundary fixture cannot fit URL padding: %d", padding)
	}
	bundle.Manifest.Provenance.Source += strings.Repeat("x", padding)
	canonical, err = bundle.Manifest.Canonical()
	if err != nil || len(canonical) != MaxManifestBytes {
		t.Fatalf("manifest boundary bytes=%d err=%v", len(canonical), err)
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x47}, ed25519.SeedSize))
	keyID := strings.Repeat("k", fixture.SignatureKeyIDBytes)
	bundle.Signature, err = Sign(bundle.Manifest, keyID, key)
	if err != nil {
		t.Fatal(err)
	}
	policy := TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{keyID: key.Public().(ed25519.PublicKey)}}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	oldLimit := (MaxArtifactBytes+2)/3*4 + MaxManifestBytes
	if len(encoded) <= oldLimit {
		t.Fatalf("fixture does not reproduce old envelope limit: bytes=%d limit=%d", len(encoded), oldLimit)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var received Bundle
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBundleBytes)).Decode(&received); err != nil {
			http.Error(w, "invalid bundle", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(received)
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL, HTTPClient: server.Client()}
	verified, err := client.Publish(context.Background(), bundle, policy, fixtureEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if verified.Trust != fixture.ExpectedTrust {
		t.Fatalf("trust=%s, want %s", verified.Trust, fixture.ExpectedTrust)
	}
	if requests.Load() != fixture.ExpectedRegistryRequests {
		t.Fatalf("registry requests=%d, want %d", requests.Load(), fixture.ExpectedRegistryRequests)
	}
}

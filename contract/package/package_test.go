package packagecontract

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/tool"
)

func fixtureBundle(t *testing.T) Bundle {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "local-node-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var bundle Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func fixtureEnvironment() Environment {
	return Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}
}

func TestPackageGoldenIdentityAndCompatibility(t *testing.T) {
	bundle := fixtureBundle(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "local-node-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		ExpectedManifestDigest string `json:"expectedManifestDigest"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	verified, err := bundle.Verify(TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if verified.Trust != TrustUnsignedLocal || verified.Identity != bundle.Manifest.Identity {
		t.Fatalf("unexpected verification result: %+v", verified)
	}
	if verified.ManifestDigest != golden.ExpectedManifestDigest || verified.ArtifactDigest != bundle.Manifest.ArtifactDigest {
		t.Fatalf("missing content identity: %+v", verified)
	}

	reordered := bundle.Manifest
	reordered.Dependencies = append([]Dependency(nil), bundle.Manifest.Dependencies...)
	reordered.Dependencies = append(reordered.Dependencies, Dependency{Name: "shop/extra", Version: "1.0.0"})
	reversed := reordered
	reversed.Dependencies = []Dependency{reordered.Dependencies[1], reordered.Dependencies[0]}
	a, err := reordered.Digest()
	if err != nil {
		t.Fatal(err)
	}
	b, err := reversed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("dependency order changed canonical digest: %s != %s", a, b)
	}
}

func TestPackageMetadataIsRequiredTypedAndCanonical(t *testing.T) {
	bundle := fixtureBundle(t)
	metadata := bundle.Manifest.Metadata
	if metadata.NodeDescriptor == nil || metadata.WorkflowDocument != nil {
		t.Fatalf("node package metadata did not resolve to an existing node descriptor: %+v", metadata)
	}
	if metadata.CapabilityManifest == nil {
		t.Fatal("fixture must declare its agent policy explicitly")
	}
	if err := metadata.CapabilityManifest.Validate(); err != nil {
		t.Fatalf("existing capability manifest rejected: %v", err)
	}
	first, err := bundle.Manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}

	reordered := bundle.Manifest
	descriptor := *metadata.NodeDescriptor
	descriptor.InputSchema = json.RawMessage(`{"required":["sku","quantity"],"properties":{"quantity":{"minimum":1,"type":"integer"},"sku":{"type":"string"}},"additionalProperties":false,"type":"object"}`)
	reordered.Metadata.NodeDescriptor = &descriptor
	second, err := reordered.Digest()
	if err != nil || first != second {
		t.Fatalf("schema JSON order changed canonical package digest: first=%s second=%s err=%v", first, second, err)
	}

	missing := bundle.Manifest
	missing.Metadata = Metadata{}
	if err := missing.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("missing package contract metadata: got %v", err)
	}
	ordinaryNode := bundle.Manifest
	ordinaryNode.Metadata.CapabilityManifest = nil
	if err := ordinaryNode.Validate(); err != nil {
		t.Fatalf("ordinary node descriptor should carry its own effect/capability metadata: %v", err)
	}
	ordinaryBundle := cloneBundle(bundle)
	ordinaryBundle.Manifest.Metadata.CapabilityManifest = nil
	ordinaryPrivateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x46}, ed25519.SeedSize))
	ordinaryBundle.Signature, err = Sign(ordinaryBundle.Manifest, "ordinary-node", ordinaryPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryPolicy := TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{"ordinary-node": ordinaryPrivateKey.Public().(ed25519.PublicKey)}}
	verified, err := ordinaryBundle.Verify(ordinaryPolicy, fixtureEnvironment())
	if err != nil || verified.Trust != TrustTrusted {
		t.Fatalf("signed ordinary package without an agent manifest should verify as package trust only: result=%+v err=%v", verified, err)
	}
	// Package signature trust is not agent admission: the existing agent policy
	// contract rejects absent declarations, and admission still requires an
	// application-registered node/workflow in agent.Catalog.
	if err := (tool.Manifest{}).Validate(); !errors.Is(err, tool.ErrInvalidManifest) {
		t.Fatalf("package trust implicitly supplied agent policy: %v", err)
	}
	invalidAgentPolicy := bundle.Manifest
	legacy := tool.Manifest{Version: 1, Compatibility: "trusted-legacy"}
	invalidAgentPolicy.Metadata.CapabilityManifest = &legacy
	if err := invalidAgentPolicy.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("unsupported declared agent policy accepted: %v", err)
	}
	missingSchema := bundle.Manifest
	missingSchema.Metadata.NodeDescriptor.InputSchema = nil
	if err := missingSchema.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("missing input schema: got %v", err)
	}
	invalid := bundle.Manifest
	badDescriptor := *metadata.NodeDescriptor
	badDescriptor.InputSchema = json.RawMessage(`{"type":"unsupported"}`)
	invalid.Metadata.NodeDescriptor = &badDescriptor
	if err := invalid.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("invalid existing schema contract: got %v", err)
	}
}

func TestPackageMetadataPreflightBoundsBeforeSchemaValidation(t *testing.T) {
	oversizedSchema := fixtureBundle(t).Manifest
	descriptor := *oversizedSchema.Metadata.NodeDescriptor
	descriptor.InputSchema = bytes.Repeat([]byte{' '}, MaxManifestBytes+1)
	oversizedSchema.Metadata.NodeDescriptor = &descriptor
	if err := oversizedSchema.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("oversized native schema was not rejected by preflight: %v", err)
	}

	tooManyInstructions := fixtureBundle(t).Manifest
	workflow := &contract.Document{
		Workflow: contract.Workflow{
			Instructions: make([]contract.Instruction, maxPackageWorkflowInstructions+1),
			InputSchema:  bytes.Repeat([]byte{' '}, MaxManifestBytes+1),
		},
	}
	tooManyInstructions.Kind = "workflow"
	tooManyInstructions.Metadata = Metadata{WorkflowDocument: workflow}
	if err := tooManyInstructions.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("workflow count cap did not precede schema parsing: %v", err)
	}
}

func TestPackageMetadataTamperingInvalidatesSignature(t *testing.T) {
	bundle := fixtureBundle(t)
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x45}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var err error
	bundle.Signature, err = Sign(bundle.Manifest, "metadata-fixture", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	policy := TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{"metadata-fixture": publicKey}}
	if _, err := bundle.Verify(policy, fixtureEnvironment()); err != nil {
		t.Fatalf("valid typed metadata signature failed: %v", err)
	}

	tampered := cloneBundle(bundle)
	descriptor := *tampered.Manifest.Metadata.NodeDescriptor
	descriptor.InputSchema = json.RawMessage(`{"type":"object","additionalProperties":false}`)
	tampered.Manifest.Metadata.NodeDescriptor = &descriptor
	if _, err := tampered.Verify(policy, fixtureEnvironment()); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("metadata tampering did not invalidate signature: %v", err)
	}
}

func TestWorkflowPackageReusesValidatedContractDocument(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := contract.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	bundle := fixtureBundle(t)
	bundle.Manifest.Kind = "workflow"
	bundle.Manifest.Identity = Identity{Name: document.Workflow.Name, Version: document.Workflow.Version}
	bundle.Manifest.Metadata = Metadata{
		WorkflowDocument: &document,
	}
	bundle.Artifact = append([]byte(nil), data...)
	bundle.Manifest.ArtifactDigest = ArtifactDigest(bundle.Artifact)
	if _, err := bundle.Verify(TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()); err != nil {
		t.Fatalf("ordinary workflow document/schema contract rejected: %v", err)
	}
	agentWorkflow := cloneBundle(bundle)
	agentPolicy := tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}
	agentWorkflow.Manifest.Metadata.CapabilityManifest = &agentPolicy
	if _, err := agentWorkflow.Verify(TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()); err != nil {
		t.Fatalf("explicit existing workflow agent policy rejected: %v", err)
	}

	invalid := cloneBundle(bundle)
	invalid.Manifest.Metadata.WorkflowDocument.Workflow.InputSchema = json.RawMessage(`{"type":"not-a-portable-schema"}`)
	invalid.Manifest.ArtifactDigest = ArtifactDigest(invalid.Artifact)
	if err := invalid.Manifest.Validate(); storeErrorCode(err) != "invalid_metadata" {
		t.Fatalf("invalid workflow schema accepted: %v", err)
	}
}

func TestTrustAndCompatibilityFailClosed(t *testing.T) {
	bundle := fixtureBundle(t)
	if _, err := bundle.Verify(TrustPolicy{}, fixtureEnvironment()); !errors.Is(err, ErrSignatureRequired) {
		t.Fatalf("unsigned package without local policy: got %v", err)
	}
	for _, tc := range []struct {
		name string
		env  Environment
		code string
	}{
		{"engine", Environment{EngineVersion: "2.0.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}, "incompatible_engine"},
		{"schema", Environment{EngineVersion: "1.4.0", SchemaVersion: "2.0.0", RuntimeVersions: map[string]string{"nodejs": "22.18.0"}}, "incompatible_schema"},
		{"runtime", Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0", RuntimeVersions: map[string]string{"nodejs": "25.0.0"}}, "incompatible_runtime"},
		{"missing-runtime", Environment{EngineVersion: "1.4.0", SchemaVersion: "1.2.0"}, "incompatible_runtime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bundle.Verify(TrustPolicy{AllowUnsignedLocal: true}, tc.env)
			var contractErr *Error
			if !errors.As(err, &contractErr) || contractErr.Code != tc.code {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
}

func TestRejectsWhitespaceOnlyRangesEverywhere(t *testing.T) {
	base := fixtureBundle(t).Manifest
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"engine", func(m *Manifest) { m.Compatibility.Engine = " \t " }},
		{"schema", func(m *Manifest) { m.Compatibility.Schema = "\n" }},
		{"runtime", func(m *Manifest) { m.Compatibility.Runtimes["nodejs"] = " \t " }},
		{"dependency", func(m *Manifest) { m.Dependencies[0].Version = " \t " }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := base
			manifest.Dependencies = append([]Dependency(nil), base.Dependencies...)
			manifest.Compatibility.Runtimes = map[string]string{"nodejs": ">=20.0.0 <25.0.0"}
			tc.mutate(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatal("whitespace-only range accepted")
			}
		})
	}
}

func TestIdentityVersionUsesRangeNumericBounds(t *testing.T) {
	id := fixtureBundle(t).Manifest.Identity
	id.Version = "18446744073709551616.0.0"
	if err := id.Validate(); err == nil {
		t.Fatal("version component over uint64 was accepted")
	}
}

func TestSignatureVerificationUsesCanonicalManifest(t *testing.T) {
	bundle := fixtureBundle(t)
	// Fixed synthetic key seed keeps signature fixtures deterministic and is
	// test-only; it is not a publisher credential.
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var err error
	bundle.Signature, err = Sign(bundle.Manifest, "fixture-key", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	policy := TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{"fixture-key": publicKey}}
	verified, err := bundle.Verify(policy, fixtureEnvironment())
	if err != nil || verified.Trust != TrustTrusted {
		t.Fatalf("trusted signature failed: result=%+v err=%v", verified, err)
	}

	bundle.Artifact[0] ^= 0xff
	if _, err := bundle.Verify(policy, fixtureEnvironment()); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("tampered artifact: got %v", err)
	}
	bundle = fixtureBundle(t)
	bundle.Signature, err = Sign(bundle.Manifest, "unknown", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.Verify(TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{}}, fixtureEnvironment()); !errors.Is(err, ErrUnknownSigner) {
		t.Fatalf("unknown signer: got %v", err)
	}
	bundle.Signature.KeyID = "fixture-key"
	bundle.Signature.Value = strings.Repeat("A", 88)
	if _, err := bundle.Verify(policy, fixtureEnvironment()); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("bad signature: got %v", err)
	}
}

func TestSignatureInputLengthsAreBounded(t *testing.T) {
	bundle := fixtureBundle(t)
	publicKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x43}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	for _, tc := range []struct {
		name   string
		mutate func(*Signature)
	}{
		{"key-id", func(s *Signature) { s.KeyID = strings.Repeat("k", MaxSignatureKeyIDBytes+1) }},
		{"signature", func(s *Signature) { s.Value = strings.Repeat("A", MaxSignatureValueBytes+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle.Signature = &Signature{Algorithm: "ed25519", KeyID: "fixture-key", Value: strings.Repeat("A", MaxSignatureValueBytes)}
			tc.mutate(bundle.Signature)
			_, err := bundle.Verify(TrustPolicy{TrustedKeys: map[string]ed25519.PublicKey{"fixture-key": publicKey}}, fixtureEnvironment())
			var contractErr *Error
			if !errors.As(err, &contractErr) || contractErr.Code != "bad_signature" {
				t.Fatalf("got %v, want bounded bad signature", err)
			}
		})
	}
	if _, err := Sign(fixtureBundle(t).Manifest, strings.Repeat("k", MaxSignatureKeyIDBytes+1), make(ed25519.PrivateKey, ed25519.PrivateKeySize)); err == nil {
		t.Fatal("oversized signer key id accepted")
	}
}

func TestManifestRequiresLicenseAndSourceProvenance(t *testing.T) {
	base := fixtureBundle(t).Manifest
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
		code   string
	}{
		{"missing-license", func(m *Manifest) { m.License = "" }, "invalid_license"},
		{"missing-source", func(m *Manifest) { m.Provenance.Source = "" }, "invalid_provenance"},
		{"local-path-source", func(m *Manifest) { m.Provenance.Source = "file:///tmp/source" }, "invalid_provenance"},
		{"credential-source", func(m *Manifest) { m.Provenance.Source = "https://user:secret@example.test/repo" }, "invalid_provenance"},
		{"missing-revision", func(m *Manifest) { m.Provenance.Revision = "" }, "invalid_provenance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.mutate(&m)
			var contractErr *Error
			if err := m.Validate(); !errors.As(err, &contractErr) || contractErr.Code != tc.code {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
}

func TestInvalidManifestGoldenFixtures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "invalid-manifest-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		FormatVersion int                                              `json:"formatVersion"`
		Cases         []struct{ Name, Mutation, ExpectedError string } `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.FormatVersion != 1 {
		t.Fatalf("unsupported fixture format %d", fixture.FormatVersion)
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			manifest := fixtureBundle(t).Manifest
			switch tc.Mutation {
			case "invalid-name":
				manifest.Identity.Name = "../quote"
			case "overlong-name":
				manifest.Identity.Name = strings.Repeat("n", 258)
			case "prerelease-version":
				manifest.Identity.Version = "1.0.0-rc.1"
			case "overflowing-identity-version":
				manifest.Identity.Version = "18446744073709551616.0.0"
			case "missing-license":
				manifest.License = ""
			case "overlong-license":
				manifest.License = strings.Repeat("L", MaxLicenseBytes+1)
			case "missing-provenance":
				manifest.Provenance.Source = ""
			case "oversized-provenance-source":
				manifest.Provenance.Source = "https://example.test/" + strings.Repeat("x", 2048)
			case "invalid-engine-range":
				manifest.Compatibility.Engine = "latest"
			case "overflowing-engine-range":
				manifest.Compatibility.Engine = ">=18446744073709551616.0.0"
			case "invalid-dependency-range":
				manifest.Dependencies[0].Version = "^1.0.0"
			case "whitespace-engine-range":
				manifest.Compatibility.Engine = " \t "
			case "whitespace-schema-range":
				manifest.Compatibility.Schema = "\n"
			case "whitespace-runtime-range":
				manifest.Compatibility.Runtimes["nodejs"] = " \t "
			case "whitespace-dependency-range":
				manifest.Dependencies[0].Version = " \t "
			default:
				t.Fatalf("unknown fixture mutation %q", tc.Mutation)
			}
			var contractErr *Error
			if err := manifest.Validate(); !errors.As(err, &contractErr) || contractErr.Code != tc.ExpectedError {
				t.Fatalf("got %v, want code %s", err, tc.ExpectedError)
			}
		})
	}
}

func TestStoreBindsImmutableVersionsAndReturnsCopies(t *testing.T) {
	store := NewStore()
	bundle := fixtureBundle(t)
	policy, env := TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()
	if _, err := store.Publish(bundle, policy, env); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(bundle, policy, env); err != nil {
		t.Fatalf("identical publish should be idempotent: %v", err)
	}

	conflict := cloneBundle(bundle)
	conflict.Artifact = []byte("different immutable content")
	conflict.Manifest.ArtifactDigest = ArtifactDigest(conflict.Artifact)
	if _, err := store.Publish(conflict, policy, env); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("got %v, want version conflict", err)
	}

	fetched, _, err := store.Fetch(bundle.Manifest.Identity, policy, env)
	if err != nil {
		t.Fatal(err)
	}
	fetched.Artifact[0] ^= 0xff
	fetched.Manifest.Metadata.NodeDescriptor.Description = "caller mutation"
	fetched.Manifest.Metadata.NodeDescriptor.InputSchema[0] = ' '
	again, _, err := store.Fetch(bundle.Manifest.Identity, policy, env)
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Artifact) != string(bundle.Artifact) {
		t.Fatal("caller mutation escaped local store ownership")
	}
	if again.Manifest.Metadata.NodeDescriptor.Description != bundle.Manifest.Metadata.NodeDescriptor.Description || string(again.Manifest.Metadata.NodeDescriptor.InputSchema) != string(bundle.Manifest.Metadata.NodeDescriptor.InputSchema) {
		t.Fatal("metadata mutation escaped local store ownership")
	}
}

func TestStoreCapacityIsBoundedAndIdempotenceSurvivesSaturation(t *testing.T) {
	bundle := fixtureBundle(t)
	policy, env := TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()
	store, err := NewStoreWithLimits(StoreLimits{MaxPackages: 1, MaxArtifactBytes: int64(len(bundle.Artifact))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(bundle, policy, env); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(bundle, policy, env); err != nil {
		t.Fatalf("identical publish at capacity should succeed: %v", err)
	}

	countLimit := cloneBundle(bundle)
	countLimit.Manifest.Identity.Version = "1.0.1"
	countLimit.Manifest.Metadata.NodeDescriptor.Version = "1.0.1"
	if _, err := store.Publish(countLimit, policy, env); !errors.Is(err, ErrStoreFull) || storeErrorCode(err) != "store_capacity_exceeded" {
		t.Fatalf("package count saturation returned %v", err)
	}
	if packages, bytes := store.Usage(); packages != 1 || bytes != int64(len(bundle.Artifact)) {
		t.Fatalf("usage changed after rejected package: packages=%d bytes=%d", packages, bytes)
	}

	byteLimited, err := NewStoreWithLimits(StoreLimits{MaxPackages: 2, MaxArtifactBytes: int64(len(bundle.Artifact))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := byteLimited.Publish(bundle, policy, env); err != nil {
		t.Fatal(err)
	}
	countLimit.Manifest.Identity.Version = "1.0.2"
	countLimit.Manifest.Metadata.NodeDescriptor.Version = "1.0.2"
	if _, err := byteLimited.Publish(countLimit, policy, env); !errors.Is(err, ErrStoreFull) || storeErrorCode(err) != "store_capacity_exceeded" {
		t.Fatalf("artifact-byte saturation returned %v", err)
	}
	if packages, bytes := byteLimited.Usage(); packages != 1 || bytes != int64(len(bundle.Artifact)) {
		t.Fatalf("usage changed after byte-limit rejection: packages=%d bytes=%d", packages, bytes)
	}
}

func TestStoreHasFiniteDefaultsAndRejectsOverlargeConfiguration(t *testing.T) {
	store := NewStore()
	if limits := store.Limits(); limits.MaxPackages != DefaultStoreMaxPackages || limits.MaxArtifactBytes != DefaultStoreMaxArtifactBytes {
		t.Fatalf("unexpected defaults: %+v", limits)
	}
	if _, err := NewStoreWithLimits(StoreLimits{MaxPackages: HardStoreMaxPackages + 1}); err == nil {
		t.Fatal("configuration exceeded the hard package-count cap")
	}
}

func TestStoreCapacityFixtures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "store-capacity-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		FormatVersion             int         `json:"formatVersion"`
		ExpectedPackageExecutions int         `json:"expectedPackageExecutions"`
		Defaults                  StoreLimits `json:"defaults"`
		HardLimits                StoreLimits `json:"hardLimits"`
		Cases                     []struct {
			Name                  string `json:"name"`
			MaxPackages           int    `json:"maxPackages"`
			MaxArtifactBytes      int64  `json:"maxArtifactBytes"`
			SeedPackage           bool   `json:"seedPackage"`
			Operation             string `json:"operation"`
			ExpectedError         string `json:"expectedError"`
			ExpectedPackages      int    `json:"expectedPackages"`
			ExpectedArtifactBytes int64  `json:"expectedArtifactBytes"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.FormatVersion != 1 {
		t.Fatalf("unsupported fixture format %d", fixture.FormatVersion)
	}
	defaults := NewStore().Limits()
	if defaults != fixture.Defaults {
		t.Fatalf("default store limits=%+v, want %+v", defaults, fixture.Defaults)
	}
	if fixture.HardLimits.MaxPackages != HardStoreMaxPackages || fixture.HardLimits.MaxArtifactBytes != HardStoreMaxArtifactBytes {
		t.Fatalf("hard store limits=%+v", fixture.HardLimits)
	}
	bundle := fixtureBundle(t)
	if int64(len(bundle.Artifact)) != fixture.Cases[0].MaxArtifactBytes {
		t.Fatal("capacity fixture no longer matches golden artifact size")
	}
	policy, env := TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			store, err := NewStoreWithLimits(StoreLimits{MaxPackages: tc.MaxPackages, MaxArtifactBytes: tc.MaxArtifactBytes})
			if err != nil {
				t.Fatal(err)
			}
			if tc.SeedPackage {
				if _, err := store.Publish(bundle, policy, env); err != nil {
					t.Fatalf("seed package: %v", err)
				}
			}
			candidate := cloneBundle(bundle)
			switch tc.Operation {
			case "publish-same":
			case "publish-new-version":
				candidate.Manifest.Identity.Version = "1.0.1"
				candidate.Manifest.Metadata.NodeDescriptor.Version = "1.0.1"
			default:
				t.Fatalf("unknown capacity operation %q", tc.Operation)
			}
			_, err = store.Publish(candidate, policy, env)
			if tc.ExpectedError == "" && err != nil {
				t.Fatalf("unexpected publish error: %v", err)
			}
			if tc.ExpectedError != "" && storeErrorCode(err) != tc.ExpectedError {
				t.Fatalf("publish error=%v, want %s", err, tc.ExpectedError)
			}
			packages, artifactBytes := store.Usage()
			if packages != tc.ExpectedPackages || artifactBytes != tc.ExpectedArtifactBytes {
				t.Fatalf("usage=(%d,%d), want (%d,%d)", packages, artifactBytes, tc.ExpectedPackages, tc.ExpectedArtifactBytes)
			}
		})
	}
	if fixture.ExpectedPackageExecutions != 0 {
		t.Fatalf("fixture package executions=%d, want 0", fixture.ExpectedPackageExecutions)
	}
}

func storeErrorCode(err error) string {
	var contractErr *Error
	if errors.As(err, &contractErr) {
		return contractErr.Code
	}
	return ""
}

type protocolFixture struct {
	FormatVersion             int `json:"formatVersion"`
	ExpectedPackageExecutions int `json:"expectedPackageExecutions"`
	Cases                     []struct {
		Name            string `json:"name"`
		Method          string `json:"method"`
		Path            string `json:"path"`
		ResponseVariant string `json:"responseVariant,omitempty"`
		ExpectedStatus  int    `json:"expectedStatus"`
		ExpectedTrust   string `json:"expectedTrust,omitempty"`
		ExpectedError   string `json:"expectedError"`
	} `json:"cases"`
}

func TestRegistryProtocolFixtureAgainstLocalMock(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "packages", "protocol-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture protocolFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.FormatVersion != 1 {
		t.Fatalf("unsupported fixture format %d", fixture.FormatVersion)
	}

	localTrust := TrustPolicy{AllowUnsignedLocal: true}
	store := NewStore()
	bundle := fixtureBundle(t)
	if _, err := store.Publish(bundle, localTrust, fixtureEnvironment()); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	var executions atomic.Int32
	var tamperNext atomic.Bool
	var substituteNext atomic.Bool
	var denyNext atomic.Int32
	var lastStatus atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, protocolPrefix), "/")
		if len(parts) != 3 {
			http.Error(w, "bad package path", http.StatusBadRequest)
			return
		}
		id := Identity{Name: parts[0] + "/" + parts[1], Version: parts[2]}
		switch r.Method {
		case http.MethodGet:
			found, _, err := store.Fetch(id, localTrust, fixtureEnvironment())
			if err != nil {
				lastStatus.Store(http.StatusNotFound)
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if tamperNext.Swap(false) {
				found.Artifact = append(found.Artifact, '!')
			}
			lastStatus.Store(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(found)
		case http.MethodPut:
			if deniedStatus := denyNext.Swap(0); deniedStatus != 0 {
				lastStatus.Store(deniedStatus)
				http.Error(w, http.StatusText(int(deniedStatus)), int(deniedStatus))
				return
			}
			var incoming Bundle
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBundleBytes))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&incoming); err != nil {
				http.Error(w, "invalid bundle", http.StatusBadRequest)
				return
			}
			verified, err := store.Publish(incoming, localTrust, fixtureEnvironment())
			if errors.Is(err, ErrVersionConflict) {
				lastStatus.Store(http.StatusConflict)
				http.Error(w, "version conflict", http.StatusConflict)
				return
			}
			if err != nil {
				http.Error(w, "invalid bundle", http.StatusBadRequest)
				return
			}
			status := http.StatusCreated
			if verified.ManifestDigest == mustManifestDigest(t, bundle.Manifest) {
				status = http.StatusOK
			}
			responseBundle := incoming
			if substituteNext.Swap(false) {
				responseBundle.Artifact = []byte("different valid artifact, same immutable identity")
				responseBundle.Manifest.ArtifactDigest = ArtifactDigest(responseBundle.Artifact)
				status = http.StatusCreated
			}
			lastStatus.Store(int32(status))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(responseBundle)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL, HTTPClient: server.Client()}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			id := bundle.Manifest.Identity
			if strings.Contains(tc.Path, "/absent/") {
				id.Name = "shop/absent"
			}
			policy := localTrust
			env := fixtureEnvironment()
			if tc.ResponseVariant == "tampered-artifact" {
				tamperNext.Store(true)
			}
			if tc.ResponseVariant == "substituted-content" {
				substituteNext.Store(true)
			}
			if tc.ResponseVariant == "unauthenticated-publisher" {
				denyNext.Store(http.StatusUnauthorized)
			}
			if tc.ResponseVariant == "unauthorized-namespace" {
				denyNext.Store(http.StatusForbidden)
			}
			packagesBefore, bytesBefore := store.Usage()
			var gotErr error
			var verified Verified
			switch tc.Name {
			case "fetch-valid-local", "fetch-without-local-trust", "fetch-incompatible-engine", "fetch-missing", "fetch-tampered-artifact":
				if tc.Name == "fetch-without-local-trust" {
					policy = TrustPolicy{}
				}
				if tc.Name == "fetch-incompatible-engine" {
					env.EngineVersion = "2.0.0"
				}
				_, verified, gotErr = client.Fetch(context.Background(), id, policy, env)
			case "publish-identical", "publish-version-conflict", "publish-substituted-content", "publish-unauthenticated", "publish-namespace-forbidden":
				toPublish := cloneBundle(bundle)
				if tc.Name == "publish-version-conflict" {
					toPublish.Artifact = []byte("different immutable content")
					toPublish.Manifest.ArtifactDigest = ArtifactDigest(toPublish.Artifact)
				}
				verified, gotErr = client.Publish(context.Background(), toPublish, policy, env)
			default:
				t.Fatalf("fixture case has no consumer: %s", tc.Name)
			}
			if got, want := int(lastStatus.Load()), tc.ExpectedStatus; got != want {
				t.Fatalf("mock registry status=%d, want %d", got, want)
			}
			if tc.ExpectedStatus == http.StatusUnauthorized || tc.ExpectedStatus == http.StatusForbidden {
				packagesAfter, bytesAfter := store.Usage()
				if packagesAfter != packagesBefore || bytesAfter != bytesBefore {
					t.Fatalf("denied publish changed local store usage from (%d, %d) to (%d, %d)", packagesBefore, bytesBefore, packagesAfter, bytesAfter)
				}
			}
			if tc.ExpectedError == "" {
				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if string(verified.Trust) != tc.ExpectedTrust {
					t.Fatalf("trust=%q, want %q", verified.Trust, tc.ExpectedTrust)
				}
				return
			}
			if gotErr == nil {
				t.Fatalf("expected %s", tc.ExpectedError)
			}
			if tc.ExpectedError == "not_found" && errors.Is(gotErr, ErrNotFound) {
				return
			}
			if tc.ExpectedError == "version_conflict" && errors.Is(gotErr, ErrVersionConflict) {
				return
			}
			if tc.ExpectedError == "protocol_error" && errors.Is(gotErr, ErrProtocol) {
				return
			}
			var contractErr *Error
			if !errors.As(gotErr, &contractErr) || contractErr.Code != tc.ExpectedError {
				t.Fatalf("got %v, want code %s", gotErr, tc.ExpectedError)
			}
		})
	}
	if got, want := int(requests.Load()), len(fixture.Cases); got != want {
		t.Fatalf("mock requests=%d, want %d", got, want)
	}
	if got := executions.Load(); got != int32(fixture.ExpectedPackageExecutions) {
		t.Fatalf("package executions=%d, want %d", got, fixture.ExpectedPackageExecutions)
	}
}

func mustManifestDigest(t *testing.T, manifest Manifest) string {
	t.Helper()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestClientRejectsRegistryURLCredentialsAndBoundedResponse(t *testing.T) {
	client := Client{BaseURL: "https://user:secret@example.test"}
	if _, _, err := client.Fetch(context.Background(), fixtureBundle(t).Manifest.Identity, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()); err == nil {
		t.Fatal("credential-bearing registry URL accepted")
	}
	client.BaseURL = "https://example.test/" + strings.Repeat("x", MaxRegistryURLBytes)
	if _, _, err := client.Fetch(context.Background(), fixtureBundle(t).Manifest.Identity, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()); err == nil {
		t.Fatal("overlong registry URL accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, strings.Repeat("x", MaxBundleBytes+10))
	}))
	defer server.Close()
	client = Client{BaseURL: server.URL, HTTPClient: server.Client()}
	if _, _, err := client.Fetch(context.Background(), fixtureBundle(t).Manifest.Identity, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversized response: got %v", err)
	}
}

func TestClientRejectsResponseForDifferentPackageIdentity(t *testing.T) {
	bundle := fixtureBundle(t)
	bundle.Manifest.Identity = Identity{Name: "other/package", Version: "1.0.0"}
	bundle.Manifest.Metadata.NodeDescriptor.Name = "other/package"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bundle)
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL, HTTPClient: server.Client()}
	_, _, err := client.Fetch(context.Background(), fixtureBundle(t).Manifest.Identity, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment())
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want protocol identity mismatch", err)
	}
}

func TestReviewPublishRejectsSubstitutedContent(t *testing.T) {
	original := fixtureBundle(t)
	substitute := cloneBundle(original)
	substitute.Artifact = []byte("different artifact, same immutable version")
	substitute.Manifest.ArtifactDigest = ArtifactDigest(substitute.Artifact)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(substitute); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	verified, err := (Client{BaseURL: server.URL, HTTPClient: server.Client()}).Publish(context.Background(), original, TrustPolicy{AllowUnsignedLocal: true}, fixtureEnvironment())
	if err == nil {
		t.Fatalf("accepted substitution: uploaded=%s returned=%s", original.Manifest.ArtifactDigest, verified.ArtifactDigest)
	}
}

func TestReviewRejectsWhitespaceOnlyCompatibilityRange(t *testing.T) {
	bundle := fixtureBundle(t)
	bundle.Manifest.Compatibility.Engine = " \t "
	if err := bundle.Manifest.Validate(); err == nil {
		t.Fatal("blank compatibility constraint accepted")
	}
}

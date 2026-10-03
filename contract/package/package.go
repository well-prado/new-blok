// Package package defines immutable node/workflow package identities, manifests,
// compatibility requirements, provenance, signatures, and trust verification.
// It does not download, execute, or install package code.
package packagecontract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	ManifestVersion        = 1
	MaxArtifactBytes       = 8 << 20
	MaxManifestBytes       = 64 << 10
	MaxDependencies        = 256
	MaxRuntimeRequirements = 32
	MaxSignatureKeyIDBytes = 128
	MaxSignatureValueBytes = 88
)

var (
	ErrInvalid                  = errors.New("package: invalid package")
	ErrIncompatible             = errors.New("package: incompatible package")
	ErrDigestMismatch           = errors.New("package: digest mismatch")
	ErrSignatureRequired        = errors.New("package: trusted signature required")
	ErrUnknownSigner            = errors.New("package: signer is not trusted")
	ErrBadSignature             = errors.New("package: invalid signature")
	ErrVersionConflict          = errors.New("package: immutable version conflict")
	ErrNotFound                 = errors.New("package: not found")
	ErrProtocol                 = errors.New("package: registry protocol error")
	ErrStoreFull                = errors.New("package: local store capacity exceeded")
	ErrPublishedContentMismatch = errors.New("package: registry returned different published content")
)

type Error struct {
	Code    string `json:"code"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	if e.Path == "" {
		return e.Code + ": " + e.Message
	}
	return e.Code + " at " + e.Path + ": " + e.Message
}

func (e *Error) Unwrap() error {
	switch e.Code {
	case "invalid_manifest", "invalid_identity", "invalid_compatibility", "invalid_provenance", "invalid_license", "invalid_store_limits":
		return ErrInvalid
	case "incompatible_runtime", "incompatible_engine", "incompatible_schema", "incompatible_dependency":
		return ErrIncompatible
	case "artifact_digest_mismatch", "manifest_digest_mismatch":
		return ErrDigestMismatch
	case "signature_required":
		return ErrSignatureRequired
	case "unknown_signer":
		return ErrUnknownSigner
	case "bad_signature":
		return ErrBadSignature
	case "version_conflict":
		return ErrVersionConflict
	case "store_capacity_exceeded":
		return ErrStoreFull
	case "published_content_mismatch":
		return ErrPublishedContentMismatch
	case "package_not_found":
		return ErrNotFound
	default:
		return nil
	}
}

type Identity struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Dependency struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	ManifestDigest string `json:"manifestDigest,omitempty"`
}

type Compatibility struct {
	Engine   string            `json:"engine"`
	Schema   string            `json:"schema"`
	Runtimes map[string]string `json:"runtimes,omitempty"`
}

type Provenance struct {
	Source   string `json:"source"`
	Revision string `json:"revision"`
	Builder  string `json:"builder"`
}

type Manifest struct {
	FormatVersion  int           `json:"formatVersion"`
	Identity       Identity      `json:"identity"`
	Kind           string        `json:"kind"`
	ArtifactDigest string        `json:"artifactDigest"`
	Dependencies   []Dependency  `json:"dependencies,omitempty"`
	Compatibility  Compatibility `json:"compatibility"`
	License        string        `json:"license"`
	Provenance     Provenance    `json:"provenance"`
}

type Signature struct {
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"keyId"`
	Value     string `json:"value"`
}

// Bundle is the complete portable package. JSON encodes Artifact as base64.
type Bundle struct {
	Manifest  Manifest   `json:"manifest"`
	Signature *Signature `json:"signature,omitempty"`
	Artifact  []byte     `json:"artifact"`
}

type TrustLevel string

const (
	TrustUnsignedLocal TrustLevel = "unsigned-local"
	TrustTrusted       TrustLevel = "trusted"
)

// TrustPolicy must only allow unsigned packages when the caller has established
// that bytes came from an explicitly local source. A registry's TLS/auth policy
// is separate from package publisher signature verification.
type TrustPolicy struct {
	AllowUnsignedLocal bool
	TrustedKeys        map[string]ed25519.PublicKey
}

type Environment struct {
	EngineVersion   string
	SchemaVersion   string
	RuntimeVersions map[string]string
}

type Verified struct {
	Identity       Identity   `json:"identity"`
	ManifestDigest string     `json:"manifestDigest"`
	ArtifactDigest string     `json:"artifactDigest"`
	Trust          TrustLevel `json:"trust"`
}

var (
	namePart         = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	semverRE         = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	digestRE         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	licenseRE        = regexp.MustCompile(`^(LicenseRef-[A-Za-z0-9.-]+|[A-Za-z0-9][A-Za-z0-9.-]*)$`)
	revisionRE       = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
	signatureKeyIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
)

func (id Identity) Validate() error {
	parts := strings.Split(id.Name, "/")
	if len(parts) != 2 || !namePart.MatchString(parts[0]) || !namePart.MatchString(parts[1]) {
		return &Error{Code: "invalid_identity", Path: "identity.name", Message: "name must be namespace/name using lowercase ASCII letters, digits, dot, underscore, or hyphen"}
	}
	if !semverRE.MatchString(id.Version) {
		return &Error{Code: "invalid_identity", Path: "identity.version", Message: "version must be stable major.minor.patch semantic version"}
	}
	if len(id.Version) > 62 {
		return &Error{Code: "invalid_identity", Path: "identity.version", Message: "version components must fit unsigned 64-bit values"}
	}
	if _, ok := parseVersion(id.Version); !ok {
		return &Error{Code: "invalid_identity", Path: "identity.version", Message: "version components must fit unsigned 64-bit values"}
	}
	return nil
}

func (m Manifest) Validate() error {
	if m.FormatVersion != ManifestVersion {
		return &Error{Code: "invalid_manifest", Path: "formatVersion", Message: "unsupported package manifest version"}
	}
	if err := m.Identity.Validate(); err != nil {
		return err
	}
	if m.Kind != "node" && m.Kind != "workflow" {
		return &Error{Code: "invalid_manifest", Path: "kind", Message: "kind must be node or workflow"}
	}
	if !digestRE.MatchString(m.ArtifactDigest) {
		return &Error{Code: "invalid_manifest", Path: "artifactDigest", Message: "artifact digest must use sha256 and 64 lowercase hex characters"}
	}
	if !licenseRE.MatchString(m.License) {
		return &Error{Code: "invalid_license", Path: "license", Message: "license must be an SPDX identifier or LicenseRef identifier"}
	}
	if err := validateProvenance(m.Provenance); err != nil {
		return err
	}
	if len(m.Dependencies) > MaxDependencies {
		return &Error{Code: "invalid_manifest", Path: "dependencies", Message: "dependency count exceeds the 256 entry limit"}
	}
	if len(m.Compatibility.Runtimes) > MaxRuntimeRequirements {
		return &Error{Code: "invalid_compatibility", Path: "compatibility.runtimes", Message: "runtime requirement count exceeds the 32 entry limit"}
	}
	if err := validateRange(m.Compatibility.Engine); err != nil || m.Compatibility.Engine == "" {
		return &Error{Code: "invalid_compatibility", Path: "compatibility.engine", Message: "engine must declare a supported semantic version range"}
	}
	if err := validateRange(m.Compatibility.Schema); err != nil || m.Compatibility.Schema == "" {
		return &Error{Code: "invalid_compatibility", Path: "compatibility.schema", Message: "schema must declare a supported semantic version range"}
	}
	for runtime, constraint := range m.Compatibility.Runtimes {
		if !namePart.MatchString(runtime) || validateRange(constraint) != nil {
			return &Error{Code: "invalid_compatibility", Path: "compatibility.runtimes", Message: "runtime names and version ranges must be valid"}
		}
	}
	seen := make(map[string]bool, len(m.Dependencies))
	for i, dep := range m.Dependencies {
		path := fmt.Sprintf("dependencies[%d]", i)
		id := Identity{Name: dep.Name, Version: "1.0.0"}
		if err := id.Validate(); err != nil {
			return &Error{Code: "invalid_manifest", Path: path + ".name", Message: "dependency name must be namespace/name"}
		}
		if err := validateRange(dep.Version); err != nil {
			return &Error{Code: "invalid_manifest", Path: path + ".version", Message: "dependency must declare a supported version range"}
		}
		if dep.ManifestDigest != "" && !digestRE.MatchString(dep.ManifestDigest) {
			return &Error{Code: "invalid_manifest", Path: path + ".manifestDigest", Message: "dependency digest is invalid"}
		}
		if seen[dep.Name] {
			return &Error{Code: "invalid_manifest", Path: path + ".name", Message: "dependency names must be unique"}
		}
		seen[dep.Name] = true
	}
	return nil
}

func validateProvenance(p Provenance) error {
	u, err := url.Parse(p.Source)
	if err != nil || len(p.Source) > 2048 || (u.Scheme != "https" && u.Scheme != "git+https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return &Error{Code: "invalid_provenance", Path: "provenance.source", Message: "source must be an HTTPS or git+HTTPS URL without credentials or fragment"}
	}
	if !revisionRE.MatchString(p.Revision) {
		return &Error{Code: "invalid_provenance", Path: "provenance.revision", Message: "revision must be a hexadecimal source commit"}
	}
	if strings.TrimSpace(p.Builder) == "" || len(p.Builder) > 256 || strings.ContainsAny(p.Builder, "\x00\r\n") {
		return &Error{Code: "invalid_provenance", Path: "provenance.builder", Message: "builder identity is required and bounded"}
	}
	return nil
}

func (m Manifest) Canonical() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	c := m
	c.Dependencies = append([]Dependency(nil), m.Dependencies...)
	sort.Slice(c.Dependencies, func(i, j int) bool { return c.Dependencies[i].Name < c.Dependencies[j].Name })
	if m.Compatibility.Runtimes != nil {
		c.Compatibility.Runtimes = make(map[string]string, len(m.Compatibility.Runtimes))
		for k, v := range m.Compatibility.Runtimes {
			c.Compatibility.Runtimes[k] = v
		}
	}
	canonical, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaxManifestBytes {
		return nil, &Error{Code: "invalid_manifest", Path: "manifest", Message: "canonical manifest exceeds the 64 KiB limit"}
	}
	return canonical, nil
}

func (m Manifest) Digest() (string, error) {
	b, err := m.Canonical()
	if err != nil {
		return "", err
	}
	return digestBytes(b), nil
}

func ArtifactDigest(artifact []byte) string { return digestBytes(artifact) }

func digestBytes(data []byte) string {
	d := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(d[:])
}

func (b Bundle) Verify(policy TrustPolicy, environment Environment) (Verified, error) {
	if err := validateArtifactSize(b.Artifact); err != nil {
		return Verified{}, err
	}
	if err := b.Manifest.Validate(); err != nil {
		return Verified{}, err
	}
	artifactDigest := ArtifactDigest(b.Artifact)
	if artifactDigest != b.Manifest.ArtifactDigest {
		return Verified{}, &Error{Code: "artifact_digest_mismatch", Path: "artifact", Message: "artifact bytes do not match manifest digest"}
	}
	manifestDigest, err := b.Manifest.Digest()
	if err != nil {
		return Verified{}, err
	}
	if err := checkCompatibility(b.Manifest.Compatibility, environment); err != nil {
		return Verified{}, err
	}
	if b.Signature == nil {
		if !policy.AllowUnsignedLocal {
			return Verified{}, &Error{Code: "signature_required", Path: "signature", Message: "unsigned package is allowed only by an explicit local-source policy"}
		}
		return Verified{Identity: b.Manifest.Identity, ManifestDigest: manifestDigest, ArtifactDigest: artifactDigest, Trust: TrustUnsignedLocal}, nil
	}
	if b.Signature.Algorithm != "ed25519" || len(b.Signature.KeyID) > MaxSignatureKeyIDBytes || !signatureKeyIDRE.MatchString(b.Signature.KeyID) || len(b.Signature.Value) != MaxSignatureValueBytes {
		return Verified{}, &Error{Code: "bad_signature", Path: "signature", Message: "signature must use ed25519 with a bounded key id and 64-byte base64 signature"}
	}
	key, ok := policy.TrustedKeys[b.Signature.KeyID]
	if !ok || len(key) != ed25519.PublicKeySize {
		return Verified{}, &Error{Code: "unknown_signer", Path: "signature.keyId", Message: "signature key is not present in the caller's trust policy"}
	}
	sig, err := base64.StdEncoding.DecodeString(b.Signature.Value)
	canonical, canonicalErr := b.Manifest.Canonical()
	if err != nil || canonicalErr != nil || !ed25519.Verify(key, canonical, sig) {
		return Verified{}, &Error{Code: "bad_signature", Path: "signature", Message: "signature does not verify over canonical manifest bytes"}
	}
	return Verified{Identity: b.Manifest.Identity, ManifestDigest: manifestDigest, ArtifactDigest: artifactDigest, Trust: TrustTrusted}, nil
}

func validateArtifactSize(artifact []byte) error {
	if len(artifact) == 0 || len(artifact) > MaxArtifactBytes {
		return &Error{Code: "invalid_manifest", Path: "artifact", Message: "artifact must be nonempty and at most 8 MiB"}
	}
	return nil
}

func Sign(m Manifest, keyID string, privateKey ed25519.PrivateKey) (*Signature, error) {
	if len(keyID) > MaxSignatureKeyIDBytes || !signatureKeyIDRE.MatchString(keyID) || len(privateKey) != ed25519.PrivateKeySize {
		return nil, &Error{Code: "invalid_manifest", Path: "signature", Message: "a bounded key id and Ed25519 private key are required"}
	}
	b, err := m.Canonical()
	if err != nil {
		return nil, err
	}
	return &Signature{Algorithm: "ed25519", KeyID: keyID, Value: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, b))}, nil
}

func checkCompatibility(c Compatibility, env Environment) error {
	if !satisfies(env.EngineVersion, c.Engine) {
		return &Error{Code: "incompatible_engine", Path: "compatibility.engine", Message: "engine version does not satisfy package range"}
	}
	if !satisfies(env.SchemaVersion, c.Schema) {
		return &Error{Code: "incompatible_schema", Path: "compatibility.schema", Message: "schema version does not satisfy package range"}
	}
	for name, constraint := range c.Runtimes {
		version, ok := env.RuntimeVersions[name]
		if !ok || !satisfies(version, constraint) {
			return &Error{Code: "incompatible_runtime", Path: "compatibility.runtimes." + name, Message: "runtime is missing or its version does not satisfy package range"}
		}
	}
	return nil
}

// Version ranges are a deliberately small, portable subset: whitespace-separated
// exact or comparator clauses, such as ">=1.2.0 <2.0.0". OR and prereleases are
// rejected so every consumer evaluates the same bounded grammar.
func validateRange(expression string) error {
	if expression == "" || len(expression) > 256 {
		return ErrInvalid
	}
	clauses := strings.Fields(expression)
	if len(clauses) == 0 {
		return ErrInvalid
	}
	for _, clause := range clauses {
		for _, operator := range []string{">=", "<=", ">", "<", "="} {
			if strings.HasPrefix(clause, operator) {
				clause = strings.TrimPrefix(clause, operator)
				break
			}
		}
		if _, ok := parseVersion(clause); !ok {
			return ErrInvalid
		}
	}
	return nil
}

func satisfies(version, expression string) bool {
	got, ok := parseVersion(version)
	if !ok || validateRange(expression) != nil {
		return false
	}
	for _, clause := range strings.Fields(expression) {
		op := "="
		for _, candidate := range []string{">=", "<=", ">", "<", "="} {
			if strings.HasPrefix(clause, candidate) {
				op = candidate
				clause = strings.TrimPrefix(clause, candidate)
				break
			}
		}
		want, ok := parseVersion(clause)
		if !ok {
			return false
		}
		cmp := compareVersion(got, want)
		if (op == "=" && cmp != 0) || (op == ">" && cmp <= 0) || (op == ">=" && cmp < 0) || (op == "<" && cmp >= 0) || (op == "<=" && cmp > 0) {
			return false
		}
	}
	return true
}

type version struct{ major, minor, patch uint64 }

func parseVersion(value string) (version, bool) {
	parts := semverRE.FindStringSubmatch(value)
	if parts == nil {
		return version{}, false
	}
	var v version
	var err error
	if v.major, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
		return version{}, false
	}
	if v.minor, err = strconv.ParseUint(parts[2], 10, 64); err != nil {
		return version{}, false
	}
	if v.patch, err = strconv.ParseUint(parts[3], 10, 64); err != nil {
		return version{}, false
	}
	return v, true
}

func compareVersion(a, b version) int {
	if a.major != b.major {
		if a.major < b.major {
			return -1
		}
		return 1
	}
	if a.minor != b.minor {
		if a.minor < b.minor {
			return -1
		}
		return 1
	}
	if a.patch != b.patch {
		if a.patch < b.patch {
			return -1
		}
		return 1
	}
	return 0
}

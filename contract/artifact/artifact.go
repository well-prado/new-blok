// Package artifact owns immutable execution artifact identity. It has no
// filesystem, registry, runtime or deployment provider dependency.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

type Error struct {
	Code    string
	Path    string
	Message string
}

func (e *Error) Error() string {
	if e.Path == "" {
		return e.Code + ": " + e.Message
	}
	return e.Code + " at " + e.Path + ": " + e.Message
}

type Manifest struct {
	Name                string   `json:"name"`
	Version             string   `json:"version"`
	WorkflowDigest      string   `json:"workflowDigest"`
	NativeBinaryDigest  string   `json:"nativeBinaryDigest"`
	WorkerCatalogDigest string   `json:"workerCatalogDigest,omitempty"`
	SchemaDigests       []string `json:"schemaDigests"`
	LockDigest          string   `json:"lockDigest"`
	CompilerDigest      string   `json:"compilerDigest"`
	CheckpointFormat    string   `json:"checkpointFormat"`
}

type Checkpoint struct {
	ArtifactDigest   string `json:"artifactDigest"`
	CheckpointFormat string `json:"checkpointFormat"`
}

type Store struct {
	byDigest  map[string]Manifest
	byVersion map[string]string
}

var semver = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func (m Manifest) Validate() error {
	if m.Name == "" || strings.Contains(m.Name, `\`) || strings.HasPrefix(m.Name, "/") || strings.Contains(m.Name, "..") {
		return &Error{Code: "invalid_manifest_name", Path: "name", Message: "manifest name must be a namespace, not a machine-local path"}
	}
	if !semver.MatchString(m.Version) {
		return &Error{Code: "invalid_version", Path: "version", Message: "version must be major.minor.patch"}
	}
	for _, item := range []struct{ name, value string }{
		{"workflowDigest", m.WorkflowDigest}, {"nativeBinaryDigest", m.NativeBinaryDigest},
		{"workerCatalogDigest", m.WorkerCatalogDigest}, {"lockDigest", m.LockDigest}, {"compilerDigest", m.CompilerDigest},
	} {
		if item.value != "" && !digest.MatchString(item.value) {
			return &Error{Code: "invalid_digest", Path: item.name, Message: "digest must be sha256 followed by 64 lowercase hex characters"}
		}
	}
	if m.WorkflowDigest == "" || m.NativeBinaryDigest == "" || m.LockDigest == "" || m.CompilerDigest == "" || m.CheckpointFormat == "" {
		return &Error{Code: "incomplete_manifest", Message: "execution manifest is missing a required identity"}
	}
	for i, d := range m.SchemaDigests {
		if !digest.MatchString(d) {
			return &Error{Code: "invalid_digest", Path: fmt.Sprintf("schemaDigests[%d]", i), Message: "schema digest is invalid"}
		}
	}
	if strings.ContainsAny(m.CheckpointFormat, `/\\`) || strings.Contains(m.CheckpointFormat, "..") {
		return &Error{Code: "invalid_checkpoint_format", Path: "checkpointFormat", Message: "checkpoint format must not contain a machine-local path"}
	}
	return nil
}

func (m Manifest) Canonical() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	c := m
	c.SchemaDigests = append([]string(nil), m.SchemaDigests...)
	sort.Strings(c.SchemaDigests)
	return json.Marshal(c)
}

func (m Manifest) Digest() (string, error) {
	b, err := m.Canonical()
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func NewStore() *Store {
	return &Store{byDigest: map[string]Manifest{}, byVersion: map[string]string{}}
}

func (s *Store) Register(m Manifest) (string, error) {
	if s == nil {
		return "", &Error{Code: "nil_store", Message: "artifact store is nil"}
	}
	d, err := m.Digest()
	if err != nil {
		return "", err
	}
	if old, ok := s.byVersion[m.Name+"@"+m.Version]; ok && old != d {
		return "", &Error{Code: "artifact_version_conflict", Message: "version is already bound to a different artifact digest"}
	}
	s.byDigest[d] = m
	s.byVersion[m.Name+"@"+m.Version] = d
	return d, nil
}

func (s *Store) Locate(digest string) (Manifest, bool) {
	if s == nil {
		return Manifest{}, false
	}
	m, ok := s.byDigest[digest]
	return m, ok
}

func VerifyCheckpoint(m Manifest, c Checkpoint) error {
	if _, err := m.Digest(); err != nil {
		return err
	}
	if c.ArtifactDigest == "" || c.ArtifactDigest != mustDigest(m) || c.CheckpointFormat != m.CheckpointFormat {
		return &Error{Code: "incompatible_checkpoint", Message: "checkpoint identity does not match the execution artifact"}
	}
	return nil
}

func mustDigest(m Manifest) string { d, _ := m.Digest(); return d }

func CanonicalJSON(data []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON values")
		}
		return nil, fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return json.Marshal(v)
}

// Package approval defines durable, digest-bound approval and publication
// gates. It does not infer authority from model content or execution output.
package approval

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrDenied   = errors.New("approval: denied")
	ErrStale    = errors.New("approval: stale or mismatched")
	ErrEvidence = errors.New("approval: trusted evidence required")
)

type Proposal struct {
	Action         string   `json:"action"`
	InputDigest    string   `json:"inputDigest"`
	Workflow       string   `json:"workflow"`
	ArtifactDigest string   `json:"artifactDigest"`
	Effects        []string `json:"effects"`
	Scope          []string `json:"scope"`
}
type Decision struct {
	ID             string    `json:"id"`
	ProposalDigest string    `json:"proposalDigest"`
	Reviewer       string    `json:"reviewer"`
	Scope          []string  `json:"scope"`
	ExpiresAt      time.Time `json:"expiresAt"`
	Approved       bool      `json:"approved"`
}
type Assertion struct {
	Name          string `json:"name"`
	Digest        string `json:"digest"`
	Source        string `json:"source"`
	Deterministic bool   `json:"deterministic"`
}
type Request struct {
	Proposal        Proposal
	ApprovalID      string
	Assertions      []Assertion
	RequireEvidence bool
}

type Store interface {
	Put(Decision) error
	Get(string) (Decision, bool, error)
}

func Digest(p Proposal) (string, error) {
	p.Effects = sortedUnique(p.Effects)
	p.Scope = sortedUnique(p.Scope)
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func Authorize(store Store, now time.Time, req Request) error {
	if store == nil || req.ApprovalID == "" {
		return ErrDenied
	}
	want, err := Digest(req.Proposal)
	if err != nil {
		return err
	}
	decision, ok, err := store.Get(req.ApprovalID)
	if err != nil {
		return err
	}
	if !ok || !decision.Approved || decision.ProposalDigest != want || decision.Reviewer == "" || !decision.ExpiresAt.After(now) {
		return ErrStale
	}
	if !subset(decision.Scope, req.Proposal.Scope) {
		return ErrDenied
	}
	if req.RequireEvidence && !trustedEvidence(req.Assertions) {
		return ErrEvidence
	}
	return nil
}

func Publish(store Store, now time.Time, req Request, output []byte) ([]byte, error) {
	if err := Authorize(store, now, req); err != nil {
		return nil, err
	}
	return append([]byte(nil), output...), nil
}
func trustedEvidence(values []Assertion) bool {
	for _, value := range values {
		if value.Name != "" && value.Digest != "" && value.Source == "deterministic" && value.Deterministic {
			return true
		}
	}
	return false
}
func subset(narrow, broad []string) bool {
	for _, value := range narrow {
		found := false
		for _, candidate := range broad {
			if value == candidate {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func sortedUnique(values []string) []string {
	set := map[string]bool{}
	for _, value := range values {
		set[value] = true
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type FileStore struct {
	path string
	mu   sync.Mutex
}
type fileState struct {
	Decisions map[string]Decision `json:"decisions"`
}

func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("approval: store path required")
	}
	return &FileStore{path: path}, nil
}
func (s *FileStore) Put(decision Decision) error {
	if decision.ID == "" {
		return errors.New("approval: decision id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.read()
	if err != nil {
		return err
	}
	state.Decisions[decision.ID] = decision
	return s.write(state)
}
func (s *FileStore) Get(id string) (Decision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.read()
	if err != nil {
		return Decision{}, false, err
	}
	decision, ok := state.Decisions[id]
	return decision, ok, nil
}
func (s *FileStore) read() (fileState, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return fileState{Decisions: map[string]Decision{}}, nil
	}
	if err != nil {
		return fileState{}, err
	}
	var state fileState
	if err := json.Unmarshal(data, &state); err != nil {
		return fileState{}, fmt.Errorf("approval: corrupt store: %w", err)
	}
	if state.Decisions == nil {
		state.Decisions = map[string]Decision{}
	}
	return state, nil
}
func (s *FileStore) write(state fileState) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), "approval-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, s.path)
}

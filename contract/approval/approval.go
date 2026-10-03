// Package approval defines immutable, execution-bound review decisions.
// Authentication and deterministic evidence are application-owned ports;
// caller JSON and model text cannot supply either authority.
package approval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

var (
	ErrDenied   = errors.New("approval: denied")
	ErrStale    = errors.New("approval: stale or mismatched")
	ErrEvidence = errors.New("approval: trusted evidence required")
	ErrConflict = errors.New("approval: immutable decision conflict")
	ErrCapacity = errors.New("approval: decision capacity exhausted")
)

const MaxRecordBytes = 16 << 10

type Proposal struct {
	Action         string   `json:"action"`
	InputDigest    string   `json:"inputDigest"`
	Workflow       string   `json:"workflow"`
	ArtifactDigest string   `json:"artifactDigest"`
	ToolDigest     string   `json:"toolDigest,omitempty"`
	RunID          string   `json:"runId"`
	InvocationPath string   `json:"invocationPath"`
	IterationPath  string   `json:"iterationPath"`
	Effects        []string `json:"effects"`
	Scope          []string `json:"scope"`
}

type Decision struct {
	ID             string    `json:"id"`
	ProposalDigest string    `json:"proposalDigest"`
	Reviewer       string    `json:"reviewer"`
	Scope          []string  `json:"scope"`
	ExpiresAt      time.Time `json:"expiresAt"`
	RecordedAt     time.Time `json:"recordedAt"`
	Approved       bool      `json:"approved"`
}

// Assertion is untrusted evidence to inspect, never an attestation. Legacy
// Source/Deterministic fields are retained solely to reject forged fixtures.
type Assertion struct {
	Name          string `json:"name"`
	Digest        string `json:"digest"`
	Source        string `json:"source"`
	Deterministic bool   `json:"deterministic"`
}

type Request struct {
	Proposal   Proposal
	ApprovalID string
}

// Reader returns only authenticated, committed decisions. The concrete store
// has no unrestricted Put: Record resolves the reviewer through trusted code.
type Reader interface {
	Get(context.Context, string) (Decision, bool, error)
}

// ReviewAuthorizer authenticates the reviewer from a trusted channel (normally
// context), authorizes this exact proposal/grant, and returns an audit identity.
// A reviewer name in a model request is never passed as authority.
type ReviewAuthorizer interface {
	AuthorizeReview(context.Context, Proposal, []string) (string, error)
}

func BytesDigest(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func validDigest(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(s[7:])
	return err == nil && s == strings.ToLower(s)
}

func Digest(p Proposal) (string, error) {
	if !validDigest(p.InputDigest) || !validDigest(p.ArtifactDigest) ||
		(p.ToolDigest != "" && !validDigest(p.ToolDigest)) ||
		!validText(p.Action) || !validText(p.Workflow) || !validText(p.RunID) ||
		!validText(p.InvocationPath) || !validText(p.IterationPath) ||
		!validSet(p.Scope) || !validSet(p.Effects) {
		return "", ErrDenied
	}
	p = CloneProposal(p)
	p.Scope = sortedUnique(p.Scope)
	p.Effects = sortedUnique(p.Effects)
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	if len(b) > MaxRecordBytes {
		return "", ErrCapacity
	}
	return BytesDigest(b), nil
}

func validText(s string) bool {
	return s != "" && len(s) <= 512 && !strings.ContainsAny(s, "\x00\r\n")
}

func validSet(values []string) bool {
	if len(values) > 64 {
		return false
	}
	for _, v := range values {
		if !validText(v) {
			return false
		}
	}
	return true
}

func CloneProposal(p Proposal) Proposal {
	p.Scope = append([]string(nil), p.Scope...)
	p.Effects = append([]string(nil), p.Effects...)
	return p
}

func Authorize(ctx context.Context, reader Reader, now time.Time, req Request) error {
	if reader == nil || !validText(req.ApprovalID) || now.IsZero() {
		return ErrDenied
	}
	want, err := Digest(req.Proposal)
	if err != nil {
		return err
	}
	d, ok, err := reader.Get(ctx, req.ApprovalID)
	if err != nil {
		return err
	}
	if !ok || d.ID != req.ApprovalID || !d.Approved || d.ProposalDigest != want ||
		!validText(d.Reviewer) || d.RecordedAt.IsZero() || d.RecordedAt.After(now) ||
		!d.ExpiresAt.After(now) || !d.ExpiresAt.After(d.RecordedAt) {
		return ErrStale
	}
	// Requested authority must fit within the authenticated reviewer's grant.
	if !validSet(d.Scope) || !Subset(req.Proposal.Scope, d.Scope) {
		return ErrDenied
	}
	return nil
}

// Subset performs exact capability matching; there are no wildcard grants.
func Subset(narrow, broad []string) bool {
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
	for _, v := range values {
		set[v] = true
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

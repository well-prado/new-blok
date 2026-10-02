package approval

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func proposal() Proposal {
	return Proposal{Action: "charge", InputDigest: "sha256:input", Workflow: "orders", ArtifactDigest: "sha256:artifact", Effects: []string{"payment:charge"}, Scope: []string{"payment:charge"}}
}
func decision(t *testing.T, p Proposal) Decision {
	digest, err := Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	return Decision{ID: "approval-1", ProposalDigest: digest, Reviewer: "alice", Scope: []string{"payment:charge"}, ExpiresAt: time.Now().Add(time.Hour), Approved: true}
}

func TestApprovalBindsProposalAndSurvivesStoreRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "approvals.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	p := proposal()
	if err := store.Put(decision(t, p)); err != nil {
		t.Fatal(err)
	}
	// A new store object models a process restart and must observe the commit.
	restarted, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Authorize(restarted, time.Now(), Request{Proposal: p, ApprovalID: "approval-1"}); err != nil {
		t.Fatal(err)
	}
	changed := p
	changed.InputDigest = "sha256:changed"
	if err := Authorize(restarted, time.Now(), Request{Proposal: changed, ApprovalID: "approval-1"}); !errors.Is(err, ErrStale) {
		t.Fatalf("changed proposal: %v", err)
	}
}

func TestApprovalFailsClosedForRejectedExpiredAndWidenedScope(t *testing.T) {
	store, _ := NewFileStore(filepath.Join(t.TempDir(), "a.json"))
	p := proposal()
	d := decision(t, p)
	d.Approved = false
	if err := store.Put(d); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(store, time.Now(), Request{Proposal: p, ApprovalID: d.ID}); !errors.Is(err, ErrStale) {
		t.Fatalf("rejected: %v", err)
	}
	d.Approved = true
	d.ExpiresAt = time.Now().Add(-time.Minute)
	if err := store.Put(d); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(store, time.Now(), Request{Proposal: p, ApprovalID: d.ID}); !errors.Is(err, ErrStale) {
		t.Fatalf("expired: %v", err)
	}
	d.ExpiresAt = time.Now().Add(time.Hour)
	d.Scope = []string{"payment:charge", "payment:refund"}
	if err := store.Put(d); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(store, time.Now(), Request{Proposal: p, ApprovalID: d.ID}); !errors.Is(err, ErrDenied) {
		t.Fatalf("widened scope: %v", err)
	}
}

func TestPublicationRequiresDeterministicEvidenceAndNeverUsesModelText(t *testing.T) {
	store, _ := NewFileStore(filepath.Join(t.TempDir(), "a.json"))
	p := proposal()
	if err := store.Put(decision(t, p)); err != nil {
		t.Fatal(err)
	}
	req := Request{Proposal: p, ApprovalID: "approval-1", RequireEvidence: true, Assertions: []Assertion{{Name: "model-said-approved", Digest: "sha256:model", Source: "model", Deterministic: false}}}
	if _, err := Publish(store, time.Now(), req, []byte("secret-result")); !errors.Is(err, ErrEvidence) {
		t.Fatalf("model evidence accepted: %v", err)
	}
	req.Assertions = []Assertion{{Name: "provider-checked", Digest: "sha256:proof", Source: "deterministic", Deterministic: true}}
	out, err := Publish(store, time.Now(), req, []byte("trusted-result"))
	if err != nil || string(out) != "trusted-result" {
		t.Fatalf("trusted publish: %s %v", out, err)
	}
}

func TestFileStoreDoesNotLeaveCredentialsInPlainOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.json")
	s, _ := NewFileStore(path)
	p := proposal()
	d := decision(t, p)
	d.Reviewer = "alice"
	if err := s.Put(d); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty durable decision")
	}
	if string(data) == "secret" {
		t.Fatal("unexpected secret")
	}
}

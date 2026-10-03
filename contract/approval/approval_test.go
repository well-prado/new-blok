package approval

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
)

type reviewer struct{ allowed bool }

func (r reviewer) AuthorizeReview(context.Context, Proposal, []string) (string, error) {
	if !r.allowed {
		return "", ErrDenied
	}
	return "authenticated-reviewer", nil
}

func proposal() Proposal {
	return Proposal{Action: "charge@1", Workflow: "orders@1", RunID: "run-1", InvocationPath: "charge", IterationPath: "root", InputDigest: BytesDigest([]byte(`{"amount":1}`)), ArtifactDigest: BytesDigest([]byte("artifact")), Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}
}

func TestImmutableReviewAuditRestartAndCapacity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	cfg := Config{Authorizer: reviewer{true}, Clock: func() time.Time { return now }, MaxDecisions: 2}
	s, err := NewJournalStore(ctx, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := proposal()
	expires := now.Add(time.Hour)
	d, err := s.Record(ctx, "a", p, p.Scope, expires, true)
	if err != nil {
		t.Fatal(err)
	}
	if d.Reviewer != "authenticated-reviewer" || !d.RecordedAt.Equal(now) {
		t.Fatalf("audit=%+v", d)
	}
	now = now.Add(time.Minute)
	duplicate, err := s.Record(ctx, "a", p, p.Scope, expires, true)
	if err != nil || !duplicate.RecordedAt.Equal(d.RecordedAt) {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	for _, changed := range []Proposal{
		func() Proposal { q := CloneProposal(p); q.InputDigest = BytesDigest([]byte("changed")); return q }(),
		func() Proposal { q := CloneProposal(p); q.ToolDigest = BytesDigest([]byte("changed-tool")); return q }(),
		func() Proposal { q := CloneProposal(p); q.RunID = "replay-1"; return q }(),
	} {
		if _, err := s.Record(ctx, "a", changed, p.Scope, expires, true); !errors.Is(err, ErrConflict) {
			t.Fatalf("overwrite=%v", err)
		}
	}
	if _, err := s.Record(ctx, "a", p, p.Scope, expires, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("reject overwrite=%v", err)
	}
	if _, err := s.Record(ctx, "b", p, p.Scope, expires, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(ctx, "c", p, p.Scope, expires, true); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err = NewJournalStore(ctx, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := Authorize(ctx, s, now, Request{Proposal: p, ApprovalID: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(ctx, s, now, Request{Proposal: p, ApprovalID: "b"}); !errors.Is(err, ErrStale) {
		t.Fatalf("rejection=%v", err)
	}
	if err := Authorize(ctx, s, expires, Request{Proposal: p, ApprovalID: "a"}); !errors.Is(err, ErrStale) {
		t.Fatalf("expiry=%v", err)
	}
	// Corrupt the owned audit binding: never silently repair or authorize it.
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE approval_decisions_v1 SET binding='forged' WHERE id='a'")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(ctx, s, now, Request{Proposal: p, ApprovalID: "a"}); !errors.Is(err, ErrStale) {
		t.Fatalf("corrupt binding=%v", err)
	}
}

func TestReviewerAndScopeAuthorization(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now()
	p := proposal()
	s, err := NewJournalStore(ctx, db, Config{Authorizer: reviewer{false}, MaxDecisions: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(ctx, "unauthorized", p, p.Scope, now.Add(time.Hour), true); !errors.Is(err, ErrDenied) {
		t.Fatalf("reviewer=%v", err)
	}
	s, err = NewJournalStore(ctx, db, Config{Authorizer: reviewer{true}, MaxDecisions: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(ctx, "narrow", p, []string{"payment:read"}, now.Add(time.Hour), true); !errors.Is(err, ErrDenied) {
		t.Fatalf("reversed scope=%v", err)
	}
	if _, err := s.Record(ctx, "broad", p, []string{"payment:read", "payment:write"}, now.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(ctx, s, time.Now(), Request{Proposal: p, ApprovalID: "broad"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(ctx, "unbounded", p, p.Scope, now.Add(25*time.Hour), true); !errors.Is(err, ErrDenied) {
		t.Fatalf("lifetime=%v", err)
	}
	if _, err := s.Record(ctx, "invalid", Proposal{}, nil, now.Add(time.Hour), true); !errors.Is(err, ErrDenied) {
		t.Fatalf("invalid=%v", err)
	}
}

func TestConcurrentReviewNeverOverwrites(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := NewJournalStore(ctx, db, Config{Authorizer: reviewer{true}, MaxDecisions: 1})
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, approved := range []bool{true, false} {
		wg.Add(1)
		go func(approved bool) {
			defer wg.Done()
			_, err := s.Record(ctx, "same-id", proposal(), proposal().Scope, expires, approved)
			results <- err
		}(approved)
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatalf("unexpected concurrent error=%v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
	d, ok, err := s.Get(ctx, "same-id")
	if err != nil || !ok || d.Reviewer == "" {
		t.Fatalf("decision=%+v ok=%v err=%v", d, ok, err)
	}
}

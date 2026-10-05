package policy

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
)

// barrierDB stops a subprocess inside or immediately after the actual SQLite
// transaction. It delegates durability to the real backend, never a fake store.
type barrierDB struct {
	store.Database
	phase  string
	marker string
	armed  bool
}

func (b *barrierDB) wait() {
	if err := os.WriteFile(b.marker, []byte("barrier"), 0600); err != nil {
		panic(err)
	}
	select {}
}

func (b *barrierDB) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	matched := false
	err := b.Database.WithTx(ctx, func(tx *sql.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if !b.armed {
			return nil
		}
		var count int
		switch b.phase {
		case "approval-before", "approval-after":
			if err := tx.QueryRow("SELECT COUNT(*) FROM approval_decisions_v1 WHERE id='review-1'").Scan(&count); err != nil {
				return err
			}
		case "publication-before", "publication-after":
			if err := tx.QueryRow("SELECT COUNT(*) FROM journal_operations WHERE state='committed'").Scan(&count); err != nil {
				return err
			}
		}
		matched = count == 1
		if matched && (b.phase == "approval-before" || b.phase == "publication-before") {
			b.wait()
		}
		return nil
	})
	if err == nil && matched && (b.phase == "approval-after" || b.phase == "publication-after") {
		b.wait()
	}
	return err
}

func TestProcessKillApprovalDispatchPublication(t *testing.T) {
	if os.Getenv("BLOK_APPROVAL_CRASH_CHILD") == "1" {
		path := os.Getenv("BLOK_APPROVAL_CRASH_PATH")
		phase := os.Getenv("BLOK_APPROVAL_CRASH_PHASE")
		r := setup(t, path)
		barrier := &barrierDB{Database: r.db, phase: phase, marker: path + ".marker"}
		a, err := approval.NewJournalStore(context.Background(), barrier, approval.Config{Audit: testAudit(t, barrier), Authorizer: r.aAuthorizer(), Clock: func() time.Time { return r.clock }, MaxDecisions: 100})
		if err != nil {
			t.Fatal(err)
		}
		r.a = a
		r.p.cfg.Approvals = a
		if phase == "approval-before" || phase == "approval-after" {
			barrier.armed = true
		}
		r.approve(t, true)
		// Publication barriers need the effect journal on the same wrapper.
		r.j = journalWithDatabase(t, barrier)
		r.p.cfg.Journal = r.j
		barrier.armed = true
		r.target.Execute = func(context.Context, []byte) ([]byte, error) {
			if phase == "dispatch-before" {
				barrier.wait()
			}
			file, err := os.OpenFile(path+".effect", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			if err != nil {
				return nil, err
			}
			if _, err = file.Write([]byte("one synthetic charge")); err != nil {
				file.Close()
				return nil, err
			}
			if err = file.Sync(); err != nil {
				file.Close()
				return nil, err
			}
			if err = file.Close(); err != nil {
				return nil, err
			}
			r.effects.Add(1)
			if phase == "dispatch-after" {
				barrier.wait()
			}
			return []byte(`{"receipt":1}`), nil
		}
		if _, err := r.p.Invoke(context.Background(), r.target, r.call); err != nil {
			t.Fatal(err)
		}
		t.Fatal("child failed to reach selected crash barrier")
	}
	for _, phase := range []string{"approval-before", "approval-after", "dispatch-before", "dispatch-after", "publication-before", "publication-after"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestProcessKillApprovalDispatchPublication$", "-test.v")
			cmd.Env = append(os.Environ(), "BLOK_APPROVAL_CRASH_CHILD=1", "BLOK_APPROVAL_CRASH_PATH="+path, "BLOK_APPROVAL_CRASH_PHASE="+phase)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					cmd.Process.Kill()
					cmd.Wait()
				}
			})
			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(path + ".marker"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child did not reach crash barrier")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			r := setup(t, path)
			if err := r.db.Integrity(context.Background()); err != nil {
				t.Fatal(err)
			}
			d, found, err := r.a.Get(context.Background(), r.call.ApprovalID)
			if err != nil {
				t.Fatal(err)
			}
			wantReview := phase != "approval-before"
			if found != wantReview {
				t.Fatalf("review=%v want=%v", found, wantReview)
			}
			if found && (d.Reviewer != "reviewer:alice" || d.RecordedAt.IsZero()) {
				t.Fatalf("lost audit=%+v", d)
			}
			attempts, published := r.trace(t)
			wantAttempts := 0
			if phase != "approval-before" && phase != "approval-after" {
				wantAttempts = 1
			}
			wantPublished := 0
			if phase == "publication-after" {
				wantPublished = 1
			}
			if attempts != wantAttempts || published != wantPublished {
				t.Fatalf("attempts=%d published=%d", attempts, published)
			}
			_, effectErr := os.Stat(path + ".effect")
			wantEffect := phase == "dispatch-after" || phase == "publication-before" || phase == "publication-after"
			if (effectErr == nil) != wantEffect {
				t.Fatalf("external effect present=%v want=%v", effectErr == nil, wantEffect)
			}
			if wantAttempts == 1 {
				out, err := r.p.Invoke(context.Background(), r.target, r.call)
				if err == nil || out != nil || r.effects.Load() != 0 {
					t.Fatalf("unsafe redispatch output=%s err=%v effects=%d", out, err, r.effects.Load())
				}
			} else if phase == "approval-before" {
				if out, err := r.p.Invoke(context.Background(), r.target, r.call); !errors.Is(err, approval.ErrStale) || out != nil {
					t.Fatalf("uncommitted review authorized: %s %v", out, err)
				}
			} else {
				if out, err := r.p.Invoke(context.Background(), r.target, r.call); err != nil || string(out) != `{"receipt":1}` {
					t.Fatalf("committed review lost: %s %v", out, err)
				}
			}
			t.Logf("kill/restart: phase=%s durable_review=%v external_effect=%v durable_attempts=%d trusted_results=%d", phase, found, wantEffect, attempts, published)
		})
	}
}

func (r *rig) aAuthorizer() reviewFunc {
	return reviewFunc(func(context.Context, approval.Proposal, []string) (string, error) { return "reviewer:alice", nil })
}

func journalWithDatabase(t *testing.T, db store.Database) *journal.Journal {
	t.Helper()
	j, err := journal.New(context.Background(), db, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

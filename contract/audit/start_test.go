package audit_test

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
)

// The audit start marker (#284) against databases the released code paths
// wrote:
//
//   - preAuditFixture: written by origin/main at 71b8351, before #280 made
//     audit mandatory: three approval decisions and two reconciliations,
//     no audit tables (testdata/restore/audit-start-284/generate.go).
//   - alphaFixture: that database upgraded and used by v0.1.0-alpha
//     (79ee0a7), which created audit schema 1 without a marker and recorded
//     one approval and one reconciliation with their records
//     (generate_alpha.go). The alpha binary's own Verify of it is
//     ErrMismatch; the generator checks that.
const (
	preAuditFixture = "../../testdata/restore/audit-start-284/legacy-main-71b8351.db.gz"
	alphaFixture    = "../../testdata/restore/audit-start-284/alpha-79ee0a7.db.gz"
)

// verifyReport is the one call into the report API, so these tests run
// against origin/main with only it replaced (the RED proofs in the PR).
func verifyReport(r *rig) (audit.Report, error) {
	r.t.Helper()
	return r.audit.VerifyReport(r.ctx, r.approval, r.journal)
}

func fixtureDatabase(t *testing.T, fixture string) string {
	t.Helper()
	compressed, err := os.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "upgraded.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(file, reader); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// upgraded opens fixture with this binary, as an application composes it,
// and requires Verify to pass with the fixture's five pre-audit decisions
// reported as legacy and records records verified.
func upgraded(t *testing.T, fixture string, records int) *rig {
	t.Helper()
	r := openRig(t, fixtureDatabase(t, fixture), rigOptions{})
	report, err := verifyReport(r)
	if err != nil {
		t.Fatalf("verify of the upgraded database: %v, want it to pass with legacy decisions", err)
	}
	if report.Records != records || report.Legacy[audit.KindApproval] != 3 || report.Legacy[audit.KindReconciliation] != 2 {
		t.Fatalf("upgraded report=%+v, want %d records, 3 legacy approvals, 2 legacy reconciliations", report, records)
	}
	return r
}

// TestUpgradedPreAuditDatabaseReportsLegacyDecisions: decisions recorded
// before audit existed are accepted as legacy, counted per kind, on a
// database written by the pre-audit code path and on one that went through
// v0.1.0-alpha first, whose own decisions are matched by their records.
// The marker is written once: reopening changes nothing.
func TestUpgradedPreAuditDatabaseReportsLegacyDecisions(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		path    string
		records int
	}{{"pre-audit", preAuditFixture, 0}, {"via v0.1.0-alpha", alphaFixture, 2}} {
		t.Run(fixture.name, func(t *testing.T) {
			r := upgraded(t, fixture.path, fixture.records)
			first, _ := verifyReport(r)
			before := r.snapshot()
			again := r.reopen()
			if after := again.snapshot(); after != before {
				t.Fatal("reopening the upgraded database changed it")
			}
			second, err := verifyReport(again)
			if err != nil || second.Markers[audit.KindApproval] != first.Markers[audit.KindApproval] || second.Markers[audit.KindReconciliation] != first.Markers[audit.KindReconciliation] {
				t.Fatalf("reopened report=%+v err=%v, want the markers of %+v", second, err, first)
			}
			if first.Markers[audit.KindApproval].Legacy != 3 || first.Markers[audit.KindReconciliation].Legacy != 2 {
				t.Fatalf("markers=%+v, want 3 legacy approvals and 2 legacy reconciliations", first.Markers)
			}
			// A legacy decision whose record is written later is matched,
			// not legacy: compaction backfills the completed run's
			// reconciliation record before erasing its actor (ADR 0021 §7).
			again.clock = again.clock.Add(48 * time.Hour)
			if report, err := again.journal.Compact(again.ctx, again.clock); err != nil || report.RemovedRuns != 1 {
				t.Fatalf("compact=%+v err=%v", report, err)
			}
			backfilled, err := verifyReport(again)
			if err != nil || backfilled.Records != fixture.records+1 || backfilled.Legacy[audit.KindReconciliation] != 1 || backfilled.Legacy[audit.KindApproval] != 3 {
				t.Fatalf("after backfill report=%+v err=%v, want one more record and one legacy reconciliation fewer", backfilled, err)
			}
		})
	}
}

// TestPostMarkerDecisionWithoutRecordStillMismatches: the marker exempts
// only the decisions it listed. A decision recorded after it, whose record
// is then removed, is ErrMismatch, beside the legacy ones.
func TestPostMarkerDecisionWithoutRecordStillMismatches(t *testing.T) {
	for _, kind := range []audit.Kind{audit.KindApproval, audit.KindReconciliation} {
		t.Run(string(kind), func(t *testing.T) {
			r := upgraded(t, preAuditFixture, 0)
			if kind == audit.KindApproval {
				if _, err := r.approve("post-marker", "run-post", true); err != nil {
					t.Fatal(err)
				}
			} else {
				_, op := r.uncertainEffect("post-marker")
				if err := r.reconcile(op, "operator:bob"); err != nil {
					t.Fatal(err)
				}
			}
			if report, err := verifyReport(r); err != nil || report.Records != 1 || report.Legacy[kind] != map[audit.Kind]int{audit.KindApproval: 3, audit.KindReconciliation: 2}[kind] {
				t.Fatalf("with the post-marker record report=%+v err=%v", report, err)
			}
			r.exec(`DELETE FROM audit_records_v1 WHERE kind = '`+string(kind)+`'`, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
			if _, err := verifyReport(r); !errors.Is(err, audit.ErrMismatch) {
				t.Fatalf("post-marker decision without its record: %v, want ErrMismatch", err)
			}
		})
	}
}

// TestMovingTheStartMarkerForwardIsDetected: hiding a post-marker decision
// whose record was removed by listing it as legacy, by rewriting the
// marker, or by removing the marker so that a later open writes a new one
// is refused. Rewriting the marker AND its digest consistently is not
// detectable inside the database; the marker digests in the report are
// what an operator keeps outside it to catch that (pinned last).
func TestMovingTheStartMarkerForwardIsDetected(t *testing.T) {
	hidden := audit.Digest([]byte("approval:post-marker"))
	setup := func(t *testing.T) (*rig, audit.Report) {
		r := upgraded(t, preAuditFixture, 0)
		anchor, err := verifyReport(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.approve("post-marker", "run-post", true); err != nil {
			t.Fatal(err)
		}
		r.exec(`DELETE FROM audit_records_v1 WHERE id = 'approval:post-marker'`, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		return r, anchor
	}
	t.Run("decision listed as legacy", func(t *testing.T) {
		r, _ := setup(t)
		r.exec(`INSERT INTO audit_legacy_v1 (kind, id_digest) VALUES ('approval.decision', '` + hidden + `')`)
		if _, err := verifyReport(r); !errors.Is(err, audit.ErrCorrupt) {
			t.Fatalf("legacy list grown after the marker: %v, want ErrCorrupt", err)
		}
	})
	t.Run("decision listed and count raised", func(t *testing.T) {
		r, _ := setup(t)
		r.exec(`INSERT INTO audit_legacy_v1 (kind, id_digest) VALUES ('approval.decision', '`+hidden+`')`,
			`UPDATE audit_start_v1 SET legacy = legacy + 1 WHERE kind = 'approval.decision'`)
		if _, err := verifyReport(r); !errors.Is(err, audit.ErrCorrupt) {
			t.Fatalf("legacy list and count rewritten: %v, want ErrCorrupt", err)
		}
	})
	t.Run("marker row removed", func(t *testing.T) {
		r, _ := setup(t)
		r.exec(`DELETE FROM audit_start_v1 WHERE kind = 'approval.decision'`)
		if _, err := verifyReport(r.reopen()); !errors.Is(err, audit.ErrCorrupt) {
			t.Fatalf("legacy list without its marker: %v, want ErrCorrupt", err)
		}
	})
	t.Run("marker and list removed, store reopened", func(t *testing.T) {
		r, _ := setup(t)
		r.exec(`DELETE FROM audit_start_v1 WHERE kind = 'approval.decision'`, `DELETE FROM audit_legacy_v1 WHERE kind = 'approval.decision'`)
		next := r.reopen()
		if n := next.count(`SELECT COUNT(*) FROM audit_start_v1 WHERE kind = 'approval.decision'`); n != 0 {
			t.Fatalf("a store already at audit schema 2 wrote a new marker (%d rows)", n)
		}
		if _, err := verifyReport(next); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("no marker: %v, want ErrMismatch (no decision is legacy)", err)
		}
	})
	t.Run("marker tables dropped, store reopened", func(t *testing.T) {
		r, _ := setup(t)
		r.exec(`DROP TABLE audit_start_v1`, `DROP TABLE audit_legacy_v1`)
		if _, err := verifyReport(r.reopen()); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("marker tables recreated empty: %v, want ErrMismatch", err)
		}
	})
	t.Run("limit: marker reopened by hand is caught only by an outside anchor", func(t *testing.T) {
		r, anchor := setup(t)
		r.exec(`DELETE FROM audit_legacy_v1 WHERE kind = 'approval.decision'`, `UPDATE audit_start_v1 SET started = 0, legacy = 0, digest = '' WHERE kind = 'approval.decision'`)
		report, err := verifyReport(r.reopen())
		if err != nil || report.Legacy[audit.KindApproval] != 4 {
			t.Fatalf("hand-reopened marker report=%+v err=%v, want it recaptured with 4 legacy approvals", report, err)
		}
		if report.Markers[audit.KindApproval] == anchor.Markers[audit.KindApproval] {
			t.Fatal("a recaptured marker must differ from the anchored one")
		}
	})
}

// TestFreshDatabaseStartsWithNoLegacy: on a new database the marker of
// every owner kind is written when the owner first opens, with nothing
// legacy, so every decision needs its record from the first one on.
func TestFreshDatabaseStartsWithNoLegacy(t *testing.T) {
	r := newRig(t, rigOptions{})
	if n := r.count(`SELECT COUNT(*) FROM audit_start_v1 WHERE started = 1 AND legacy = 0`); n != 2 {
		t.Fatalf("started markers with no legacy=%d, want 2", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_legacy_v1`); n != 0 {
		t.Fatalf("legacy decisions on a fresh database=%d", n)
	}
	if _, err := r.approve("fresh", "run-1", true); err != nil {
		t.Fatal(err)
	}
	report, err := verifyReport(r)
	if err != nil || report.Records != 1 || report.Legacy[audit.KindApproval] != 0 || report.Legacy[audit.KindReconciliation] != 0 {
		t.Fatalf("fresh report=%+v err=%v", report, err)
	}
	for _, kind := range []audit.Kind{audit.KindApproval, audit.KindReconciliation} {
		if marker := report.Markers[kind]; marker.Legacy != 0 || marker.Digest != audit.Digest([]byte(kind)) {
			t.Fatalf("%s marker=%+v, want no legacy and the digest of an empty list", kind, marker)
		}
	}
	r.exec(`DELETE FROM audit_records_v1 WHERE id = 'approval:fresh'`, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
	if _, err := verifyReport(r); !errors.Is(err, audit.ErrMismatch) {
		t.Fatalf("fresh decision without its record: %v, want ErrMismatch", err)
	}
}

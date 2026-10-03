package journal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestCompactionRetainsAuditAndActiveRuns(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := time.Unix(100, 0)
	j, err := New(context.Background(), database, Config{Clock: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "completed", Workflow: "orders", ArtifactDigest: "sha256:a", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteRun(context.Background(), completed.RunID, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	active, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "active", Workflow: "orders", ArtifactDigest: "sha256:a", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Compact(context.Background(), time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Run(context.Background(), completed.RunID); err == nil {
		t.Fatal("completed run was not compacted")
	}
	if _, err := j.Run(context.Background(), active.RunID); err != nil {
		t.Fatalf("active run was compacted: %v", err)
	}
	count, err := j.AuditCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("audit count=%d err=%v", count, err)
	}
	if _, err := j.Compact(context.Background(), time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	count, err = j.AuditCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("duplicate audit count=%d err=%v", count, err)
	}
}

func TestBackupRestoreValidatesIntegrityAndPreservesJournal(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	backupPath := filepath.Join(directory, "backup.db")
	restoredPath := filepath.Join(directory, "restored.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "backup", Workflow: "orders", ArtifactDigest: "sha256:a", Input: []byte(`{"sku":"coffee"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Backup(context.Background(), backupPath); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (sqlite.Backend{}).Restore(context.Background(), backupPath, restoredPath); err != nil {
		t.Fatal(err)
	}
	restored, err := (sqlite.Backend{}).Open(context.Background(), restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restoredJournal, err := New(context.Background(), restored, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restoredJournal.Run(context.Background(), run.RunID); err != nil {
		t.Fatalf("restored run missing: %v", err)
	}
	if err := (sqlite.Backend{}).Restore(context.Background(), backupPath, restoredPath); err == nil {
		t.Fatal("restore overwrote existing destination")
	}
}

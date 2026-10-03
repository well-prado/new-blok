package provider

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestRecordsWaitForConcurrentWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "records.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p, err := NewRecords(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	locked, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		writerDone <- db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE provider_records SET record_id=record_id WHERE 0`); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-locked:
	case err := <-writerDone:
		t.Fatalf("writer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer close(release)
	in := DatabaseInput{Key: "synthetic-operation", RecordID: "record", Value: "synthetic"}
	done := make(chan error, 1)
	go func() { _, err := p.Execute(ctx, in); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("record write returned while writer held lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(ctx, in); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	in.Value = "conflicting"
	if _, err := p.Execute(ctx, in); !IsClass(err, Business) {
		t.Fatalf("conflict: %v", err)
	}
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		var records, outbox int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_records`).Scan(&records); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_outbox`).Scan(&outbox); err != nil {
			return err
		}
		if records != 1 || outbox != 1 {
			t.Errorf("records=%d outbox=%d, want 1 each", records, outbox)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

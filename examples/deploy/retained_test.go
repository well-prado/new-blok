package deploy

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appdeploy "github.com/well-prado/new-blok/app/deploy"
	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

func durableRequest(d *appdeploy.Deployment, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer synthetic-fixture-token")
	w := httptest.NewRecorder()
	d.ServeHTTP(w, r)
	return w
}

func startDurableTest(t *testing.T, path string) (*appdeploy.Deployment, func()) {
	t.Helper()
	d, err := NewDurable(deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 2, DrainTimeout: time.Second}, path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, nil) }()
	for deadline := time.Now().Add(5 * time.Second); ; {
		if durableRequest(d, "GET", "/readyz", "").Code == 200 {
			break
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("readiness deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return d, func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func openDurableTest(t *testing.T, path string) store.Database {
	t.Helper()
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func executeDurableTest(t *testing.T, db store.Database, statement string) {
	t.Helper()
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error { _, err := tx.Exec(statement); return err }); err != nil {
		t.Fatal(err)
	}
}

func retainedCounts(t *testing.T, db store.Database) [4]int {
	t.Helper()
	var counts [4]int
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT (SELECT COUNT(*) FROM journal_runs), (SELECT COUNT(*) FROM journal_checkpoints), (SELECT COUNT(*) FROM worker_jobs), (SELECT COUNT(*) FROM orders)`).Scan(&counts[0], &counts[1], &counts[2], &counts[3])
	}); err != nil {
		t.Fatal(err)
	}
	return counts
}

func TestActualRetainedJournalColdRestartAndRestore(t *testing.T) {
	t.Setenv("BLOK_DEPLOY_TOKEN", "synthetic-fixture-token")
	path := filepath.Join(t.TempDir(), "orders.db")
	d, stop := startDurableTest(t, path)
	if w := durableRequest(d, "POST", "/orders", `{"requestKey":"accepted-81","sku":"coffee","quantity":2}`); w.Code != 202 {
		t.Fatalf("admit: %d %s", w.Code, w.Body)
	}
	stop()
	db := openDurableTest(t, path)
	if got := retainedCounts(t, db); got != [4]int{1, 1, 1, 0} {
		t.Fatalf("committed handoff: %v", got)
	}
	j, err := journal.New(context.Background(), db, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	manifest, codec, err := durableArtifact()
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := manifest.Digest()
	var retainedID string
	if err := j.RetainedArtifacts(context.Background(), func(item journal.RetainedArtifact) error {
		if item.ArtifactDigest != identity || item.CheckpointDigest != codec {
			t.Fatalf("retained identity: %+v", item)
		}
		retainedID = item.RunID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Recover(context.Background(), retainedID, identity, codec); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := j.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored.db")
	if err := (sqlite.Backend{}).Restore(context.Background(), backup, restored); err != nil {
		t.Fatal(err)
	}
	for _, volume := range []string{path, restored} {
		d, stop = startDurableTest(t, volume)
		if w := durableRequest(d, "POST", "/process", ""); w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"processed":true}` {
			t.Fatalf("cold processing: %d %s", w.Code, w.Body)
		}
		if w := durableRequest(d, "GET", "/orders/accepted-81", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"TotalCents":3000`) {
			t.Fatalf("order: %d %s", w.Code, w.Body)
		}
		stop()
		d, stop = startDurableTest(t, volume)
		if w := durableRequest(d, "POST", "/process", ""); w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"processed":false}` {
			t.Fatalf("repeat effect: %d %s", w.Code, w.Body)
		}
		stop()
	}
}

func TestActualRetainedJournalFaultsRejectAdmissionAndColdStart(t *testing.T) {
	for name, mutation := range map[string]string{
		"missing-artifact":                             `DELETE FROM journal_artifacts`,
		"invalid-manifest":                             `UPDATE journal_artifacts SET manifest_json = '{"name":"invalid"}'`,
		"changed-manifest":                             `UPDATE journal_artifacts SET manifest_json = json_set(manifest_json, '$.checkpointFormat', 'unsupported-v2')`,
		"incompatible-codec":                           `UPDATE journal_checkpoints SET checkpoint_digest = 'unsupported-v2'`,
		"wrong-checkpoint-artifact":                    `UPDATE journal_checkpoints SET artifact_digest = 'sha256:unavailable'`,
		"empty-checkpoint-identity":                    `UPDATE journal_checkpoints SET artifact_digest = '', checkpoint_digest = ''`,
		"retained-other-executable":                    "",
		"admission-before-checkpoint-missing-artifact": `DELETE FROM journal_checkpoints; DELETE FROM journal_artifacts`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("BLOK_DEPLOY_TOKEN", "synthetic-fixture-token")
			path := filepath.Join(t.TempDir(), "orders.db")
			d, stop := startDurableTest(t, path)
			if w := durableRequest(d, "POST", "/orders", `{"requestKey":"accepted-81","sku":"coffee","quantity":2}`); w.Code != 202 {
				t.Fatalf("admission: %d", w.Code)
			}
			db := openDurableTest(t, path)
			if name == "retained-other-executable" {
				manifest, _, err := durableArtifact()
				if err != nil {
					t.Fatal(err)
				}
				manifest.NativeBinaryDigest = deploymentDigest([]byte("unavailable retained executable"))
				identity, _ := manifest.Digest()
				canonical, _ := manifest.Canonical()
				j, err := journal.New(context.Background(), db, journal.Config{})
				if err != nil {
					t.Fatal(err)
				}
				if err := j.RegisterArtifact(context.Background(), journal.ArtifactRecord{Digest: identity, Version: manifest.Version, ManifestJSON: canonical}); err != nil {
					t.Fatal(err)
				}
				if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
					if _, err := tx.Exec("UPDATE journal_runs SET artifact_digest = ?", identity); err != nil {
						return err
					}
					_, err := tx.Exec("UPDATE journal_checkpoints SET artifact_digest = ?", identity)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, statement := range strings.Split(mutation, ";") {
					executeDurableTest(t, db, statement)
				}
			}
			before := retainedCounts(t, db)
			if w := durableRequest(d, "GET", "/healthz", ""); w.Code != 200 {
				t.Fatalf("health: %d", w.Code)
			}
			if w := durableRequest(d, "GET", "/readyz", ""); w.Code != 503 {
				t.Fatalf("readiness: %d", w.Code)
			}
			if w := durableRequest(d, "GET", "/metrics", ""); !strings.Contains(w.Body.String(), "blok_ready 0") {
				t.Fatalf("metrics: %s", w.Body)
			}
			if w := durableRequest(d, "POST", "/orders", `{"requestKey":"denied-81","sku":"coffee","quantity":2}`); w.Code != 503 {
				t.Fatalf("fault admission: %d", w.Code)
			}
			if got := retainedCounts(t, db); got != before {
				t.Fatalf("fault admitted work: %v -> %v", before, got)
			}
			stop()
			d, err := NewDurable(deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 2, DrainTimeout: time.Second}, path)
			if err != nil {
				t.Fatal(err)
			}
			err = d.Run(context.Background(), nil)
			if err == nil || !strings.Contains(err.Error(), "retained journal incompatible") {
				t.Fatalf("cold fault: %v", err)
			}
			// The refusal is structured, so blok dev can tell it from a crash
			// without reading the message (ADR 0026).
			if !errors.Is(err, deployment.ErrRetainedIncompatible) || deployment.ExitCode(err) != deployment.ExitRetainedIncompatible {
				t.Fatalf("cold fault is not deployment.ErrRetainedIncompatible (exit %d): %v", deployment.ExitCode(err), err)
			}
			if got := retainedCounts(t, db); got != before {
				t.Fatalf("cold startup changed work: %v -> %v", before, got)
			}
		})
	}
}

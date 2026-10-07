// Package deploy contains application-owned deployment composition examples.
package deploy

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/well-prado/new-blok/app"
	appdeploy "github.com/well-prado/new-blok/app/deploy"
	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/examples/order"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// NewDurable demonstrates committed order admission and business persistence,
// with readiness bound to the selected executable and real retained journal.
func NewDurable(c deployment.Config, path string) (*appdeploy.Deployment, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("deployment: absolute durable volume path required")
	}
	c.StoreRequired = true
	c.RequiredSecrets = []string{"BLOK_DEPLOY_TOKEN"}
	var database store.Database
	var service *order.Service
	var retained *journal.Journal
	inputSchema, err := schema.Parse([]byte(durableInputSchema))
	if err != nil {
		return nil, err
	}
	manifest, codec, err := durableArtifact()
	if err != nil {
		return nil, err
	}
	probe, err := app.RetainedArtifactProbe(manifest, codec, func(ctx context.Context, visit func(app.RetainedArtifact) error) error {
		if retained == nil {
			return errors.New("deployment: journal unavailable")
		}
		return retained.RetainedArtifacts(ctx, func(item journal.RetainedArtifact) error {
			return visit(app.RetainedArtifact{ArtifactDigest: item.ArtifactDigest, ManifestJSON: item.ManifestJSON, CheckpointArtifact: item.CheckpointArtifact, CheckpointDigest: item.CheckpointDigest, CheckpointPresent: item.CheckpointPresent})
		})
	})
	if err != nil {
		return nil, err
	}
	const format = "deploy-order-v1"
	storeCheck := func(ctx context.Context) error {
		if database == nil {
			return errors.New("missing store")
		}
		if err := database.Integrity(ctx); err != nil {
			return err
		}
		return database.WithTx(ctx, func(tx *sql.Tx) error {
			var version string
			if err := tx.QueryRowContext(ctx, "SELECT format FROM deployment_format WHERE id = 1").Scan(&version); err != nil {
				return err
			}
			if version != format {
				return errors.New("incompatible store format")
			}
			return nil
		})
	}
	a, err := app.New(app.Config{DrainTimeout: c.DrainTimeout, Dependencies: []app.Dependency{{
		Name: "sqlite-orders",
		Start: func(ctx context.Context) error {
			var err error
			database, err = (sqlite.Backend{}).Open(ctx, path)
			if err != nil {
				return errors.New("deployment: store unavailable")
			}
			// A failing dependency owns cleanup of its partially opened resources.
			ok := false
			defer func() {
				if !ok {
					_ = database.Close()
				}
			}()
			err = database.WithTx(ctx, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS deployment_format (id INTEGER PRIMARY KEY CHECK(id=1), format TEXT NOT NULL)"); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, "INSERT INTO deployment_format VALUES (1, ?) ON CONFLICT(id) DO NOTHING", format)
				return err
			})
			if err != nil {
				return errors.New("deployment: store schema unavailable")
			}
			if err := storeCheck(ctx); err != nil {
				return errors.New("deployment: incompatible store")
			}
			retained, err = journal.New(ctx, database, journal.Config{})
			if err != nil {
				return errors.New("deployment: journal unavailable")
			}
			// Check before registering: never repair a missing retained artifact.
			// A refusal decided by the journal's content
			// (app.RetainedIncompatibleError) is the structured
			// deployment.ErrRetainedIncompatible (exit status
			// deployment.ExitRetainedIncompatible); failing to read it is not.
			if err := probe(ctx); err != nil {
				if refusal := (*app.RetainedIncompatibleError)(nil); errors.As(err, &refusal) {
					return deployment.ErrRetainedIncompatible
				}
				return errors.New("deployment: retained journal unreadable")
			}
			identity, _ := manifest.Digest()
			canonical, _ := manifest.Canonical()
			if err := retained.RegisterArtifact(ctx, journal.ArtifactRecord{Digest: identity, Version: manifest.Version, ManifestJSON: canonical}); err != nil {
				return err
			}
			service, err = order.New(ctx, database, map[string]int64{"coffee": 1500}, time.Now)
			if err != nil {
				return errors.New("deployment: order schema unavailable")
			}
			ok = true
			return nil
		},
		Close: func(context.Context) error { return database.Close() },
	}}})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		token := os.Getenv("BLOK_DEPLOY_TOKEN")
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		var input order.Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&input); err != nil || input.RequestKey == "" || len(input.RequestKey) > 128 || input.SKU != "coffee" {
			http.Error(w, "invalid order", 400)
			return
		}
		var trailing any
		if dec.Decode(&trailing) != io.EOF {
			http.Error(w, "invalid order", 400)
			return
		}
		// Bind admission to the executable before acknowledging the queue handoff.
		// Partial commits before 202 are retried with the same request key; queue
		// deduplication still owns business processing and outbox atomicity.
		payload, err := json.Marshal(input)
		if err != nil {
			http.Error(w, "admission failed", 503)
			return
		}
		if err := inputSchema.ValidateValue(payload); err != nil {
			http.Error(w, "invalid order", 400)
			return
		}
		identity, _ := manifest.Digest()
		run, err := retained.Admit(r.Context(), journal.AdmissionRequest{RequestKey: input.RequestKey, Workflow: manifest.Name, ArtifactDigest: identity, Input: payload})
		if err != nil {
			http.Error(w, "admission failed", 503)
			return
		}
		result, err := service.Enqueue(r.Context(), input)
		if err != nil {
			http.Error(w, "admission failed", 503)
			return
		}
		state, _ := json.Marshal(struct {
			Format  string        `json:"format"`
			Request order.Request `json:"request"`
			Queued  bool          `json:"queued"`
		}{durableCheckpointFormat, input, true})
		if err := retained.SaveCheckpoint(r.Context(), journal.Checkpoint{RunID: run.RunID, ArtifactDigest: identity, CheckpointDigest: codec, State: state}); err != nil {
			http.Error(w, "admission failed", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("POST /process", func(w http.ResponseWriter, r *http.Request) {
		token := os.Getenv("BLOK_DEPLOY_TOKEN")
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		processed, err := service.ProcessOnce(r.Context())
		if err != nil {
			http.Error(w, "processing failed", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"processed": processed})
	})
	mux.HandleFunc("GET /orders/{key}", func(w http.ResponseWriter, r *http.Request) {
		token := os.Getenv("BLOK_DEPLOY_TOKEN")
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		value, err := service.Get(r.Context(), r.PathValue("key"))
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", 404)
			return
		}
		if err != nil {
			http.Error(w, "store unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	})
	return appdeploy.NewDeployment(a, c, appdeploy.DeploymentChecks{Artifact: func(ctx context.Context) error {
		if err := probe(ctx); err != nil {
			return err
		}
		identity, _ := manifest.Digest()
		return retained.RequireArtifact(ctx, identity)
	}, Store: storeCheck}, mux)
}

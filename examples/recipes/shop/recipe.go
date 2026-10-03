// Package shop is a runnable application recipe composed from Blok's real
// HTTP, worker, webhook and SSE adapters. SQLite is selected by the example,
// not required by the engine or the application-facing store interface.
package shop

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

const (
	RecordSchema = `{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"},"value":{"type":"string"}},"required":["id","value"]}`
	JobSchema    = `{"type":"object","additionalProperties":false,"properties":{"requestKey":{"type":"string"},"recordId":{"type":"string"},"value":{"type":"string"}},"required":["requestKey","recordId","value"]}`
	UpdateSchema = `{"type":"object","additionalProperties":false,"properties":{"requestKey":{"type":"string"},"value":{"type":"string"}},"required":["requestKey","value"]}`
	JobKind      = "shop.record"
)

type Config struct {
	Database   store.Database
	Tokens     map[string]string
	WebhookKey []byte
}

type Record struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	Owner string `json:"owner"`
}

type jobInput struct {
	RequestKey string `json:"requestKey"`
	RecordID   string `json:"recordId"`
	Value      string `json:"value"`
}

// hiddenRecord keeps missing and another-owner rows indistinguishable. The
// current HTTP adapter maps classified validation failures to 400; when it
// adds a not-found class this recipe can switch to that status without
// changing the authorization predicate or public error code.
type hiddenRecord struct{}

func (hiddenRecord) Error() string      { return "record not found" }
func (hiddenRecord) ErrorCode() string  { return "not_found" }
func (hiddenRecord) ErrorClass() string { return "validation" }

type Application struct {
	Database       store.Database
	Queue          *worker.Queue
	Hub            *sse.Hub
	HTTP           *blokhttp.Server
	Webhook        *webhook.Server
	SSE            *sse.Server
	Handler        http.Handler
	app            *app.Application
	engine         *engine.Engine
	program        contract.InternalProgram
	streamClosures chan string
}

// Migrate applies the recipe's ordered, application-owned schema migrations.
// Calling it repeatedly is safe; the worker adapter owns its own schema.
func Migrate(ctx context.Context, database store.Database) error {
	if database == nil {
		return errors.New("shop: database is required")
	}
	return database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS shop_schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
			return err
		}
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM shop_schema_migrations`).Scan(&version); err != nil {
			return err
		}
		for next := version + 1; next <= len(migrations); next++ {
			for _, statement := range migrations[next-1] {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("shop: migration %d: %w", next, err)
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO shop_schema_migrations(version, applied_at) VALUES(?, ?)`, next, time.Now().UTC().UnixNano()); err != nil {
				return err
			}
		}
		return nil
	})
}

var migrations = [][]string{
	{`CREATE TABLE shop_records (record_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, value TEXT NOT NULL, created_at INTEGER NOT NULL)`},
	{`ALTER TABLE shop_records ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`, `CREATE TABLE shop_outbox (event_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, payload_json BLOB NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL)`},
}

// Teardown removes only this recipe's tables and the worker queue table it
// selected. Close all application processes before calling it.
func Teardown(ctx context.Context, database store.Database) error {
	if database == nil {
		return errors.New("shop: database is required")
	}
	return database.WithTx(ctx, func(tx *sql.Tx) error {
		for _, table := range []string{"shop_outbox", "shop_records", "shop_schema_migrations", "worker_jobs"} {
			if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
				return err
			}
		}
		return nil
	})
}

func New(ctx context.Context, config Config) (*Application, error) {
	if config.Database == nil {
		return nil, errors.New("shop: database is required")
	}
	if err := Migrate(ctx, config.Database); err != nil {
		return nil, err
	}
	for _, name := range []string{"alice", "bob"} {
		if len(config.Tokens[name]) < 16 {
			return nil, fmt.Errorf("shop: token for %s must contain at least 16 bytes", name)
		}
	}
	if config.Tokens["alice"] == config.Tokens["bob"] {
		return nil, errors.New("shop: alice and bob tokens must be distinct")
	}
	secret, err := webhook.NewSecret(config.WebhookKey)
	if err != nil {
		return nil, err
	}
	queue, err := worker.New(ctx, config.Database, nil)
	if err != nil {
		return nil, err
	}
	if err := queue.RegisterKind(JobKind, []byte(JobSchema)); err != nil {
		return nil, err
	}
	service := &Application{Database: config.Database, Queue: queue, streamClosures: make(chan string, 64)}
	recordNode, err := node.Define("recipes/validate-record", "1.0.0", func(_ context.Context, record Record) (Record, error) {
		if record.ID == "" || len(record.ID) > 128 || len(record.Value) > 4096 || record.Owner == "" {
			return Record{}, &node.DomainError{Code: "invalid_record", Class: "validation"}
		}
		return record, nil
	}, node.Description("Validates the recipe record value"), node.Schemas(
		[]byte(`{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"},"value":{"type":"string"},"owner":{"type":"string"}},"required":["id","value","owner"]}`),
		[]byte(`{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"},"value":{"type":"string"},"owner":{"type":"string"}},"required":["id","value","owner"]}`),
	), node.Pure())
	if err != nil {
		return nil, err
	}
	recordFlow, err := flow.Define(flow.Spec{Name: "shop/validate-record", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[Record]) flow.Ref[Record] {
		return flow.Call(builder, "validate-record", recordNode, input)
	})
	if err != nil {
		return nil, err
	}
	service.program, err = recordFlow.Lower()
	if err != nil {
		return nil, err
	}
	service.engine = engine.New(map[string]node.Any{recordNode.Descriptor().Name: recordNode.Any()})
	service.Hub, err = sse.NewHub(sse.HubConfig{
		MaxStreams: 128, MaxStreamsPerOwner: 16, RetainEvents: 32,
		RetainBytes: 64 << 10, MaxEventBytes: 8 << 10, Retention: 5 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	authenticate := func(request *http.Request) (trigger.Principal, error) {
		value, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		if !ok || value == "" {
			return trigger.Principal{}, errors.New("missing bearer token")
		}
		for _, name := range []string{"alice", "bob"} {
			providedDigest := sha256.Sum256([]byte(value))
			configuredDigest := sha256.Sum256([]byte(config.Tokens[name]))
			if subtle.ConstantTimeCompare(providedDigest[:], configuredDigest[:]) == 1 {
				return trigger.Principal{ID: name}, nil
			}
		}
		return trigger.Principal{}, errors.New("invalid bearer token")
	}
	workflowNames := []app.Workflow{{Name: "shop/validate-record"}, {Name: "shop.records.create"}, {Name: "shop.records.get"}, {Name: "shop.records.delete"}, {Name: "shop.jobs.submit"}}
	workflowNames = append(workflowNames, app.Workflow{Name: "shop.records.update"})
	routes := []app.Route{{Method: "POST", Path: "/records", Workflow: "shop.records.create"}, {Method: "GET", Path: "/records/:id", Workflow: "shop.records.get"}, {Method: "PUT", Path: "/records/:id", Workflow: "shop.records.update"}, {Method: "DELETE", Path: "/records/:id", Workflow: "shop.records.delete"}, {Method: "POST", Path: "/jobs", Workflow: "shop.jobs.submit"}}
	service.app, err = app.New(app.Config{Workflows: workflowNames, Routes: routes, DrainTimeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	service.HTTP, err = blokhttp.New(service.app, []blokhttp.Endpoint{
		{Method: http.MethodPost, Path: "/records", InputSchema: []byte(RecordSchema), Authenticate: authenticate, Handle: service.createRecord},
		{Method: http.MethodGet, Path: "/records/:id", Authenticate: authenticate, Handle: service.getRecord},
		{Method: http.MethodPut, Path: "/records/:id", InputSchema: []byte(UpdateSchema), Authenticate: authenticate, Handle: service.updateRecord},
		{Method: http.MethodDelete, Path: "/records/:id", Authenticate: authenticate, Handle: service.deleteRecord},
		{Method: http.MethodPost, Path: "/jobs", InputSchema: []byte(JobSchema), Authenticate: authenticate, Handle: service.submitJob},
	})
	if err != nil {
		return nil, err
	}
	service.Webhook, err = webhook.New(service.app, nil, []webhook.Endpoint{{
		Path: "/webhooks/orders", Provider: "synthetic", Principal: trigger.Principal{ID: "provider:synthetic"},
		Kind: JobKind, Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "current", Secret: secret}}},
		Submit: queue, InputSchema: []byte(JobSchema), MaxBodyBytes: 8 << 10, Tolerance: 5 * time.Minute,
	}})
	if err != nil {
		return nil, err
	}
	service.SSE, err = sse.New(service.app, service.Hub, []sse.Endpoint{{
		Name: "jobs", Path: "/jobs/stream", Kind: JobKind, Submit: queue, Tracker: queue,
		Authenticate: authenticate, InputSchema: []byte(JobSchema), MaxBodyBytes: 8 << 10,
		QueueDepth: 1, MaxSubscribers: 32, StreamSubscribers: 4,
		Heartbeat: 10 * time.Second, WriteTimeout: 2 * time.Second, MaxDuration: 2 * time.Minute,
		OnClose: func(_, reason string) {
			select {
			case service.streamClosures <- reason:
			default:
			}
		},
	}})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/records", service.HTTP)
	mux.Handle("/records/", service.HTTP)
	mux.Handle("/jobs/stream", service.SSE)
	mux.Handle("/jobs/stream/", service.SSE)
	mux.Handle("/jobs", service.HTTP)
	mux.Handle("/webhooks/orders", service.Webhook)
	service.Handler = mux
	return service, nil
}

func (a *Application) Start(ctx context.Context) error { return a.app.Start(ctx) }

func (a *Application) Shutdown(ctx context.Context) error {
	if a.SSE != nil {
		if err := a.SSE.Shutdown(ctx); err != nil {
			return err
		}
	}
	return a.app.Shutdown(ctx)
}

func (a *Application) createRecord(ctx context.Context, input blokhttp.Input) (any, error) {
	var request struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(input.Body, &request); err != nil {
		return nil, &node.DomainError{Code: "invalid_record", Class: "validation", Err: err}
	}
	record, err := a.runRecord(ctx, Record{ID: request.ID, Value: request.Value, Owner: input.Principal.ID})
	if err != nil {
		return nil, err
	}
	err = a.Database.WithTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC().UnixNano()
		if _, err := tx.ExecContext(ctx, `INSERT INTO shop_records(record_id, owner_id, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)`, record.ID, record.Owner, record.Value, now, now); err != nil {
			return err
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO shop_outbox(event_id, record_id, payload_json, state, created_at) VALUES(?, ?, ?, 'pending', ?)`, "record.created:"+record.ID, record.ID, payload, now)
		return err
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (a *Application) getRecord(ctx context.Context, input blokhttp.Input) (any, error) {
	var record Record
	err := a.Database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT record_id, value, owner_id FROM shop_records WHERE record_id = ? AND owner_id = ?`, input.Params["id"], input.Principal.ID).Scan(&record.ID, &record.Value, &record.Owner)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, hiddenRecord{}
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (a *Application) updateRecord(ctx context.Context, input blokhttp.Input) (any, error) {
	var request struct {
		RequestKey string `json:"requestKey"`
		Value      string `json:"value"`
	}
	if err := json.Unmarshal(input.Body, &request); err != nil {
		return nil, &node.DomainError{Code: "invalid_update", Class: "validation", Err: err}
	}
	if request.RequestKey == "" || len(request.RequestKey) > 256 || len(request.Value) > 4096 {
		return nil, &node.DomainError{Code: "invalid_update", Class: "validation"}
	}
	record := Record{ID: input.Params["id"], Value: request.Value, Owner: input.Principal.ID}
	validated, err := a.runRecord(ctx, record)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(validated)
	if err != nil {
		return nil, err
	}
	err = a.Database.WithTx(ctx, func(tx *sql.Tx) error {
		eventID := "record.updated:" + request.RequestKey
		insert, err := tx.ExecContext(ctx, `INSERT INTO shop_outbox(event_id, record_id, payload_json, state, created_at) VALUES(?, ?, ?, 'pending', ?) ON CONFLICT(event_id) DO NOTHING`, eventID, validated.ID, payload, time.Now().UTC().UnixNano())
		if err != nil {
			return err
		}
		inserted, err := insert.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			var previousID string
			var previousPayload []byte
			if err := tx.QueryRowContext(ctx, `SELECT record_id, payload_json FROM shop_outbox WHERE event_id = ?`, eventID).Scan(&previousID, &previousPayload); err != nil {
				return err
			}
			if previousID != validated.ID || string(previousPayload) != string(payload) {
				return errors.New("shop: update idempotency key conflicts with a prior change")
			}
			return nil
		}
		updated, err := tx.ExecContext(ctx, `UPDATE shop_records SET value = ?, updated_at = ? WHERE record_id = ? AND owner_id = ?`, validated.Value, time.Now().UTC().UnixNano(), validated.ID, validated.Owner)
		if err != nil {
			return err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return hiddenRecord{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return validated, nil
}

func (a *Application) deleteRecord(ctx context.Context, input blokhttp.Input) (any, error) {
	deleted := false
	err := a.Database.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM shop_records WHERE record_id = ? AND owner_id = ?`, input.Params["id"], input.Principal.ID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		deleted = count == 1
		return err
	})
	if err != nil {
		return nil, err
	}
	if !deleted {
		return nil, hiddenRecord{}
	}
	return map[string]bool{"deleted": true}, nil
}

func (a *Application) submitJob(ctx context.Context, input blokhttp.Input) (any, error) {
	var job jobInput
	if err := json.Unmarshal(input.Body, &job); err != nil {
		return nil, &node.DomainError{Code: "invalid_job", Class: "validation", Err: err}
	}
	if job.RequestKey == "" || len(job.RequestKey) > 256 || job.RecordID == "" || len(job.RecordID) > 128 || len(job.Value) > 4096 {
		return nil, &node.DomainError{Code: "invalid_job", Class: "validation"}
	}
	return a.Queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: job.RequestKey, Kind: JobKind, Payload: input.Body, Principal: input.Principal})
}

// ProcessOne runs one accepted job in the real worker queue. The business row,
// outbox event and queue acknowledgment share the worker's SQLite transaction.
func (a *Application) ProcessOne(ctx context.Context) (bool, error) {
	var key string
	var record Record
	var handlerErr error
	processed, err := a.Queue.ProcessOnce(ctx, func(ctx context.Context, tx *sql.Tx, job worker.Job) error {
		key = job.RequestKey
		var request jobInput
		if err := json.Unmarshal(job.Payload, &request); err != nil {
			handlerErr = &worker.HandlerError{Message: "invalid job payload"}
			return handlerErr
		}
		record = Record{ID: request.RecordID, Value: request.Value, Owner: job.Principal.ID}
		validated, runErr := a.runRecord(ctx, record)
		if runErr != nil {
			handlerErr = runErr
			return runErr
		}
		record = validated
		now := time.Now().UTC().UnixNano()
		inserted, err := tx.ExecContext(ctx, `INSERT INTO shop_records(record_id, owner_id, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(record_id) DO NOTHING`, record.ID, record.Owner, record.Value, now, now)
		if err != nil {
			return err
		}
		count, err := inserted.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			var owner, value string
			if err := tx.QueryRowContext(ctx, `SELECT owner_id, value FROM shop_records WHERE record_id = ?`, record.ID).Scan(&owner, &value); err != nil {
				return err
			}
			if owner != record.Owner || value != record.Value {
				return &worker.HandlerError{Message: "record key conflicts with existing data"}
			}
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO shop_outbox(event_id, record_id, payload_json, state, created_at) VALUES(?, ?, ?, 'pending', ?) ON CONFLICT(event_id) DO NOTHING`, "job.completed:"+job.RequestKey, record.ID, payload, now)
		return err
	})
	if err != nil || !processed {
		return processed, err
	}
	job, err := a.Queue.Get(ctx, key)
	if err != nil {
		return processed, err
	}
	streamID := sse.StreamID(key)
	switch job.State {
	case worker.StateCompleted:
		data, _ := json.Marshal(record)
		_, _ = a.Hub.Finish(streamID, sse.Result(data))
	case worker.StateDead:
		if handlerErr == nil {
			handlerErr = errors.New("job failed")
		}
		_, _ = a.Hub.Finish(streamID, sse.Failure(handlerErr))
	}
	return true, nil
}

func (a *Application) runRecord(ctx context.Context, record Record) (Record, error) {
	result, err := a.engine.Run(ctx, a.program, record)
	if err != nil {
		return Record{}, err
	}
	validated, ok := result.Output.(Record)
	if !ok {
		return Record{}, errors.New("shop: workflow returned an unexpected output type")
	}
	return validated, nil
}

// DrainOutbox delivers pending outbox entries with stable event identities.
// A sink must deduplicate Event.ID across an ambiguous publish/retry window.
func (a *Application) DrainOutbox(ctx context.Context, publish func(context.Context, string, []byte) error) (bool, error) {
	if publish == nil {
		return false, errors.New("shop: outbox publisher is required")
	}
	sent := false
	err := a.Database.WithTx(ctx, func(tx *sql.Tx) error {
		var eventID string
		var payload []byte
		if err := tx.QueryRowContext(ctx, `SELECT event_id, payload_json FROM shop_outbox WHERE state = 'pending' ORDER BY created_at, event_id LIMIT 1`).Scan(&eventID, &payload); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		sent = true
		if err := publish(ctx, eventID, append([]byte(nil), payload...)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE shop_outbox SET state = 'sent' WHERE event_id = ? AND state = 'pending'`, eventID)
		return err
	})
	return sent, err
}

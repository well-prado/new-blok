package webhook_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

const conformanceKind = "conformance.order"

// webhookDriver drives signed HTTP requests into the real adapter, which
// submits to a real SQLite worker queue; the queue's handler invokes the
// harness workflow and commits an effect row in the acknowledgment
// transaction. The driver never calls the workflow itself.
type webhookDriver struct {
	path     string
	env      conformance.TriggerEnv
	valid    webhook.Secret
	forged   webhook.Secret
	clock    *clock
	database store.Database
	queue    *worker.Queue
	app      *app.Application
	handler  *webhook.Server
	server   *http.Server
	client   *http.Client
	mu       sync.Mutex
	address  string
}

func newWebhookDriver(t *testing.T) *webhookDriver {
	t.Helper()
	d := &webhookDriver{path: filepath.Join(t.TempDir(), "jobs.db"), valid: secret(t, "ok"), forged: secret(t, "no"), clock: &clock{now: t0}}
	// The store is an application dependency opened before the adapter.
	database, err := (sqlite.Backend{}).Open(context.Background(), d.path)
	if err != nil {
		t.Fatal(err)
	}
	d.database = database
	return d
}

func (*webhookDriver) Declaration() trigger.Declaration { return webhook.Declaration }

func (d *webhookDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	return d.construct(ctx)
}

func (d *webhookDriver) construct(ctx context.Context) error {
	queue, err := worker.New(ctx, d.database, d.clock.Now)
	if err != nil {
		return err
	}
	if err := queue.RegisterKind(conformanceKind, d.env.InputSchema); err != nil {
		return err
	}
	if err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS conformance_effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		return err
	}
	principal, err := d.env.Authenticate(conformance.ValidCredential)
	if err != nil {
		return err
	}
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	handler, err := webhook.New(application, d.clock.Now, []webhook.Endpoint{{
		Path: "/hooks", Provider: "conformance", Principal: principal, Kind: conformanceKind, Submit: queue, InputSchema: d.env.InputSchema,
		Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: d.valid}}},
	}})
	if err != nil {
		return err
	}
	d.queue, d.app, d.handler = queue, application, handler
	d.client = &http.Client{Transport: &http.Transport{}}
	return nil
}

func (d *webhookDriver) Start(ctx context.Context) error {
	if err := d.app.Start(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	d.server = &http.Server{Handler: d.handler, ReadHeaderTimeout: 5 * time.Second}
	d.mu.Lock()
	d.address = listener.Addr().String()
	d.mu.Unlock()
	go func() { _ = d.server.Serve(listener) }()
	return nil
}

func (d *webhookDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

func (d *webhookDriver) handle(ctx context.Context, tx *sql.Tx, job worker.Job) error {
	if _, err := d.env.Workflow(ctx, conformance.Call{Input: job.Payload, Principal: job.Principal}); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
	return err
}

func (d *webhookDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	address := d.Endpoint()
	if address == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	now := d.clock.Now()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/hooks", bytes.NewReader(delivery.Payload))
	if err != nil {
		return conformance.Outcome{}, err
	}
	signing := map[string]*webhook.Secret{conformance.ValidCredential: &d.valid, "forged": &d.forged}[delivery.Credential]
	if signing != nil {
		request.Header.Set("webhook-id", delivery.Key)
		request.Header.Set("webhook-timestamp", strconv.FormatInt(now.Unix(), 10))
		request.Header.Set("webhook-signature", webhook.SignStandard(*signing, delivery.Key, now, delivery.Payload))
	}
	response, err := d.client.Do(request)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
		}
		return conformance.Outcome{}, err
	}
	var body map[string]string
	_ = json.NewDecoder(response.Body).Decode(&body)
	response.Body.Close()
	switch response.StatusCode {
	case http.StatusAccepted:
	case http.StatusOK:
		return conformance.Outcome{Kind: conformance.Duplicate}, nil
	case http.StatusUnauthorized, http.StatusBadRequest, http.StatusConflict, http.StatusServiceUnavailable:
		code := body["error"]
		if code == "conflict" || code == "unauthorized" || code == "invalid_input" || code == "saturated" || code == "unavailable" {
			return conformance.Outcome{Kind: conformance.Rejected, Code: code}, nil
		}
		return conformance.Outcome{}, errors.New("unexpected webhook error " + code)
	default:
		return conformance.Outcome{}, errors.New("unexpected webhook status " + response.Status)
	}
	// The provider has been acknowledged; the queue now runs the workflow.
	// The harness's disconnect is a lost consumer: the queue redelivers.
	consumer, cancel := context.WithCancel(ctx)
	defer cancel()
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				cancel()
			case <-consumer.Done():
			}
		}()
	}
	if _, err := d.queue.ProcessOnce(consumer, d.handle); err != nil {
		if errors.Is(err, worker.ErrConsumerLost) && ctx.Err() == nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		return conformance.Outcome{}, err
	}
	return d.outcome(ctx, webhook.SubmissionKey("conformance", delivery.Key))
}

// Recover lets backoff expire and runs what the queue redelivers.
func (d *webhookDriver) Recover(ctx context.Context) (conformance.Outcome, error) {
	d.clock.Set(d.clock.Now().Add(2 * time.Minute))
	var key string
	if _, err := d.queue.ProcessOnce(ctx, func(ctx context.Context, tx *sql.Tx, job worker.Job) error {
		key = job.RequestKey
		return d.handle(ctx, tx, job)
	}); err != nil {
		return conformance.Outcome{}, err
	}
	if key == "" {
		return conformance.Outcome{}, errors.New("nothing was pending redelivery")
	}
	return d.outcome(ctx, key)
}

func (d *webhookDriver) outcome(ctx context.Context, key string) (conformance.Outcome, error) {
	job, err := d.queue.Get(ctx, key)
	if err != nil {
		return conformance.Outcome{}, err
	}
	switch job.State {
	case worker.StateCompleted:
		return conformance.Outcome{Kind: conformance.Completed}, nil
	case worker.StatePending:
		return conformance.Outcome{Kind: conformance.Deferred}, nil
	case worker.StateDead:
		code := job.Error
		if code == "job handler failed" {
			code = "internal"
		}
		return conformance.Outcome{Kind: conformance.Rejected, Code: code, Message: job.Error}, nil
	}
	return conformance.Outcome{}, errors.New("job left in state " + job.State)
}

func (d *webhookDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

func (d *webhookDriver) stopServing(ctx context.Context) error {
	shutdownErr := d.app.Shutdown(ctx)
	closeErr := d.server.Close()
	d.client.CloseIdleConnections()
	d.mu.Lock()
	d.address = ""
	d.mu.Unlock()
	return errors.Join(shutdownErr, closeErr)
}

// Restart stops the listener and store, then rebuilds the adapter over the
// same database file.
func (d *webhookDriver) Restart(ctx context.Context) error {
	if err := errors.Join(d.stopServing(ctx), d.database.Close()); err != nil {
		return err
	}
	database, err := (sqlite.Backend{}).Open(ctx, d.path)
	if err != nil {
		return err
	}
	d.database = database
	if err := d.construct(ctx); err != nil {
		return err
	}
	return d.Start(ctx)
}

func (d *webhookDriver) Stop(ctx context.Context) error {
	return errors.Join(d.stopServing(ctx), d.database.Close())
}

func TestWebhookAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newWebhookDriver(t), corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		} else {
			t.Logf("not applicable: %s (%s)", result.CaseID, result.Reason)
		}
	}
	if ran != 14 {
		t.Fatalf("ran=%d report=%+v", ran, report)
	}
}

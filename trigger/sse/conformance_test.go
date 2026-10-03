package sse_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/worker"
)

const conformanceKind = "conformance.order"

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

// sseDriver starts work with a real POST, which the adapter submits to a real
// SQLite worker queue, and observes it through a real event stream. The
// queue's handler (the application's worker wiring) invokes the harness
// workflow, commits an effect row in the acknowledgment transaction and,
// once that commits, finishes the stream. The driver never calls the
// workflow itself.
type sseDriver struct {
	path     string
	env      conformance.TriggerEnv
	clock    *clock
	database store.Database
	queue    *worker.Queue
	hub      *sse.Hub
	app      *app.Application
	handler  *sse.Server
	server   *http.Server
	client   *http.Client
	mu       sync.Mutex
	address  string
}

func newSSEDriver(t *testing.T) *sseDriver {
	t.Helper()
	d := &sseDriver{path: filepath.Join(t.TempDir(), "jobs.db"), clock: &clock{now: t0}}
	database, err := (sqlite.Backend{}).Open(context.Background(), d.path)
	if err != nil {
		t.Fatal(err)
	}
	d.database = database
	return d
}

func (*sseDriver) Declaration() trigger.Declaration { return sse.Declaration }

func (d *sseDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	return d.construct(ctx)
}

func bearer(authenticate func(string) (trigger.Principal, error)) func(*http.Request) (trigger.Principal, error) {
	return func(request *http.Request) (trigger.Principal, error) {
		credential, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		if !ok || credential == "" {
			return trigger.Principal{}, errors.New("no credential")
		}
		return authenticate(credential)
	}
}

func (d *sseDriver) construct(ctx context.Context) error {
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
	// The hub is memory: a restart begins a new epoch.
	hub, err := sse.NewHub(sse.HubConfig{})
	if err != nil {
		return err
	}
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	handler, err := sse.New(application, hub, []sse.Endpoint{{
		Name: "orders", Path: "/orders", Kind: conformanceKind, Submit: queue, Tracker: queue, Authenticate: bearer(d.env.Authenticate), InputSchema: d.env.InputSchema,
	}})
	if err != nil {
		return err
	}
	d.queue, d.hub, d.app, d.handler = queue, hub, application, handler
	d.client = &http.Client{Transport: &http.Transport{}}
	return nil
}

func (d *sseDriver) Start(ctx context.Context) error {
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

func (d *sseDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

// work is the application's worker wiring for SSE-started jobs: it runs the
// workflow, commits its effect with the acknowledgment, and reports the
// outcome to the job's stream only after the job's transaction has
// committed. A saturated workflow fails the job instead of deferring it, so
// an interactive stream ends with "saturated" rather than waiting.
func (d *sseDriver) work(ctx context.Context) error {
	var stream string
	var output json.RawMessage
	var failure error
	processed, err := d.queue.ProcessOnce(ctx, func(ctx context.Context, tx *sql.Tx, job worker.Job) error {
		stream = sse.StreamID(job.RequestKey)
		output, failure = d.env.Workflow(ctx, conformance.Call{Input: job.Payload, Principal: job.Principal})
		if errors.Is(failure, trigger.ErrSaturated) {
			return &worker.HandlerError{Message: "saturated"}
		}
		if failure != nil {
			return failure
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
		return err
	})
	if err != nil || !processed {
		return errors.Join(err, errors.New("no job was processed"))
	}
	if failure != nil {
		_, err = d.hub.Finish(stream, sse.Failure(failure))
	} else {
		_, err = d.hub.Finish(stream, sse.Result(output))
	}
	return err
}

func (d *sseDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	address := d.Endpoint()
	if address == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/orders", bytes.NewReader(delivery.Payload))
	if err != nil {
		return conformance.Outcome{}, err
	}
	request.Header.Set("Idempotency-Key", delivery.Key)
	if delivery.Credential != "" {
		request.Header.Set("Authorization", "Bearer "+delivery.Credential)
	}
	response, err := d.client.Do(request)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
		}
		return conformance.Outcome{}, err
	}
	var started struct {
		Stream    string `json:"stream"`
		Duplicate bool   `json:"duplicate"`
		Error     string `json:"error"`
	}
	_ = json.NewDecoder(response.Body).Decode(&started)
	response.Body.Close()
	switch response.StatusCode {
	case http.StatusAccepted:
	case http.StatusOK:
		return conformance.Outcome{Kind: conformance.Duplicate}, nil
	case http.StatusUnauthorized, http.StatusBadRequest, http.StatusConflict, http.StatusServiceUnavailable:
		return conformance.Outcome{Kind: conformance.Rejected, Code: started.Error}, nil
	default:
		return conformance.Outcome{}, errors.New("unexpected start status " + response.Status)
	}
	// The start is acknowledged; the worker runs the work while the caller
	// follows its stream. The worker's context is not the caller's.
	worked := make(chan error, 1)
	go func() { worked <- d.work(context.WithoutCancel(ctx)) }()
	stream, refused, err := subscribe(ctx, d.client, "http://"+address+"/orders/"+started.Stream, delivery.Credential, "")
	if err != nil || refused != nil {
		<-worked
		status := ""
		if refused != nil {
			body, _ := io.ReadAll(refused.Body)
			status = fmt.Sprintf("%d %q %q", refused.StatusCode, refused.Status, body)
		}
		return conformance.Outcome{}, errors.Join(err, errors.New("subscription refused: "+status))
	}
	defer stream.close()
	frames := make(chan frame, 1)
	go func() {
		for {
			f, err := stream.next(time.Minute, false)
			if err != nil || f.Type == "result" || f.Type == "failed" {
				frames <- f
				return
			}
		}
	}()
	select {
	case <-delivery.Disconnect:
		// The caller goes away. The work is not the caller's to cancel; the
		// driver waits for it only so the harness can count its effect.
		stream.close()
		if err := <-worked; err != nil {
			return conformance.Outcome{}, err
		}
		return conformance.Outcome{Kind: conformance.Disconnected}, nil
	case f := <-frames:
		if err := <-worked; err != nil {
			return conformance.Outcome{}, err
		}
		switch f.Type {
		case "result":
			return conformance.Outcome{Kind: conformance.Completed, Output: json.RawMessage(f.Data)}, nil
		case "failed":
			var failure struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal([]byte(f.Data), &failure); err != nil {
				return conformance.Outcome{}, err
			}
			return conformance.Outcome{Kind: conformance.Rejected, Code: failure.Code, Message: f.Data}, nil
		}
		return conformance.Outcome{}, errors.New("the stream ended without a final event")
	}
}

func (d *sseDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

func (d *sseDriver) stopServing(ctx context.Context) error {
	shutdownErr := d.app.Shutdown(ctx)
	streamsErr := d.handler.Shutdown(ctx)
	closeErr := d.server.Close()
	d.client.CloseIdleConnections()
	d.mu.Lock()
	d.address = ""
	d.mu.Unlock()
	return errors.Join(shutdownErr, streamsErr, closeErr)
}

// Restart stops the listener and store, then rebuilds the adapter, its hub
// and its queue over the same database file.
func (d *sseDriver) Restart(ctx context.Context) error {
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

func (d *sseDriver) Stop(ctx context.Context) error {
	return errors.Join(d.stopServing(ctx), d.database.Close())
}

func TestSSEAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newSSEDriver(t), corpus, conformance.TriggerOptions{})
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

package catalog_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/catalog"
	"github.com/well-prado/new-blok/examples/order"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/provider"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

type fixture struct {
	Name, Kind, Scenario, Error string
	Input, Output               json.RawMessage
	ValueSchema                 json.RawMessage
	Requests, Effects           int
	Published                   *int
}

func effectManifest(capability string) provider.Manifest {
	return provider.Manifest{Capabilities: []string{capability}, SecretRefs: []string{"SYNTHETIC_TOKEN"}, MaxRequestBytes: 4096, MaxResponseBytes: 4096, Timeout: time.Second}
}

var generationSchema = []byte(`{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}`)

func TestEffectEndpointFixtures(t *testing.T) {
	raw, err := os.ReadFile("../testdata/providers/effects.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct{ Cases []fixture }
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures.Cases {
		t.Run(f.Name, func(t *testing.T) {
			switch f.Kind {
			case "http":
				runEndpoint(t, f, "http:request", catalog.HTTPRequest)
			case "email":
				runEndpoint(t, f, "email:send", catalog.Email)
			case "payment":
				runEndpoint(t, f, "payment:charge", catalog.Payment)
			case "database":
				runEndpoint(t, f, "database:write-outbox", catalog.Database)
			case "publish":
				runEndpoint(t, f, "message:publish", catalog.Publish)
			case "audit":
				runEndpoint(t, f, "audit:append", catalog.Audit)
			case "generate":
				runEndpoint(t, f, "model:generate", func(p provider.Port[provider.GenerateInput, provider.GenerateOutput], m provider.Manifest) (catalog.EffectNode[provider.GenerateInput, provider.GenerateOutput], error) {
					valueSchema := f.ValueSchema
					if len(valueSchema) == 0 {
						valueSchema = generationSchema
					}
					return catalog.Generate(p, m, valueSchema)
				})
			default:
				t.Fatal("unknown fixture kind")
			}
		})
	}
}

func runEndpoint[I provider.Keyed, O any](t *testing.T, f fixture, capability string, makeNode func(provider.Port[I, O], provider.Manifest) (catalog.EffectNode[I, O], error)) {
	t.Helper()
	var mu sync.Mutex
	requests, effects := 0, 0
	keys := map[string]string{}
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 8192))
		if err != nil {
			t.Error(err)
			return
		}
		if string(compactJSON(raw)) != string(compactJSON(f.Input)) {
			t.Errorf("input=%s want=%s", raw, f.Input)
		}
		if r.Header.Get("Authorization") != "Bearer synthetic-secret" {
			t.Error("credential did not reach endpoint")
		}
		mu.Lock()
		requests++
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			t.Error("missing key")
		}
		if f.Scenario != "business" && f.Scenario != "transient" {
			if previous, ok := keys[key]; ok {
				if previous != string(raw) {
					t.Error("conflicting repeat")
				}
			} else {
				keys[key] = string(raw)
				effects++
			}
		}
		mu.Unlock()
		switch f.Scenario {
		case "business":
			w.WriteHeader(422)
			_, _ = w.Write([]byte("synthetic-secret"))
			return
		case "transient":
			w.WriteHeader(429)
			return
		case "server-unknown":
			w.WriteHeader(500)
			return
		case "drop-after":
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		case "timeout-after":
			<-release
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(f.Output)
	}))
	defer server.Close()
	defer close(release)
	m := effectManifest(capability)
	if f.Scenario == "timeout-after" {
		m.Timeout = 100 * time.Millisecond
	}
	p, err := provider.NewEndpoint[I, O](server.URL, http.Header{"Authorization": []string{"Bearer synthetic-secret"}}, provider.HTTP{Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	n, err := makeNode(p, m)
	if err != nil {
		t.Fatal(err)
	}
	registry := node.NewRegistry()
	if err := registry.Register(n.Any()); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Lookup(n.Descriptor().Name, "1.0.0"); !ok {
		t.Fatal("not registered")
	}
	if n.Descriptor().Deterministic || len(n.Descriptor().Effects) != 1 || n.Descriptor().Effects[0] != capability {
		t.Fatal("untruthful effects")
	}
	manifest := n.Manifest()
	manifest.SecretRefs[0] = "mutated"
	if n.Manifest().SecretRefs[0] != "SYNTHETIC_TOKEN" {
		t.Fatal("manifest alias")
	}
	var input I
	if err := json.Unmarshal(f.Input, &input); err != nil {
		t.Fatal(err)
	}
	wf := flow.MustDefine[I, O](flow.Spec{Name: "fixture", Version: "1.0.0", Durability: flow.Memory}, func(w *flow.Builder, in flow.Ref[I]) flow.Ref[O] { return flow.Call(w, "effect", n.Definition, in) })
	program, err := wf.Lower()
	if err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatal("builder executed provider")
	}
	runner := engine.New(map[string]node.Any{n.Descriptor().Name: n.Any()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if f.Scenario == "cancel-before" {
		cancel()
	}
	count := 1
	if f.Scenario == "repeat" {
		count = 2
	}
	for i := 0; i < count; i++ {
		run, err := runner.Run(ctx, program, input)
		if f.Published != nil {
			published := 0
			if run.Output != nil {
				published = 1
			}
			if published != *f.Published {
				t.Fatalf("published=%d want=%d", published, *f.Published)
			}
		}
		if f.Error == "" {
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(run.Output)
			if string(compactJSON(got)) != string(compactJSON(f.Output)) {
				t.Fatalf("output=%s want=%s", got, f.Output)
			}
		} else {
			var classified *engine.Error
			if !errors.As(err, &classified) || classified.Class != f.Error {
				t.Fatalf("err=%v want %s", err, f.Error)
			}
			if len(run.State) != 0 || run.Output != nil {
				t.Fatal("failed effect published output")
			}
			checkSafe(t, err)
			if f.Error != "cancellation" && f.Error != "validation" {
				var domain *node.DomainError
				if !errors.As(err, &domain) || domain.Uncertain != (f.Error == "uncertain") || domain.Retryable != (f.Error == "transient") {
					t.Fatalf("wrong domain semantics: %v", err)
				}
				var pe *provider.Error
				if !errors.As(err, &pe) || pe.IdempotencyKey != input.EffectKey() {
					t.Fatal("lost provider identity")
				}
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != f.Requests || effects != f.Effects {
		t.Fatalf("requests=%d effects=%d want=%d/%d", requests, effects, f.Requests, f.Effects)
	}
}
func compactJSON(raw []byte) []byte {
	var value any
	_ = json.Unmarshal(raw, &value)
	compact, _ := json.Marshal(value)
	return compact
}
func checkSafe(t *testing.T, err error) {
	t.Helper()
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), "synthetic-secret") || strings.Contains(e.Error(), "http://") {
			t.Fatalf("unsafe error: %s", e)
		}
	}
}

type portFunc[I provider.Keyed, O any] func(context.Context, I) (O, error)

func (f portFunc[I, O]) Execute(ctx context.Context, i I) (O, error) { return f(ctx, i) }
func TestMissingProviderAndBadManifestFailComposition(t *testing.T) {
	var typedNil *provider.Endpoint[provider.PaymentInput, provider.Receipt]
	for _, p := range []provider.Port[provider.PaymentInput, provider.Receipt]{nil, typedNil} {
		if _, err := catalog.Payment(p, effectManifest("payment:charge")); err == nil {
			t.Fatal("missing provider accepted")
		}
	}
	p := portFunc[provider.PaymentInput, provider.Receipt](func(context.Context, provider.PaymentInput) (provider.Receipt, error) {
		t.Fatal("startup dispatched")
		return provider.Receipt{}, nil
	})
	if _, err := catalog.Payment(p, effectManifest("email:send")); err == nil {
		t.Fatal("broad/wrong capability accepted")
	}
	if _, err := catalog.Generate(nil, effectManifest("model:generate"), []byte(`{"type":"unsafe"}`)); err == nil {
		t.Fatal("invalid schema accepted")
	}
}
func TestUntrustedProviderErrorsAndPanicsAreSafe(t *testing.T) {
	for _, panicProvider := range []bool{false, true} {
		p := portFunc[provider.PaymentInput, provider.Receipt](func(context.Context, provider.PaymentInput) (provider.Receipt, error) {
			if panicProvider {
				panic("synthetic-secret")
			}
			return provider.Receipt{}, errors.New("https://user:synthetic-secret@example.invalid")
		})
		n, err := catalog.Payment(p, effectManifest("payment:charge"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = n.Invoke(context.Background(), provider.PaymentInput{Key: "key", Account: "synthetic", AmountCents: 1, Currency: "USD"})
		checkSafe(t, err)
		var domain *node.DomainError
		if !errors.As(err, &domain) || !domain.Uncertain || domain.Retryable {
			t.Fatal("unclassified dispatch was retryable")
		}
	}
}

func openDB(t *testing.T, path string) store.Database {
	t.Helper()
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
func countRows(t *testing.T, db store.Database, table string, state string) int {
	t.Helper()
	var count int
	err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		q := "SELECT COUNT(*) FROM " + table
		if state != "" {
			q += " WHERE state = '" + state + "'"
		}
		return tx.QueryRow(q).Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}
func TestDatabaseNodeRollsBackOutboxFailureAndDeduplicatesAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "effects.db")
	db := openDB(t, path)
	p, err := provider.NewRecords(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	n, err := catalog.Database(p, effectManifest("database:write-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`CREATE TRIGGER reject_outbox BEFORE INSERT ON provider_outbox BEGIN SELECT RAISE(ABORT,'synthetic-secret'); END`)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	input := provider.DatabaseInput{Key: "record-op", RecordID: "record-1", Value: "synthetic"}
	if _, err := n.Invoke(ctx, input); err == nil {
		t.Fatal("outbox failure accepted")
	} else {
		checkSafe(t, err)
	}
	if countRows(t, db, "provider_records", "") != 0 || countRows(t, db, "provider_outbox", "") != 0 {
		t.Fatal("partial transaction survived")
	}
	if err := db.WithTx(ctx, func(tx *sql.Tx) error { _, err := tx.Exec(`DROP TRIGGER reject_outbox`); return err }); err != nil {
		t.Fatal(err)
	}
	result, err := n.Invoke(ctx, input)
	if err != nil || result.RecordID != "record-1" || result.EventID != "event:record-op" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, path)
	defer db.Close()
	p, err = provider.NewRecords(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	n, err = catalog.Database(p, effectManifest("database:write-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Invoke(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.Value = "conflict"
	_, err = n.Invoke(ctx, input)
	var domain *node.DomainError
	if !errors.As(err, &domain) || domain.Class != "business" {
		t.Fatalf("conflict=%v", err)
	}
	if countRows(t, db, "provider_records", "") != 1 || countRows(t, db, "provider_outbox", "pending") != 1 {
		t.Fatal("duplicate business/outbox effect")
	}
}

func TestOrderOutboxLostEmailResponseRecoversWithStableProviderKey(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.db")
	db := openDB(t, path)
	var mu sync.Mutex
	requests, effects := 0, 0
	var keys []string
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		request := requests
		key := r.Header.Get("Idempotency-Key")
		keys = append(keys, key)
		if !seen[key] {
			seen[key] = true
			effects++
		}
		mu.Unlock()
		if request == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		_, _ = w.Write([]byte(`{"id":"mail-order"}`))
	}))
	defer server.Close()
	m := effectManifest("email:send")
	p, err := provider.NewEndpoint[provider.EmailInput, provider.Receipt](server.URL, nil, provider.HTTP{Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	email, err := catalog.Email(p, m)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := func(ctx context.Context, event order.Event) error {
		_, err := email.Invoke(ctx, provider.EmailInput{Key: event.ID, To: "synthetic@example.invalid", Subject: "order", Text: string(event.Payload)})
		return err
	}
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	service, err := order.New(ctx, db, map[string]int64{"coffee": 1500}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(ctx, order.Request{RequestKey: "outbox", SKU: "coffee", Quantity: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.DispatchOne(ctx, dispatch); err == nil {
		t.Fatal("lost response committed outbox")
	}
	if countRows(t, db, "order_outbox", "pending") != 1 {
		t.Fatal("pending outbox lost")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, path)
	defer db.Close()
	now = now.Add(time.Minute) // the restarted dispatcher runs after the retry backoff
	service, err = order.New(ctx, db, map[string]int64{"coffee": 1500}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if sent, err := service.DispatchOne(ctx, dispatch); err != nil || !sent {
		t.Fatalf("recovery=%v %v", sent, err)
	}
	if sent, err := service.DispatchOne(ctx, dispatch); err != nil || sent {
		t.Fatal("committed event redelivered")
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 || effects != 1 || keys[0] != keys[1] || keys[0] != "event:outbox" {
		t.Fatalf("requests=%d effects=%d keys=%v", requests, effects, keys)
	}
	if countRows(t, db, "order_outbox", "sent") != 1 {
		t.Fatal("outbox not committed")
	}
}

func TestJournalUnknownPaymentFailsClosedAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	db := openDB(t, path)
	j, err := journal.New(ctx, db, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "payment", Workflow: "fixture", ArtifactDigest: "synthetic-artifact", Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: admission.RunID, ArtifactDigest: "synthetic-artifact", InvocationPath: "payment", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, op.Key)
	if err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		if r.Header.Get("Idempotency-Key") != attempt.ProviderOperationKey {
			t.Error("wrong operation key")
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()
	m := effectManifest("payment:charge")
	p, err := provider.NewEndpoint[provider.PaymentInput, provider.Receipt](server.URL, nil, provider.HTTP{Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	n, err := catalog.Payment(p, m)
	if err != nil {
		t.Fatal(err)
	}
	_, err = n.Invoke(ctx, provider.PaymentInput{Key: attempt.ProviderOperationKey, Account: "synthetic", AmountCents: 3000, Currency: "USD"})
	var domain *node.DomainError
	if !errors.As(err, &domain) || !domain.Uncertain {
		t.Fatalf("err=%v", err)
	}
	if err := j.MarkUncertain(ctx, op.Key, attempt.ID, provider.RedactedError(err)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, path)
	defer db.Close()
	j, err = journal.New(ctx, db, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartAttempt(ctx, op.Key); !errors.Is(err, journal.ErrUncertain) {
		t.Fatalf("uncertain redispatched: %v", err)
	}
	if err := j.CommitEffect(ctx, journal.EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: json.RawMessage(`{"id":"fake"}`)}); !errors.Is(err, journal.ErrUncertain) {
		t.Fatalf("uncertain published: %v", err)
	}
	if effects.Load() != 1 {
		t.Fatalf("effects=%d", effects.Load())
	}
}

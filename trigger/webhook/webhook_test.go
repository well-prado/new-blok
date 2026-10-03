package webhook_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

var t0 = time.Unix(1_800_000_000, 0)

type fixture struct {
	InputSchema      json.RawMessage `json:"inputSchema"`
	Body             string          `json:"body"`
	MaxBodyBytes     int64           `json:"maxBodyBytes"`
	ToleranceMinutes int             `json:"toleranceMinutes"`
	Requests         []struct {
		Name        string `json:"name"`
		Mutation    string `json:"mutation"`
		Status      int    `json:"status"`
		Result      string `json:"result"`
		Submissions int    `json:"submissions"`
	} `json:"requests"`
	Rotation []struct {
		Name    string   `json:"name"`
		Event   string   `json:"event"`
		Minutes int      `json:"minutes"`
		Keys    []string `json:"keys"`
		Status  int      `json:"status"`
	} `json:"rotation"`
	Expected struct {
		Output     int `json:"output"`
		Duplicates int `json:"duplicates"`
		Errors     int `json:"errors"`
		Effects    int `json:"effects"`
	} `json:"expected"`
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "webhook", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func secret(t *testing.T, seed string) webhook.Secret {
	t.Helper()
	s, err := webhook.NewSecret([]byte(strings.Repeat(seed, 32)[:32]))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Set(at time.Time) {
	c.mu.Lock()
	c.now = at
	c.mu.Unlock()
}

// env is a webhook endpoint in front of a real SQLite worker queue, served
// over a real TCP listener.
type env struct {
	server   *httptest.Server
	queue    *worker.Queue
	database store.Database
	clock    *clock
}

var shopPrincipal = trigger.Principal{ID: "provider:shop", Roles: []string{"orders"}}

func newEnv(t *testing.T, f fixture, keys []webhook.Key, submit func(*worker.Queue) trigger.Submitter, options ...func(*webhook.Endpoint)) *env {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	c := &clock{now: t0}
	queue, err := worker.New(context.Background(), database, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	var submitter trigger.Submitter = queue
	if submit != nil {
		submitter = submit(queue)
	}
	endpoint := webhook.Endpoint{
		Path: "/hooks/orders", Provider: "shop", Principal: shopPrincipal, Kind: "order.event", Verifier: webhook.StandardWebhooks{Keys: keys},
		Submit: submitter, InputSchema: f.InputSchema, MaxBodyBytes: f.MaxBodyBytes, Tolerance: time.Duration(f.ToleranceMinutes) * time.Minute,
	}
	for _, option := range options {
		option(&endpoint)
	}
	handler, err := webhook.New(application, c.Now, []webhook.Endpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &env{server: server, queue: queue, database: database, clock: c}
}

type signed struct {
	method, path, id, timestamp, signature string
	body                                   []byte
}

func sign(s webhook.Secret, id string, at time.Time, body []byte) signed {
	return signed{method: http.MethodPost, path: "/hooks/orders", id: id, timestamp: strconv.FormatInt(at.Unix(), 10), signature: webhook.SignStandard(s, id, at, body), body: body}
}

func (e *env) send(t *testing.T, r signed) (int, string) {
	t.Helper()
	request, err := http.NewRequest(r.method, e.server.URL+r.path, bytes.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if r.id != "" {
		request.Header.Set("webhook-id", r.id)
	}
	if r.timestamp != "" {
		request.Header.Set("webhook-timestamp", r.timestamp)
	}
	if r.signature != "" {
		request.Header.Set("webhook-signature", r.signature)
	}
	response, err := e.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]string
	_ = json.NewDecoder(response.Body).Decode(&body)
	if body["status"] != "" {
		return response.StatusCode, body["status"]
	}
	return response.StatusCode, body["error"]
}

func (e *env) jobs(t *testing.T) int {
	t.Helper()
	var count int
	if err := e.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestSignedRequestMutationsOverRealListener(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	body := []byte(f.Body)
	base := sign(key, "evt_base", t0, body)
	output, duplicates, failures := 0, 0, 0
	for index, tc := range f.Requests {
		id := fmt.Sprintf("evt_%d", index)
		r := sign(key, id, t0, body)
		switch tc.Mutation {
		case "none":
			r = base
		case "replay":
			r = base
		case "whitespace":
			r.body = []byte(`{"sku": "coffee", "quantity": 2}`)
		case "body":
			r.body = []byte(`{"sku":"coffee","quantity":3}`)
		case "wrong-key":
			r = sign(secret(t, "zz"), id, t0, body)
		case "timestamp-header":
			r.timestamp = strconv.FormatInt(t0.Unix()+1, 10)
		case "id-header":
			r.id = id + "-forged"
		case "stale":
			r = sign(key, id, t0.Add(-6*time.Minute), body)
		case "future":
			r = sign(key, id, t0.Add(6*time.Minute), body)
		case "replay-after-window":
			e.clock.Set(t0.Add(6 * time.Minute))
			r = base
		case "no-signature":
			r.signature = ""
		case "malformed-signature":
			r.signature = "v1,!!!not-base64"
		case "unknown-version":
			r.signature = strings.Replace(r.signature, "v1,", "v2,", 1)
		case "too-many-signatures":
			r.signature = strings.TrimSpace(strings.Repeat("v1,AAAA ", webhook.MaxSignatures) + r.signature)
		case "bad-timestamp":
			r.timestamp = "soon"
		case "far-future":
			r = sign(key, id, time.Unix(1_000_000_000_000, 0), body)
		case "max-int64":
			r.timestamp = "9223372036854775807"
		case "valid-among-bogus":
			bogus := "v1," + base64.StdEncoding.EncodeToString(make([]byte, 32)) + " "
			r.signature = strings.Repeat(bogus, webhook.MaxSignatures-1) + r.signature
		case "long-id":
			r = sign(key, strings.Repeat("x", webhook.MaxEventIDBytes+1), t0, body)
		case "oversized":
			large := []byte(`{"sku":"` + strings.Repeat("c", int(f.MaxBodyBytes)) + `","quantity":2}`)
			r = sign(key, id, t0, large)
		case "malformed-json":
			r = sign(key, id, t0, []byte(`{"sku":`))
		case "wrong-type":
			r = sign(key, id, t0, []byte(`{"sku":"coffee","quantity":"two"}`))
		case "empty":
			r = sign(key, id, t0, nil)
		case "get":
			r.method = http.MethodGet
		case "path":
			r.path = "/hooks/unknown"
		default:
			t.Fatalf("unknown mutation %q", tc.Mutation)
		}
		before := e.jobs(t)
		status, result := e.send(t, r)
		e.clock.Set(t0)
		if status != tc.Status || result != tc.Result || e.jobs(t)-before != tc.Submissions {
			t.Fatalf("%s: status=%d result=%q submissions=%d, want %d %q %d", tc.Name, status, result, e.jobs(t)-before, tc.Status, tc.Result, tc.Submissions)
		}
		switch {
		case status == http.StatusAccepted:
			output++
		case status == http.StatusOK:
			duplicates++
		default:
			failures++
		}
	}

	oldKey, newKey := secret(t, "old"), secret(t, "new")
	r := newEnv(t, f, []webhook.Key{
		{ID: "old", Secret: oldKey, NotAfter: t0.Add(10 * time.Minute)},
		{ID: "new", Secret: newKey, NotBefore: t0},
	}, nil)
	for index, tc := range f.Rotation {
		at := t0.Add(time.Duration(tc.Minutes) * time.Minute)
		r.clock.Set(at)
		id := fmt.Sprintf("rot_%d", index)
		if tc.Event != "" {
			id = tc.Event
		}
		var entries []string
		for _, name := range tc.Keys {
			key := map[string]webhook.Secret{"old": oldKey, "new": newKey}[name]
			entries = append(entries, webhook.SignStandard(key, id, at, body))
		}
		request := sign(oldKey, id, at, body)
		request.signature = strings.Join(entries, " ")
		status, _ := r.send(t, request)
		if status != tc.Status {
			t.Fatalf("rotation %s: status=%d, want %d", tc.Name, status, tc.Status)
		}
		switch status {
		case http.StatusAccepted:
			output++
		case http.StatusOK:
			duplicates++
		default:
			failures++
		}
	}
	effects := e.jobs(t) + r.jobs(t)
	if output != f.Expected.Output || duplicates != f.Expected.Duplicates || failures != f.Expected.Errors || effects != f.Expected.Effects {
		t.Fatalf("output=%d duplicates=%d errors=%d effects=%d, want %+v", output, duplicates, failures, effects, f.Expected)
	}
}

func TestConcurrentDuplicateEventsCreateOneAcceptedRun(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	request := sign(key, "evt_concurrent", t0, []byte(f.Body))
	const senders = 64
	statuses := make(chan int, senders)
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < senders; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			status, _ := e.send(t, request)
			statuses <- status
		}()
	}
	close(start)
	group.Wait()
	close(statuses)
	counts := map[int]int{}
	for status := range statuses {
		counts[status]++
	}
	if counts[http.StatusAccepted] != 1 || counts[http.StatusOK] != senders-1 || e.jobs(t) != 1 {
		t.Fatalf("statuses=%v jobs=%d; want exactly one accepted run and %d duplicates", counts, e.jobs(t), senders-1)
	}
	runs := 0
	for {
		processed, err := e.queue.ProcessOnce(context.Background(), func(context.Context, *sql.Tx, worker.Job) error { runs++; return nil })
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if runs != 1 {
		t.Fatalf("runs=%d, want 1", runs)
	}
}

func TestVerifiedPrincipalTravelsThroughDurableSubmission(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	body := []byte(`{"sku":"coffee","quantity":2}`)
	if status, _ := e.send(t, sign(key, "evt_principal", t0, body)); status != http.StatusAccepted {
		t.Fatalf("status=%d", status)
	}
	var seen trigger.Principal
	if _, err := e.queue.ProcessOnce(context.Background(), func(_ context.Context, _ *sql.Tx, job worker.Job) error {
		seen = job.Principal
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen.ID != "provider:shop" || len(seen.Roles) != 1 {
		t.Fatalf("principal=%+v, want the endpoint's principal", seen)
	}
	job, err := e.queue.Get(context.Background(), webhook.SubmissionKey("shop", "evt_principal"))
	if err != nil || job.State != worker.StateCompleted {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

type stubSubmitter struct{ err error }

func (s stubSubmitter) Submit(context.Context, trigger.Submission) (bool, error) { return false, s.err }

func TestSubmissionFailuresMapToSafeResponses(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	for _, tc := range []struct {
		name   string
		err    error
		status int
		result string
	}{
		{"saturated", trigger.ErrSaturated, http.StatusServiceUnavailable, "saturated"},
		{"conflict", fmt.Errorf("wrapped: %w", trigger.ErrConflict), http.StatusConflict, "conflict"},
		{"invalid", trigger.ErrInvalidInput, http.StatusBadRequest, "invalid_input"},
		{"internal", errors.New("dsn=postgres://u:hunter2@db/orders"), http.StatusInternalServerError, "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, func(*worker.Queue) trigger.Submitter { return stubSubmitter{tc.err} })
			r := sign(key, "evt_"+tc.name, t0, []byte(f.Body))
			request, _ := http.NewRequest(http.MethodPost, e.server.URL+r.path, bytes.NewReader(r.body))
			request.Header.Set("webhook-id", r.id)
			request.Header.Set("webhook-timestamp", r.timestamp)
			request.Header.Set("webhook-signature", r.signature)
			response, err := e.server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			raw := new(bytes.Buffer)
			_, _ = raw.ReadFrom(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status || !strings.Contains(raw.String(), `"`+tc.result+`"`) || strings.Contains(raw.String(), "hunter2") {
				t.Fatalf("status=%d body=%s", response.StatusCode, raw)
			}
			if tc.err == trigger.ErrSaturated && response.Header.Get("Retry-After") == "" {
				t.Fatal("saturation without Retry-After")
			}
		})
	}
	// A conflicting reuse of an accepted event id through the real queue.
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	if status, _ := e.send(t, sign(key, "evt_reused", t0, []byte(f.Body))); status != http.StatusAccepted {
		t.Fatalf("first status=%d", status)
	}
	if status, result := e.send(t, sign(key, "evt_reused", t0, []byte(`{"sku":"coffee","quantity":3}`))); status != http.StatusConflict || result != "conflict" || e.jobs(t) != 1 {
		t.Fatalf("conflict status=%d result=%q jobs=%d", status, result, e.jobs(t))
	}
}

func TestSecretIsOpaqueAndRedacted(t *testing.T) {
	material := []byte("super-secret-signing-key-0123456789")
	s, err := webhook.NewSecret(material)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := webhook.ParseSecret("whsec_" + "c3VwZXItc2VjcmV0LXNpZ25pbmcta2V5LTAxMjM0NTY3ODk=")
	if err != nil {
		t.Fatal(err)
	}
	key := webhook.Key{ID: "k1", Secret: s}
	verifier := webhook.StandardWebhooks{Keys: []webhook.Key{key, {ID: "k2", Secret: encoded}}}
	// Printers bypass Secret's formatting for values reached through
	// unexported fields; the bytes must still not appear.
	hidden := struct {
		keys     []webhook.Key
		verifier acmeVerifier
	}{keys: []webhook.Key{key}, verifier: acmeVerifier{s}}
	application, _ := app.New(app.Config{})
	server, err := webhook.New(application, nil, []webhook.Endpoint{{Path: "/h", Provider: "shop", Principal: shopPrincipal, Kind: "k", Verifier: verifier, Submit: stubSubmitter{}, InputSchema: []byte(`{"type":"object"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("configured", "key", key, "secret", s, "verifier", verifier)
	jsonKey, _ := json.Marshal(key)
	jsonVerifier, _ := json.Marshal(verifier)
	_, verifyErr := verifier.Verify(webhook.Request{Header: http.Header{"Webhook-Id": {"e"}, "Webhook-Timestamp": {"1"}, "Webhook-Signature": {"v1,AAAA"}}, Received: t0})
	rendered := []string{
		fmt.Sprintf("%v", s), fmt.Sprintf("%+v", key), fmt.Sprintf("%#v", key), fmt.Sprintf("%s", s), fmt.Sprintf("%q", s), fmt.Sprintf("%x", s),
		fmt.Sprintf("%v", verifier), string(jsonKey), string(jsonVerifier), logs.String(), verifyErr.Error(),
		fmt.Sprintf("%+v", hidden), fmt.Sprintf("%#v", hidden), fmt.Sprintf("%v", hidden), fmt.Sprintf("%+v", server), fmt.Sprintf("%#v", server),
	}
	decimal := strings.Trim(fmt.Sprint(material), "[]")
	hexMaterial := hex.EncodeToString(material)
	for _, text := range rendered {
		if strings.Contains(text, string(material)) || strings.Contains(text, hexMaterial) || strings.Contains(text, "c3VwZXItc2VjcmV0") || strings.Contains(text, decimal[:20]) {
			t.Fatalf("secret material leaked: %s", text)
		}
	}
	if !strings.Contains(fmt.Sprintf("%v", s), "[redacted]") {
		t.Fatal("secret does not render as redacted")
	}
	if _, err := webhook.NewSecret([]byte("short")); err == nil {
		t.Fatal("short secret accepted")
	}
	if _, err := webhook.ParseSecret("plain-text"); err == nil {
		t.Fatal("unprefixed secret accepted")
	}
}

// acmeVerifier is a provider-specific scheme written only against the
// exported API: "Acme-Signature: t=<unix>,v1=<hex HMAC of t.body>" and the
// event id in "Acme-Event".
type acmeVerifier struct{ secret webhook.Secret }

func (v acmeVerifier) Verify(r webhook.Request) (webhook.Verified, error) {
	var stamp, signature string
	for _, part := range strings.Split(r.Header.Get("Acme-Signature"), ",") {
		name, value, _ := strings.Cut(part, "=")
		switch name {
		case "t":
			stamp = value
		case "v1":
			signature = value
		}
	}
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	mac, decodeErr := hex.DecodeString(signature)
	if err != nil || decodeErr != nil || r.Header.Get("Acme-Event") == "" {
		return webhook.Verified{}, webhook.ErrUnverified
	}
	if !hmac.Equal(v.secret.HMACSHA256([]byte(stamp+"."+string(r.Body))), mac) {
		return webhook.Verified{}, webhook.ErrUnverified
	}
	return webhook.Verified{EventID: r.Header.Get("Acme-Event"), Timestamp: time.Unix(seconds, 0), KeyID: "acme"}, nil
}

func TestProviderVerificationIsPluggableAndNamespaced(t *testing.T) {
	f := loadFixture(t)
	standardKey, acmeKey := secret(t, "k1"), secret(t, "ac")
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(context.Background(), database, func() time.Time { return t0 })
	if err != nil {
		t.Fatal(err)
	}
	application, _ := app.New(app.Config{})
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	handler, err := webhook.New(application, func() time.Time { return t0 }, []webhook.Endpoint{
		{Path: "/hooks/standard", Provider: "standard", Principal: trigger.Principal{ID: "provider:standard"}, Kind: "event", Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: standardKey}}}, Submit: queue, InputSchema: f.InputSchema},
		{Path: "/hooks/acme", Provider: "acme", Principal: trigger.Principal{ID: "provider:acme"}, Kind: "event", Verifier: acmeVerifier{acmeKey}, Submit: queue, InputSchema: f.InputSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	body := []byte(f.Body)
	post := func(path string, headers map[string]string) int {
		request, _ := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(body))
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	stamp := strconv.FormatInt(t0.Unix(), 10)
	acmeSignature := "t=" + stamp + ",v1=" + hex.EncodeToString(acmeKey.HMACSHA256([]byte(stamp+"."+string(body))))
	// The same event id from two providers is two events.
	if status := post("/hooks/standard", map[string]string{"webhook-id": "evt_1", "webhook-timestamp": stamp, "webhook-signature": webhook.SignStandard(standardKey, "evt_1", t0, body)}); status != http.StatusAccepted {
		t.Fatalf("standard status=%d", status)
	}
	if status := post("/hooks/acme", map[string]string{"Acme-Event": "evt_1", "Acme-Signature": acmeSignature}); status != http.StatusAccepted {
		t.Fatalf("acme status=%d", status)
	}
	// Each provider's signature is only valid on its own endpoint.
	if status := post("/hooks/standard", map[string]string{"Acme-Event": "evt_2", "Acme-Signature": acmeSignature}); status != http.StatusUnauthorized {
		t.Fatalf("acme signature on standard endpoint status=%d", status)
	}
	// A custom verifier that returns a far-future timestamp is still held to
	// the window by the adapter (no duration overflow).
	future := "253402300799"
	futureSignature := "t=" + future + ",v1=" + hex.EncodeToString(acmeKey.HMACSHA256([]byte(future+"."+string(body))))
	if status := post("/hooks/acme", map[string]string{"Acme-Event": "evt_future", "Acme-Signature": futureSignature}); status != http.StatusBadRequest {
		t.Fatalf("far-future custom timestamp status=%d, want 400 stale_event", status)
	}
	for _, key := range []string{webhook.SubmissionKey("standard", "evt_1"), webhook.SubmissionKey("acme", "evt_1")} {
		job, err := queue.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if want := "provider:" + strings.Split(key, ":")[1]; job.Principal.ID != want {
			t.Fatalf("%s principal=%q, want %q", key, job.Principal.ID, want)
		}
	}
}

func TestEndpointConfigurationIsBounded(t *testing.T) {
	application, _ := app.New(app.Config{})
	schema := []byte(`{"type":"object"}`)
	valid := webhook.Endpoint{Path: "/hooks", Provider: "shop", Principal: shopPrincipal, Kind: "event", Verifier: webhook.StandardWebhooks{}, Submit: stubSubmitter{}, InputSchema: schema}
	for name, mutate := range map[string]func(*webhook.Endpoint){
		"missing principal": func(e *webhook.Endpoint) { e.Principal = trigger.Principal{} },
		"unbounded read":    func(e *webhook.Endpoint) { e.ReadTimeout = webhook.MaxReadTimeout + time.Second },
		"missing verifier":  func(e *webhook.Endpoint) { e.Verifier = nil },
		"missing submitter": func(e *webhook.Endpoint) { e.Submit = nil },
		"bad provider name": func(e *webhook.Endpoint) { e.Provider = "Shop Inc" },
		"relative path":     func(e *webhook.Endpoint) { e.Path = "hooks" },
		"unbounded body":    func(e *webhook.Endpoint) { e.MaxBodyBytes = webhook.MaxBodyBytesLimit + 1 },
		"unbounded window":  func(e *webhook.Endpoint) { e.Tolerance = webhook.MaxTolerance + time.Second },
		"invalid schema":    func(e *webhook.Endpoint) { e.InputSchema = []byte(`{"type":"nope"}`) },
	} {
		endpoint := valid
		mutate(&endpoint)
		if _, err := webhook.New(application, nil, []webhook.Endpoint{endpoint}); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := webhook.New(application, nil, []webhook.Endpoint{valid, valid}); err == nil {
		t.Fatal("duplicate path accepted")
	}
	sameProvider := valid
	sameProvider.Path = "/hooks/tenant-b"
	if _, err := webhook.New(application, nil, []webhook.Endpoint{valid, sameProvider}); err == nil {
		t.Fatal("two endpoints sharing a provider namespace accepted")
	}
	tooManyKeys := webhook.StandardWebhooks{Keys: make([]webhook.Key, webhook.MaxKeys+1)}
	if _, err := tooManyKeys.Verify(webhook.Request{Header: http.Header{"Webhook-Id": {"e"}, "Webhook-Timestamp": {"1800000000"}, "Webhook-Signature": {"v1," + base64.StdEncoding.EncodeToString(make([]byte, 32))}}, Received: t0}); !errors.Is(err, webhook.ErrUnverified) {
		t.Fatalf("verifier with more than MaxKeys keys err=%v", err)
	}
	if _, err := webhook.New(application, nil, []webhook.Endpoint{valid}); err != nil {
		t.Fatal(err)
	}
}

// TestSlowBodyIsBoundedByReadTimeout: an unauthenticated caller that stops
// sending its body loses the admission slot after ReadTimeout.
func TestSlowBodyIsBoundedByReadTimeout(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: secret(t, "k1")}}, nil, func(endpoint *webhook.Endpoint) { endpoint.ReadTimeout = 200 * time.Millisecond })
	conn, err := net.Dial("tcp", strings.TrimPrefix(e.server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /hooks/orders HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"sku\":")
	started := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response to a stalled body: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusRequestTimeout || time.Since(started) > 3*time.Second || e.jobs(t) != 0 {
		t.Fatalf("status=%d after %v jobs=%d", response.StatusCode, time.Since(started), e.jobs(t))
	}
}

// TestBusyStoreAnswersSaturated: a delivery whose submission cannot get the
// store's write lock within the busy timeout is answered 503 saturated with
// Retry-After, never 500, and nothing is committed; the provider's retry
// then succeeds.
func TestBusyStoreAnswersSaturated(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- e.database.WithTx(context.Background(), func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(context.Background(), `UPDATE worker_jobs SET updated_at = updated_at`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS hold (x INTEGER)`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	r := sign(key, "evt_busy", t0, []byte(f.Body))
	request, err := http.NewRequest(r.method, e.server.URL+r.path, bytes.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("webhook-id", r.id)
	request.Header.Set("webhook-timestamp", r.timestamp)
	request.Header.Set("webhook-signature", r.signature)
	response, err := e.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var answer map[string]string
	_ = json.NewDecoder(response.Body).Decode(&answer)
	response.Body.Close()
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || answer["error"] != "saturated" || response.Header.Get("Retry-After") != "1" {
		t.Fatalf("a busy store answered %d %v Retry-After=%q; want 503 saturated", response.StatusCode, answer, response.Header.Get("Retry-After"))
	}
	if n := e.jobs(t); n != 0 {
		t.Fatalf("a saturated delivery committed %d jobs", n)
	}
	if status, code := e.send(t, r); status != http.StatusAccepted {
		t.Fatalf("the provider's retry: %d %s", status, code)
	}
}

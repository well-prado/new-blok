package shop

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

//go:embed fixtures.json
var fixtureJSON []byte

type expectedCase struct {
	ID                      string `json:"id"`
	ExpectedStatus          int    `json:"expectedStatus"`
	ExpectedRecords         int    `json:"expectedRecords"`
	ExpectedOutbox          int    `json:"expectedOutbox"`
	ExpectedEffects         int    `json:"expectedEffects"`
	ExpectedAccepted        int    `json:"expectedAcceptedJobs"`
	ExpectedJobState        string `json:"expectedJobState"`
	ExpectedClose           string `json:"expectedCloseReason"`
	MaxSubscriberQueue      int    `json:"maxSubscriberQueue"`
	ExpectedPublishAttempts int    `json:"expectedPublishAttempts"`
	ExpectedStableEventID   bool   `json:"expectedStableEventId"`
	ExpectedValue           string `json:"expectedValue"`
	ExpectedMaxClaims       int    `json:"expectedMaxClaims"`
	ExpectedOutboxState     string `json:"expectedOutboxState"`
}

func fixture(t testing.TB, id string) expectedCase {
	t.Helper()
	var document struct {
		SchemaVersion string         `json:"schemaVersion"`
		Cases         []expectedCase `json:"cases"`
	}
	if err := json.Unmarshal(fixtureJSON, &document); err != nil {
		t.Fatal(err)
	}
	if document.SchemaVersion != "recipes/v1" {
		t.Fatalf("fixture schema=%q", document.SchemaVersion)
	}
	for _, item := range document.Cases {
		if item.ID == id {
			return item
		}
	}
	t.Fatalf("missing fixture case %q", id)
	return expectedCase{}
}

const (
	aliceToken = "alice-example-token-0001"
	bobToken   = "bob-example-token-00002"
	webhookKey = "synthetic-webhook-key-00000001"
)

func openRecipe(t testing.TB, path string) (*Application, func()) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	sinkDatabase, err := (sqlite.Backend{}).Open(context.Background(), path+".sink.db")
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	sink, err := NewSyntheticSink(context.Background(), sinkDatabase)
	if err != nil {
		_ = sinkDatabase.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	application, err := New(context.Background(), Config{
		Database:    database,
		Tokens:      map[string]string{"alice": aliceToken, "bob": bobToken},
		WebhookKey:  []byte(webhookKey),
		Publisher:   sink,
		OutboxLease: 120 * time.Millisecond,
	})
	if err != nil {
		_ = sinkDatabase.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	return application, func() { _ = database.Close(); _ = sinkDatabase.Close() }
}

func TestMigrationsReplayUpgradeAndTeardown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh", "shop.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// Start from a real v1 database to exercise the upgrade path.
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE shop_schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[0][0]); err != nil {
			return err
		}
		// A v1 row must survive every later migration, including the v4
		// owner-scoped rebuild of shop_records.
		if _, err := tx.Exec(`INSERT INTO shop_records(record_id, owner_id, value, created_at) VALUES('legacy-1', 'alice', 'kept', 1)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO shop_schema_migrations(version, applied_at) VALUES(1, 1)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := Migrate(context.Background(), database); err != nil {
			t.Fatal(err)
		}
	}
	var version int
	var migrationsApplied int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT MAX(version), COUNT(*) FROM shop_schema_migrations`).Scan(&version, &migrationsApplied); err != nil {
			return err
		}
		var updatedAtColumn int
		rows, err := tx.Query(`PRAGMA table_info(shop_records)`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var cid, notNull, primary int
			var name, kind string
			var fallback sql.NullString
			if err := rows.Scan(&cid, &name, &kind, &notNull, &fallback, &primary); err != nil {
				return err
			}
			if name == "updated_at" {
				updatedAtColumn++
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if updatedAtColumn != 1 {
			return fmt.Errorf("updated_at columns=%d", updatedAtColumn)
		}
		var owner, value, incarnation string
		if err := tx.QueryRow(`SELECT owner_id, value, incarnation FROM shop_records WHERE record_id = 'legacy-1'`).Scan(&owner, &value, &incarnation); err != nil {
			return fmt.Errorf("v1 row after upgrade: %w", err)
		}
		if owner != "alice" || value != "kept" || len(incarnation) != 32 {
			return fmt.Errorf("v1 row after upgrade owner=%q value=%q incarnation=%q", owner, value, incarnation)
		}
		// The same ID under another owner is a separate record (owner-scoped key).
		if _, err := tx.Exec(`INSERT INTO shop_records(owner_id, record_id, incarnation, value, created_at) VALUES('bob', 'legacy-1', 'x', 'other', 2)`); err != nil {
			return fmt.Errorf("owner-scoped key: %w", err)
		}
		if _, err := tx.Exec(`INSERT INTO shop_records(owner_id, record_id, incarnation, value, created_at) VALUES('alice', 'legacy-1', 'y', 'dup', 3)`); err == nil {
			return errors.New("duplicate (owner, record) accepted")
		}
		var attemptsColumn int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('shop_outbox') WHERE name = 'attempts'`).Scan(&attemptsColumn); err != nil {
			return err
		}
		if attemptsColumn != 1 {
			return fmt.Errorf("outbox attempts columns=%d", attemptsColumn)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if version != len(migrations) || version != 4 || migrationsApplied != 4 {
		t.Fatalf("version=%d migration rows=%d, want 4 and 4", version, migrationsApplied)
	}
	if _, err := worker.New(context.Background(), database, nil); err != nil {
		t.Fatal(err)
	}
	if err := Teardown(context.Background(), database); err != nil {
		t.Fatal(err)
	}
	var queueTables int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('worker_jobs', 'worker_compacted', 'worker_meta')`).Scan(&queueTables)
	}); err != nil {
		t.Fatal(err)
	}
	if queueTables != 0 {
		t.Fatalf("worker queue tables after teardown=%d", queueTables)
	}
	if err := Migrate(context.Background(), database); err != nil {
		t.Fatalf("clean setup after teardown: %v", err)
	}
}

func TestAuthenticatedCRUDSeparatesTwoPrincipals(t *testing.T) {
	application, closeDatabase := openRecipe(t, filepath.Join(t.TempDir(), "crud.db"))
	defer closeDatabase()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	server := httptest.NewServer(application.Handler)
	defer server.Close()

	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"record-1","value":"synthetic"}`)
	crudExpected := fixture(t, "owner-crud")
	if created.StatusCode != crudExpected.ExpectedStatus {
		t.Fatalf("create status=%d body=%s", created.StatusCode, responseBody(t, created))
	}
	var record Record
	if err := json.NewDecoder(created.Body).Decode(&record); err != nil {
		t.Fatal(err)
	}
	_ = created.Body.Close()
	if record.Owner != "alice" || record.ID != "record-1" {
		t.Fatalf("record=%+v", record)
	}
	crudRecords, crudOutbox := recipeCounts(t, application)
	if crudRecords != crudExpected.ExpectedRecords || crudOutbox != crudExpected.ExpectedOutbox {
		t.Fatalf("owner CRUD records=%d outbox=%d", crudRecords, crudOutbox)
	}
	denied := request(t, server.Client(), http.MethodGet, server.URL+"/records/record-1", bobToken, "")
	deniedExpected := fixture(t, "cross-principal-read")
	if denied.StatusCode != deniedExpected.ExpectedStatus {
		t.Fatalf("cross-principal read status=%d, want concealed denial", denied.StatusCode)
	}
	_ = denied.Body.Close()
	afterDeniedRecords, afterDeniedOutbox := recipeCounts(t, application)
	effectDelta := afterDeniedRecords - crudRecords + afterDeniedOutbox - crudOutbox
	if deniedExpected.ExpectedEffects == 0 && (afterDeniedRecords != crudRecords || afterDeniedOutbox != crudOutbox) || deniedExpected.ExpectedEffects != 0 && effectDelta != deniedExpected.ExpectedEffects {
		t.Fatalf("cross-principal effects changed records/outbox from %d/%d to %d/%d", crudRecords, crudOutbox, afterDeniedRecords, afterDeniedOutbox)
	}
	for _, attempt := range []struct {
		name   string
		method string
		body   string
	}{
		{name: "update", method: http.MethodPut, body: `{"requestKey":"bob-cannot-update","value":"stolen"}`},
		{name: "delete", method: http.MethodDelete},
	} {
		t.Run("deny-"+attempt.name, func(t *testing.T) {
			response := request(t, server.Client(), attempt.method, server.URL+"/records/record-1", bobToken, attempt.body)
			expected := fixture(t, "cross-principal-"+attempt.name)
			if response.StatusCode != expected.ExpectedStatus {
				t.Fatalf("cross-principal %s status=%d", attempt.name, response.StatusCode)
			}
			_ = response.Body.Close()
			records, outbox := recipeCounts(t, application)
			if records != crudRecords || outbox != crudOutbox || expected.ExpectedEffects != 0 {
				t.Fatalf("cross-principal %s changed records/outbox to %d/%d", attempt.name, records, outbox)
			}
		})
	}
	allowed := request(t, server.Client(), http.MethodGet, server.URL+"/records/record-1", aliceToken, "")
	if allowed.StatusCode != http.StatusOK {
		t.Fatalf("owner read status=%d", allowed.StatusCode)
	}
	_ = allowed.Body.Close()
	updated := request(t, server.Client(), http.MethodPut, server.URL+"/records/record-1", aliceToken, `{"requestKey":"update-record-1","value":"updated"}`)
	updateExpected := fixture(t, "owner-update")
	if updated.StatusCode != updateExpected.ExpectedStatus {
		t.Fatalf("owner update status=%d body=%s", updated.StatusCode, responseBody(t, updated))
	}
	var updatedRecord Record
	if err := json.NewDecoder(updated.Body).Decode(&updatedRecord); err != nil {
		t.Fatal(err)
	}
	_ = updated.Body.Close()
	if updatedRecord.Value != updateExpected.ExpectedValue {
		t.Fatalf("updated value=%q", updatedRecord.Value)
	}
	// Repeating the same operation key and payload is idempotent.
	duplicateUpdate := request(t, server.Client(), http.MethodPut, server.URL+"/records/record-1", aliceToken, `{"requestKey":"update-record-1","value":"updated"}`)
	if duplicateUpdate.StatusCode != updateExpected.ExpectedStatus {
		t.Fatalf("duplicate update status=%d body=%s", duplicateUpdate.StatusCode, responseBody(t, duplicateUpdate))
	}
	_ = duplicateUpdate.Body.Close()
	updatedRecords, updatedOutbox := recipeCounts(t, application)
	if updatedRecords != 1 || updatedOutbox != updateExpected.ExpectedOutbox {
		t.Fatalf("updated records=%d outbox=%d", updatedRecords, updatedOutbox)
	}
	removed := request(t, server.Client(), http.MethodDelete, server.URL+"/records/record-1", aliceToken, "")
	deleteExpected := fixture(t, "owner-delete")
	if removed.StatusCode != deleteExpected.ExpectedStatus {
		t.Fatalf("owner delete status=%d", removed.StatusCode)
	}
	_ = removed.Body.Close()
	remainingRecords, remainingOutbox := recipeCounts(t, application)
	if remainingRecords != deleteExpected.ExpectedRecords || remainingOutbox != deleteExpected.ExpectedOutbox {
		t.Fatalf("after delete records=%d outbox=%d", remainingRecords, remainingOutbox)
	}
}

func TestCallerPrincipalsRequireDistinctTokens(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "tokens.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	_, err = New(context.Background(), Config{
		Database:   database,
		Tokens:     map[string]string{"alice": aliceToken, "bob": aliceToken},
		WebhookKey: []byte(webhookKey),
		Publisher:  &SyntheticSink{},
	})
	if err == nil || !strings.Contains(err.Error(), "tokens must be distinct") {
		t.Fatalf("New with duplicate caller tokens error=%v", err)
	}
}

func TestAuthenticationSnapshotsCallerOwnedTokenMap(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "token-snapshot.db")
	database, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	sinkDatabase, err := (sqlite.Backend{}).Open(ctx, path+".sink.db")
	if err != nil {
		t.Fatal(err)
	}
	defer sinkDatabase.Close()
	sink, err := NewSyntheticSink(ctx, sinkDatabase)
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{"alice": aliceToken, "bob": bobToken}
	application, err := New(ctx, Config{Database: database, Tokens: tokens, WebhookKey: []byte(webhookKey), Publisher: sink})
	if err != nil {
		t.Fatal(err)
	}
	tokens["alice"], tokens["bob"] = bobToken, aliceToken
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(ctx)
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	mutateDone := make(chan struct{})
	mutatorStopped := make(chan struct{})
	go func() {
		defer close(mutatorStopped)
		for {
			select {
			case <-mutateDone:
				return
			default:
				tokens["alice"], tokens["bob"] = aliceToken, bobToken
				tokens["alice"], tokens["bob"] = bobToken, aliceToken
			}
		}
	}()
	defer func() {
		close(mutateDone)
		<-mutatorStopped
	}()
	for _, principal := range []struct {
		token string
		owner string
		id    string
	}{{token: aliceToken, owner: "alice", id: "snapshot-alice"}, {token: bobToken, owner: "bob", id: "snapshot-bob"}} {
		response := request(t, server.Client(), http.MethodPost, server.URL+"/records", principal.token, `{"id":"`+principal.id+`","value":"stable principal"}`)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("original token for %s status=%d body=%s", principal.owner, response.StatusCode, responseBody(t, response))
		}
		var record Record
		if err := json.NewDecoder(response.Body).Decode(&record); err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if record.Owner != principal.owner {
			t.Fatalf("original token authenticated as %q, want %q", record.Owner, principal.owner)
		}
	}
	for range 8 {
		response := request(t, server.Client(), http.MethodGet, server.URL+"/records/no-such-record", aliceToken, "")
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("concurrent map mutation changed authentication status to %d", response.StatusCode)
		}
		_ = response.Body.Close()
	}
}

func TestSignedWebhookDuplicateSurvivesProcessRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable", "shop.db")
	application, closeDatabase := openRecipe(t, path)
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(application.Handler)
	body := `{"requestKey":"hook:evt-1","recordId":"webhook-record","value":"signed"}`
	tampered := sendWebhookWithSignature(t, server.Client(), server.URL, "evt-1", []byte(body), []byte(strings.Replace(body, "signed", "changed", 1)))
	if tampered.StatusCode != fixture(t, "signed-webhook-tampered").ExpectedStatus {
		t.Fatalf("tampered delivery status=%d body=%s", tampered.StatusCode, responseBody(t, tampered))
	}
	_ = tampered.Body.Close()
	if count := jobCount(t, application, webhook.SubmissionKey("synthetic", "evt-1")); count != 0 {
		t.Fatalf("tampered webhook admitted %d jobs", count)
	}
	first := sendWebhook(t, server.Client(), server.URL, "evt-1", []byte(body))
	firstExpected := fixture(t, "signed-webhook-first")
	if first.StatusCode != firstExpected.ExpectedStatus {
		t.Fatalf("first delivery status=%d body=%s", first.StatusCode, responseBody(t, first))
	}
	_ = first.Body.Close()
	duplicate := sendWebhook(t, server.Client(), server.URL, "evt-1", []byte(body))
	duplicateExpected := fixture(t, "signed-webhook-duplicate")
	if duplicate.StatusCode != duplicateExpected.ExpectedStatus {
		t.Fatalf("duplicate delivery status=%d body=%s", duplicate.StatusCode, responseBody(t, duplicate))
	}
	_ = duplicate.Body.Close()
	if count := jobCount(t, application, webhook.SubmissionKey("synthetic", "evt-1")); count != firstExpected.ExpectedAccepted || count != duplicateExpected.ExpectedAccepted {
		t.Fatalf("accepted webhook jobs=%d, want %d", count, firstExpected.ExpectedAccepted)
	}
	server.Close()
	if err := application.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeDatabase()

	// A new test process opens the same SQLite journal and consumes the job.
	// No in-memory queue or handler state is shared with the first process.
	command := exec.Command(os.Args[0], "-test.run=^TestRecipeWorkerProcess$")
	childEnv := make([]string, 0, len(os.Environ())+1)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "SHOP_RECIPE_CHILD_DB=") {
			childEnv = append(childEnv, value)
		}
	}
	command.Env = append(childEnv, "SHOP_RECIPE_CHILD_DB="+path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("restart worker process: %v\n%s", err, output)
	}

	reopened, closeReopened := openRecipe(t, path)
	defer closeReopened()
	job, err := reopened.Queue.Get(context.Background(), webhook.SubmissionKey("synthetic", "evt-1"))
	if err != nil {
		t.Fatal(err)
	}
	restartExpected := fixture(t, "restart-worker")
	if job.State != restartExpected.ExpectedJobState {
		t.Fatalf("job state=%q error=%q, want %q", job.State, job.Error, restartExpected.ExpectedJobState)
	}
	var records, outbox int
	if err := reopened.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT COUNT(*) FROM shop_records WHERE record_id = 'webhook-record'`).Scan(&records); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT COUNT(*) FROM shop_outbox WHERE record_id = 'webhook-record'`).Scan(&outbox)
	}); err != nil {
		t.Fatal(err)
	}
	if records != restartExpected.ExpectedRecords || outbox != restartExpected.ExpectedOutbox {
		t.Fatalf("post-restart records=%d outbox=%d", records, outbox)
	}
}

func TestOutboxFailureRetainsEventAndRetryUsesSameIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-outbox.db")
	application, closeDatabase := openRecipe(t, path)
	defer closeDatabase()
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"outbox-1","value":"retry"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.StatusCode, responseBody(t, created))
	}
	_ = created.Body.Close()
	expected := fixture(t, "outbox-retry")
	sink := application.publisher.(*SyntheticSink)
	sink.FailAfterAcceptOnce()
	first, err := application.DrainOutbox(context.Background())
	if !first || err == nil {
		t.Fatalf("accepted-then-error processed=%v err=%v", first, err)
	}
	eventID, err := latestOutboxID(t, application)
	if err != nil {
		t.Fatal(err)
	}
	var published RecordEvent
	if err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		var payload []byte
		if err := tx.QueryRow(`SELECT payload_json FROM shop_outbox WHERE event_id = ?`, eventID).Scan(&payload); err != nil {
			return err
		}
		return json.Unmarshal(payload, &published)
	}); err != nil {
		t.Fatal(err)
	}
	if published.Type != "shop.record.changed" || published.EventID != eventID {
		t.Fatalf("typed workflow outbox event=%+v", published)
	}
	if count := sinkEventCount(t, sink); count != expected.ExpectedEffects {
		t.Fatalf("sink accepted %d effects before simulated response loss", count)
	}
	server.Close()
	if err := application.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeDatabase()

	// A fresh app process sees the durable lease, waits for expiry, retries the
	// same event ID, and reconciles the receiver's already-accepted effect.
	restarted, closeRestarted := openRecipe(t, path)
	defer closeRestarted()
	time.Sleep(restarted.outboxLease + 20*time.Millisecond)
	processed, err := restarted.DrainOutbox(context.Background())
	if !processed || err != nil {
		t.Fatalf("post-restart reconciliation processed=%v err=%v", processed, err)
	}
	restartedSink := restarted.publisher.(*SyntheticSink)
	if count := sinkEventCount(t, restartedSink); count != expected.ExpectedEffects {
		t.Fatalf("sink effects after deduplicated restart retry=%d", count)
	}
	if err := restartedSink.Publish(context.Background(), eventID, []byte(`{"type":"changed"}`)); err == nil {
		t.Fatal("synthetic sink accepted a changed payload under an existing event ID")
	}
	state, retriedID := outboxState(t, restarted)
	if state != "sent" || expected.ExpectedPublishAttempts != 2 || (expected.ExpectedStableEventID && retriedID != eventID) {
		t.Fatalf("outbox state=%q eventID=%q original=%q", state, retriedID, eventID)
	}
	processed, err = restarted.DrainOutbox(context.Background())
	if processed || err != nil {
		t.Fatalf("post-ack drain processed=%v err=%v", processed, err)
	}
}

func latestOutboxID(t *testing.T, application *Application) (string, error) {
	t.Helper()
	var eventID string
	err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT event_id FROM shop_outbox ORDER BY created_at DESC, event_id DESC LIMIT 1`).Scan(&eventID)
	})
	return eventID, err
}

func outboxState(t *testing.T, application *Application) (string, string) {
	t.Helper()
	var state, eventID string
	if err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT state, event_id FROM shop_outbox ORDER BY created_at DESC, event_id DESC LIMIT 1`).Scan(&state, &eventID)
	}); err != nil {
		t.Fatal(err)
	}
	return state, eventID
}

func sinkEventCount(t *testing.T, sink *SyntheticSink) int {
	t.Helper()
	var count int
	if err := sink.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM synthetic_sink_events`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

type blockingPublisher struct {
	inner   OutboxPublisher
	started chan struct{}
	release chan struct{}
}

func (p blockingPublisher) Publish(ctx context.Context, eventID string, payload []byte) error {
	close(p.started)
	<-p.release
	return p.inner.Publish(ctx, eventID, payload)
}

func TestOutboxClaimIsAtomicAndPublisherHoldsNoSQLiteWriteLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "claim.db")
	application, closeDatabase := openRecipe(t, path)
	defer closeDatabase()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	secondDrainer, closeSecondDrainer := openRecipe(t, path)
	defer closeSecondDrainer()
	application.outboxLease = 5 * time.Second
	expected := fixture(t, "outbox-atomic-claim")
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"claim-1","value":"bounded"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.StatusCode, responseBody(t, created))
	}
	_ = created.Body.Close()
	blocking := blockingPublisher{inner: application.publisher, started: make(chan struct{}), release: make(chan struct{})}
	application.publisher = blocking
	firstResult := make(chan error, 1)
	go func() {
		processed, err := application.DrainOutbox(context.Background())
		if err == nil && !processed {
			err = errors.New("first drainer did not claim the pending event")
		}
		firstResult <- err
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("publisher did not start")
	}
	// A write from another connection must proceed while the external publisher
	// is blocked; publication cannot hold the claim transaction open.
	writerCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := application.Database.WithTx(writerCtx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(writerCtx, `CREATE TABLE IF NOT EXISTS contention_probe (id INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatalf("concurrent sqlite writer was blocked by publisher: %v", err)
	}
	for range 3 {
		processed, err := secondDrainer.DrainOutbox(context.Background())
		if err != nil || processed || expected.ExpectedMaxClaims != 1 {
			t.Fatalf("competing drainer processed=%v err=%v; event must remain leased", processed, err)
		}
	}
	close(blocking.release)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	if count := sinkEventCount(t, application.publisher.(blockingPublisher).inner.(*SyntheticSink)); count != 1 {
		t.Fatalf("sink event effects=%d, want one", count)
	}
}

func TestConcurrentTwoPrincipalWritesUnderStorageLoad(t *testing.T) {
	application, closeDatabase := openRecipe(t, filepath.Join(t.TempDir(), "concurrent.db"))
	defer closeDatabase()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	const concurrentRequests = 16
	expected := fixture(t, "concurrent-storage")
	errorsFound := make(chan error, concurrentRequests)
	var writers sync.WaitGroup
	for index := range concurrentRequests {
		writers.Add(1)
		go func() {
			defer writers.Done()
			token := aliceToken
			if index%2 != 0 {
				token = bobToken
			}
			body := fmt.Sprintf(`{"id":"load-%02d","value":"bounded"}`, index)
			request, err := http.NewRequest(http.MethodPost, server.URL+"/records", strings.NewReader(body))
			if err != nil {
				errorsFound <- err
				return
			}
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			response, err := server.Client().Do(request)
			if err != nil {
				errorsFound <- err
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(response.Body)
				errorsFound <- fmt.Errorf("concurrent create %d status=%d body=%s", index, response.StatusCode, body)
			}
		}()
	}
	writers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	records, outbox := recipeCounts(t, application)
	if records != expected.ExpectedRecords || outbox != expected.ExpectedOutbox {
		t.Fatalf("concurrent storage records=%d outbox=%d", records, outbox)
	}
}

// TestRecipeWorkerProcess is invoked as a separate OS process by the restart
// test. It is inert during the ordinary package test run.
func TestRecipeWorkerProcess(t *testing.T) {
	path := os.Getenv("SHOP_RECIPE_CHILD_DB")
	if path == "" {
		t.Skip("child process mode is unset")
	}
	application, closeDatabase := openRecipe(t, path)
	defer closeDatabase()
	processed, err := application.ProcessOne(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
}

func TestSSESlowReaderOverTCPIsDisconnected(t *testing.T) {
	application, closeDatabase := openRecipe(t, filepath.Join(t.TempDir(), "stream.db"))
	defer closeDatabase()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	startRequest, err := http.NewRequest(http.MethodPost, server.URL+"/jobs/stream", strings.NewReader(`{"requestKey":"slow-reader-job","recordId":"slow-reader-record","value":"waiting"}`))
	if err != nil {
		t.Fatal(err)
	}
	startRequest.Header.Set("Authorization", "Bearer "+aliceToken)
	startRequest.Header.Set("Idempotency-Key", "slow-reader-job")
	startRequest.Header.Set("Content-Type", "application/json")
	start, err := server.Client().Do(startRequest)
	if err != nil {
		t.Fatal(err)
	}
	if start.StatusCode != http.StatusAccepted {
		t.Fatalf("SSE start status=%d body=%s", start.StatusCode, responseBody(t, start))
	}
	var startResult struct {
		Stream string `json:"stream"`
	}
	if err := json.NewDecoder(start.Body).Decode(&startResult); err != nil {
		t.Fatal(err)
	}
	_ = start.Body.Close()
	streamRequest, err := http.NewRequest(http.MethodGet, server.URL+"/jobs/stream/"+startResult.Stream, nil)
	if err != nil {
		t.Fatal(err)
	}
	streamRequest.Header.Set("Authorization", "Bearer "+aliceToken)
	streamResponse, err := server.Client().Do(streamRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer streamResponse.Body.Close()
	if streamResponse.StatusCode != http.StatusOK {
		t.Fatalf("subscribe status=%d", streamResponse.StatusCode)
	}
	// Consume only the initial reconnect frame, then deliberately stop reading.
	buffer := make([]byte, 32)
	if _, err := streamResponse.Body.Read(buffer); err != nil {
		t.Fatal(err)
	}
	slowExpected := fixture(t, "slow-subscriber")
	if slowExpected.MaxSubscriberQueue != 1 {
		t.Fatalf("fixture max subscriber queue=%d", slowExpected.MaxSubscriberQueue)
	}
	payload := strings.Repeat("x", 4096)
	disconnected := false
	for i := range 8192 {
		_, _ = application.Hub.Publish(startResult.Stream, sse.Event{Type: "progress", Data: []byte(fmt.Sprintf(`{"n":%d,"p":"%s"}`, i, payload))})
		if application.Hub.Stats().SlowSubscribers > 0 {
			disconnected = true
			break
		}
	}
	if !disconnected {
		t.Fatalf("slow client was not disconnected; stats=%+v", application.Hub.Stats())
	}
	select {
	case reason := <-application.streamClosures:
		if reason != slowExpected.ExpectedClose {
			t.Fatalf("slow subscriber close reason=%q, want %q", reason, slowExpected.ExpectedClose)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow subscriber handler did not close")
	}
}

func recipeCounts(t testing.TB, application *Application) (records, outbox int) {
	t.Helper()
	if err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT COUNT(*) FROM shop_records`).Scan(&records); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT COUNT(*) FROM shop_outbox`).Scan(&outbox)
	}); err != nil {
		t.Fatal(err)
	}
	return records, outbox
}

func jobCount(t testing.TB, application *Application, requestKey string) int {
	t.Helper()
	var count int
	if err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM worker_jobs WHERE request_key = ?`, requestKey).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func request(t *testing.T, client *http.Client, method, target, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func sendWebhook(t *testing.T, client *http.Client, baseURL, eventID string, body []byte) *http.Response {
	t.Helper()
	return sendWebhookWithSignature(t, client, baseURL, eventID, body, body)
}

func sendWebhookWithSignature(t *testing.T, client *http.Client, baseURL, eventID string, signedBody, transmittedBody []byte) *http.Response {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	secret, err := webhook.NewSecret([]byte(webhookKey))
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/orders", strings.NewReader(string(transmittedBody)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("webhook-id", eventID)
	request.Header.Set("webhook-timestamp", fmt.Sprint(now.Unix()))
	request.Header.Set("webhook-signature", webhook.SignStandard(secret, eventID, now, signedBody))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func responseBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

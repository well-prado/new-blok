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
	application, err := New(context.Background(), Config{
		Database:   database,
		Tokens:     map[string]string{"alice": aliceToken, "bob": bobToken},
		WebhookKey: []byte(webhookKey),
	})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	return application, func() { _ = database.Close() }
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if version != 2 || migrationsApplied != 2 {
		t.Fatalf("version=%d migration rows=%d, want 2 and 2", version, migrationsApplied)
	}
	if _, err := worker.New(context.Background(), database, nil); err != nil {
		t.Fatal(err)
	}
	if err := Teardown(context.Background(), database); err != nil {
		t.Fatal(err)
	}
	var queueTables int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'worker_jobs'`).Scan(&queueTables)
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
	})
	if err == nil || !strings.Contains(err.Error(), "tokens must be distinct") {
		t.Fatalf("New with duplicate caller tokens error=%v", err)
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
	application, closeDatabase := openRecipe(t, filepath.Join(t.TempDir(), "outbox.db"))
	defer closeDatabase()
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"outbox-1","value":"retry"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.StatusCode, responseBody(t, created))
	}
	_ = created.Body.Close()
	expected := fixture(t, "outbox-retry")
	var IDs []string
	processed, err := application.DrainOutbox(context.Background(), func(_ context.Context, id string, _ []byte) error {
		IDs = append(IDs, id)
		return errors.New("synthetic sink unavailable")
	})
	if !processed || err == nil {
		t.Fatalf("failed publish processed=%v err=%v", processed, err)
	}
	processed, err = application.DrainOutbox(context.Background(), func(_ context.Context, id string, _ []byte) error {
		IDs = append(IDs, id)
		return nil
	})
	if !processed || err != nil {
		t.Fatalf("retry publish processed=%v err=%v", processed, err)
	}
	if len(IDs) != expected.ExpectedPublishAttempts || len(IDs) != 2 || expected.ExpectedStableEventID && IDs[0] != IDs[1] {
		t.Fatalf("published IDs=%v", IDs)
	}
	processed, err = application.DrainOutbox(context.Background(), func(context.Context, string, []byte) error {
		t.Fatal("sent event was selected again")
		return nil
	})
	if processed || err != nil {
		t.Fatalf("post-success drain processed=%v err=%v", processed, err)
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

func TestSSESlowReaderIsDisconnectedAtConfiguredQueueBound(t *testing.T) {
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

package parity

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type midExecutionExpectation struct {
	Adapter          string `json:"adapter"`
	Redelivery       string `json:"redelivery"`
	Completed        int    `json:"completedByFreshConsumer"`
	Attempts         int    `json:"attempts,omitempty"`
	ProviderCalls    int    `json:"providerCalls"`
	AbortedByKill    int    `json:"abortedByKill"`
	CommittedEffects int    `json:"committedEffects"`
}

// TestDurableMidExecutionKillRedelivers kills each engine's consumer process
// while its provider call is in flight, then starts a fresh consumer against
// the same durable store. The killed call must be observed as aborted (no
// effect), the job must be redelivered exactly once more, and the business
// output and single effect must match the predeclared contract. It uses the
// same disposable loopback PostgreSQL gate as the recovery distributions.
func TestDurableMidExecutionKillRedelivers(t *testing.T) {
	if os.Getenv("BLOK_PARITY_RECOVERY") != "1" {
		t.Skip("set BLOK_PARITY_RECOVERY=1 with a disposable loopback PostgreSQL database to run mid-execution kill recovery")
	}
	requireOldEngine(t)
	postgresURL := requireLoopbackPostgres(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		MidExecutionKill *struct {
			ExpectedBody map[string]any          `json:"expectedBody"`
			Old          midExecutionExpectation `json:"old"`
			New          midExecutionExpectation `json:"new"`
		} `json:"midExecutionKill"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil || fixture.MidExecutionKill == nil || fixture.MidExecutionKill.Old.ProviderCalls == 0 || fixture.MidExecutionKill.New.ProviderCalls == 0 {
		t.Fatalf("contracts.json must predeclare midExecutionKill expectations: %v", err)
	}
	expected := fixture.MidExecutionKill
	expectedOutput := func(businessID string) json.RawMessage {
		body := map[string]any{"jobId": businessID}
		for key, value := range expected.ExpectedBody {
			body[key] = value
		}
		encoded, _ := json.Marshal(map[string]any{"status": http.StatusOK, "body": body})
		return encoded
	}
	type settledResult struct {
		Stats struct {
			Completed int `json:"completed"`
			Failed    int `json:"failed"`
			Attempts  int `json:"attempts"`
		} `json:"stats"`
		Response json.RawMessage `json:"response"`
	}
	waitAborted := func(provider *ledgerServer, operation string, want int) int {
		deadline := time.Now().Add(10 * time.Second)
		for {
			provider.ledger.mu.Lock()
			aborted := provider.ledger.aborted[operation]
			provider.ledger.mu.Unlock()
			if aborted >= want || time.Now().After(deadline) {
				return aborted
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitHeld := func(provider *ledgerServer, consumer *persistentAppProcess, name string) {
		select {
		case <-provider.ledger.holdEntered:
		case <-time.After(60 * time.Second):
			t.Fatalf("%s consumer never started the held provider call; stderr=%s", name, consumer.stderr.String())
		}
	}
	report := map[string]any{"evidenceClass": "mid-execution-process-kill-redelivery", "provenance": captureProvenance(t), "postgresContainer": inspectPostgresContainer(t, postgresURL)}

	// Published Blok 2.5.0: PgBossAdapter + WorkerTrigger + Runner.
	{
		provider := newProvider(t)
		businessID := fmt.Sprintf("kill-business-old-%d", os.Getpid())
		key := fmt.Sprintf("kill-old-%d", os.Getpid())
		provider.ledger.mu.Lock()
		provider.ledger.holdKeys[key] = true
		provider.ledger.mu.Unlock()
		schema := fmt.Sprintf("parity108_kill_%d_%d", os.Getpid(), time.Now().UnixNano())
		queue := fmt.Sprintf("parity108_kill_%d", os.Getpid())
		payload := map[string]any{"requestKey": key, "jobId": businessID, "sku": "coffee", "quantity": 2}
		producer := startOldWorkerProcess(t, map[string]any{"mode": "durable-worker-produce", "schema": schema, "queue": queue, "payload": payload, "duplicate": false, "expireSeconds": 5}, provider.URL, postgresURL)
		accepted, err := waitPersistentLine(producer.lines, "ACCEPTED ", 45*time.Second)
		if err != nil {
			t.Fatalf("old producer: %v; stderr=%s", err, producer.stderr.String())
		}
		var submitted struct {
			StoredJobs int `json:"storedJobs"`
		}
		if err := json.Unmarshal([]byte(accepted), &submitted); err != nil || submitted.StoredJobs != 1 {
			t.Fatalf("old producer accepted=%q err=%v; want one stored job", accepted, err)
		}
		producer.stop()
		consumerInput := map[string]any{"mode": "durable-worker-consume", "schema": schema, "queue": queue, "operation": "job-hold", "deadlineSeconds": 200}
		first := startOldWorkerProcess(t, consumerInput, provider.URL, postgresURL)
		waitHeld(provider, first, "old")
		killed := time.Now()
		first.stop() // SIGKILL while the provider call is in flight
		aborted := waitAborted(provider, "job-hold", expected.Old.AbortedByKill)
		second := startOldWorkerProcess(t, consumerInput, provider.URL, postgresURL)
		if _, err := waitPersistentLine(second.lines, "RECOVERED ", 200*time.Second); err != nil {
			t.Fatalf("old job was not redelivered after mid-execution kill: %v; stderr=%s", err, second.stderr.String())
		}
		recoveredAfter := time.Since(killed)
		settledLine, err := waitPersistentLine(second.lines, "SETTLED ", 15*time.Second)
		if err != nil {
			t.Fatalf("old redelivery did not settle: %v; stderr=%s", err, second.stderr.String())
		}
		second.stop()
		var settled settledResult
		if err := json.Unmarshal([]byte(settledLine), &settled); err != nil {
			t.Fatal(err)
		}
		calls, effects := provider.ledger.snapshot()
		if settled.Stats.Completed != expected.Old.Completed || !equalJSON(settled.Response, expectedOutput(businessID)) || calls != expected.Old.ProviderCalls || aborted != expected.Old.AbortedByKill || effects != expected.Old.CommittedEffects {
			t.Fatalf("old mid-execution kill: settled=%s calls=%d aborted=%d effects=%d; want completed=%d output=%s calls=%d aborted=%d effects=%d", settledLine, calls, aborted, effects, expected.Old.Completed, expectedOutput(businessID), expected.Old.ProviderCalls, expected.Old.AbortedByKill, expected.Old.CommittedEffects)
		}
		report["old"] = map[string]any{"expected": expected.Old, "settled": json.RawMessage(settledLine), "providerCalls": calls, "abortedByKill": aborted, "committedEffects": effects, "killToRedeliveredCompletionNs": recoveredAfter.Nanoseconds()}
	}

	// Native engine: trigger/worker.Queue on SQLite.
	{
		provider := newProvider(t)
		businessID := fmt.Sprintf("kill-business-new-%d", os.Getpid())
		key := fmt.Sprintf("kill-new-%d", os.Getpid())
		provider.ledger.mu.Lock()
		provider.ledger.holdKeys[key] = true
		provider.ledger.mu.Unlock()
		databasePath := filepath.Join(t.TempDir(), "worker.db")
		producer := startNativeWorkerProcess(t, "produce", databasePath, provider.URL, key, businessID)
		if _, err := waitPersistentLine(producer.lines, "ACCEPTED ", 30*time.Second); err != nil {
			t.Fatalf("native producer: %v; stderr=%s", err, producer.stderr.String())
		}
		producer.stop()
		first := startNativeWorkerProcess(t, "consume", databasePath, provider.URL, key, businessID, "BLOK_PARITY_OPERATION=job-hold")
		waitHeld(provider, first, "native")
		killed := time.Now()
		first.stop() // SIGKILL while the provider call is in flight
		aborted := waitAborted(provider, "job-hold", expected.New.AbortedByKill)
		second := startNativeWorkerProcess(t, "consume", databasePath, provider.URL, key, businessID, "BLOK_PARITY_OPERATION=job-hold")
		if _, err := waitPersistentLine(second.lines, "RECOVERED ", 60*time.Second); err != nil {
			t.Fatalf("native job was not redelivered after mid-execution kill: %v; stderr=%s", err, second.stderr.String())
		}
		recoveredAfter := time.Since(killed)
		settledLine, err := waitPersistentLine(second.lines, "SETTLED ", 15*time.Second)
		if err != nil {
			t.Fatalf("native redelivery did not settle: %v; stderr=%s", err, second.stderr.String())
		}
		second.stop()
		var settled settledResult
		if err := json.Unmarshal([]byte(settledLine), &settled); err != nil {
			t.Fatal(err)
		}
		calls, effects := provider.ledger.snapshot()
		// expected.New.Attempts (1) pins today's behaviour: the killed attempt's
		// counter rolls back with its claim transaction. #245 questions whether
		// that should consume MaxAttempts; change the contract with its decision.
		if settled.Stats.Completed != expected.New.Completed || settled.Stats.Attempts != expected.New.Attempts || !equalJSON(settled.Response, expectedOutput(businessID)) || calls != expected.New.ProviderCalls || aborted != expected.New.AbortedByKill || effects != expected.New.CommittedEffects {
			t.Fatalf("native mid-execution kill: settled=%s calls=%d aborted=%d effects=%d; want completed=%d attempts=%d output=%s calls=%d aborted=%d effects=%d", settledLine, calls, aborted, effects, expected.New.Completed, expected.New.Attempts, expectedOutput(businessID), expected.New.ProviderCalls, expected.New.AbortedByKill, expected.New.CommittedEffects)
		}
		report["new"] = map[string]any{"expected": expected.New, "settled": json.RawMessage(settledLine), "providerCalls": calls, "abortedByKill": aborted, "committedEffects": effects, "killToRedeliveredCompletionNs": recoveredAfter.Nanoseconds()}
	}
	report["limitations"] = []string{"one sample per engine; the redelivery delay is set by each queue's orphan policy (pg-boss expiry plus its 120 s maintenance interval vs. SQLite claim-transaction rollback), not by engine speed", "PostgreSQL and SQLite are different storage backends"}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	writeRawEvidence(t, "mid-execution-kill", encoded)
	t.Logf("E20-T01 mid-execution kill raw=%s", encoded)
}

package parity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	persistentStartupSamples      = 5
	persistentIdleSamples         = 5
	persistentIdleWindow          = time.Second
	persistentLoadWorkers         = 4
	persistentLoadWindow          = 10 * time.Second
	persistentLoadResourceSamples = 10
	persistentRecoveryRuns        = 5
)

type numericDistribution struct {
	Samples []float64 `json:"samples"`
	Min     float64   `json:"min"`
	P50     float64   `json:"p50"`
	P95     float64   `json:"p95"`
	Max     float64   `json:"max"`
}

type processResourceSample struct {
	WindowNS      int64   `json:"windowNs"`
	CPUTimeDelta  int64   `json:"cpuTimeDeltaNs"`
	CPUPercent    float64 `json:"cpuPercent"`
	ResidentBytes int64   `json:"residentBytes"`
}

func TestPersistentApplicationDistributions(t *testing.T) {
	if os.Getenv("BLOK_PARITY_PERF") != "1" {
		t.Skip("set BLOK_PARITY_PERF=1 to run persistent application measurements")
	}
	quote := findWorkload(t, "quote-success")
	provider := newProvider(t)
	client := &http.Client{Timeout: 30 * time.Second}
	nodeVersion, err := exec.Command("node", "--version").Output()
	if err != nil {
		t.Fatalf("read pinned old runtime version: %v", err)
	}
	sourceRevision, err := repositoryRevision()
	if err != nil {
		t.Fatalf("read new-framework source revision: %v", err)
	}
	var oldStartup, newStartup []int64
	for sample := 0; sample < persistentStartupSamples; sample++ {
		old, duration := startOldPersistentApp(t, provider.URL)
		oldStartup = append(oldStartup, duration.Nanoseconds())
		assertPersistentHealth(t, client, old.url)
		old.stop()

		current, duration := startNewPersistentApp(t, provider.URL)
		newStartup = append(newStartup, duration.Nanoseconds())
		assertPersistentHealth(t, client, current.url)
		current.stop()
	}

	oldIdle, _ := startOldPersistentApp(t, provider.URL)
	newIdle, _ := startNewPersistentApp(t, provider.URL)
	oldResources := sampleProcessResources(t, oldIdle.command.Process.Pid, persistentIdleSamples, persistentIdleWindow)
	newResources := sampleProcessResources(t, newIdle.command.Process.Pid, persistentIdleSamples, persistentIdleWindow)
	oldIdle.stop()
	newIdle.stop()

	oldLoad, _ := startOldPersistentApp(t, provider.URL)
	newLoad, _ := startNewPersistentApp(t, provider.URL)
	warmup := copyRequest(quote.Request, "persistent-warmup")
	oldWarmup := assertPersistentQuote(t, client, oldLoad.url, warmup)
	newWarmup := assertPersistentQuote(t, client, newLoad.url, warmup)
	if !equalJSON(oldWarmup, newWarmup) {
		t.Fatalf("persistent warmup business output differs: old=%s new=%s", oldWarmup, newWarmup)
	}
	oldLoadResult := runPersistentLoad(t, client, oldLoad.url, oldLoad.command.Process.Pid, quote.Request, "persistent-old-load", persistentLoadWorkers, persistentLoadWindow)
	newLoadResult := runPersistentLoad(t, client, newLoad.url, newLoad.command.Process.Pid, quote.Request, "persistent-new-load", persistentLoadWorkers, persistentLoadWindow)
	oldLoad.stop()
	newLoad.stop()

	calls, effects := provider.ledger.snapshot()
	loadRequests := 0
	for worker := range persistentLoadWorkers {
		loadRequests += oldLoadResult.PerWorker[worker] + newLoadResult.PerWorker[worker]
	}
	wantCalls := 2 + len(oldLoadResult.Outputs) + len(newLoadResult.Outputs)
	wantEffects := 1 + loadRequests
	if calls != wantCalls || effects != wantEffects {
		t.Fatalf("persistent app provider calls=%d effects=%d, want calls=%d effects=%d after one intentionally shared warmup key and disjoint old/new load keys", calls, effects, wantCalls, wantEffects)
	}
	oldIdleCPU, newIdleCPU := cpuPercentSamples(oldResources), cpuPercentSamples(newResources)
	oldIdleRSS, newIdleRSS := rssSamples(oldResources), rssSamples(newResources)
	oldLoadRate := float64(len(oldLoadResult.Outputs)) / persistentLoadWindow.Seconds()
	newLoadRate := float64(len(newLoadResult.Outputs)) / persistentLoadWindow.Seconds()
	encoded, err := json.Marshal(map[string]any{
		"evidenceClass":   "persistent-synthetic-application-harness",
		"toolVersions":    map[string]string{"node": strings.TrimSpace(string(nodeVersion)), "go": runtime.Version()},
		"sourceRevisions": map[string]string{"newBlok": sourceRevision, "oldBlok": "7611e434f716a5a8efbed26613e546893d5bcba7"},
		"host":            map[string]any{"os": runtime.GOOS, "arch": runtime.GOARCH, "logicalCPUs": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)},
		"old":             map[string]any{"framework": "published Blok 2.5.0", "process": "one long-lived Node process; one resolved Configuration reused across HTTP requests", "startupNs": distribution("process-start-to-ready", "published Blok 2.5.0", oldStartup), "idleCpuPercent": summarizeFloat(oldIdleCPU), "idleResidentBytes": summarizeFloat(oldIdleRSS), "idleSamples": oldResources, "loadCpuPercent": summarizeFloat(cpuPercentSamples(oldLoadResult.Resources)), "loadResidentBytes": summarizeFloat(rssSamples(oldLoadResult.Resources)), "loadResourceSamples": oldLoadResult.Resources, "loadRequestNs": distribution("persistent-http-quote", "published Blok 2.5.0", oldLoadResult.Latencies), "loadRequests": len(oldLoadResult.Outputs), "loadRequestsPerSecond": oldLoadRate, "requestsPerWorker": oldLoadResult.PerWorker},
		"new":             map[string]any{"framework": "new native Go engine", "process": "one long-lived Go test-helper process; Engine and lowered program reused across HTTP requests", "startupNs": distribution("process-start-to-ready", "new native Go engine", newStartup), "idleCpuPercent": summarizeFloat(newIdleCPU), "idleResidentBytes": summarizeFloat(newIdleRSS), "idleSamples": newResources, "loadCpuPercent": summarizeFloat(cpuPercentSamples(newLoadResult.Resources)), "loadResidentBytes": summarizeFloat(rssSamples(newLoadResult.Resources)), "loadResourceSamples": newLoadResult.Resources, "loadRequestNs": distribution("persistent-http-quote", "new native Go engine", newLoadResult.Latencies), "loadRequests": len(newLoadResult.Outputs), "loadRequestsPerSecond": newLoadRate, "requestsPerWorker": newLoadResult.PerWorker},
		"idleWindowNs":    persistentIdleWindow.Nanoseconds(), "idleSamples": persistentIdleSamples,
		"loadConcurrency": persistentLoadWorkers, "loadWindowNs": persistentLoadWindow.Nanoseconds(), "loadResourceSampleWindowNs": persistentIdleWindow.Nanoseconds(), "loadResourceSampleCount": persistentLoadResourceSamples,
		"resourceMeasurement": map[string]string{"tool": "ps -o rss= -o cputime=", "cpuPrecision": "cputime is reported in whole seconds on this macOS host; 1-second sample windows are quantized and may report zero or coarse CPU deltas"},
		"loadMeasurement":     map[string]string{"executionOrder": "old process then new process; windows are sequential, not interleaved or counterbalanced", "providerKeys": "disjoint old/new prefixes; same schema and provider endpoint; each load request incurs one fresh provider effect", "interpretation": "raw descriptive distributions only; sequential load is not a controlled comparative performance claim"},
		"limitations":         []string{"one local host", "shared loopback synthetic provider", "old/new sustained-load windows are sequential", "ps cputime granularity is one second on this host"},
		"providerCalls":       calls, "committedEffects": effects,
		"recovery": map[string]string{"status": "separate-required-gate", "command": "BLOK_PARITY_RECOVERY=1 BLOK_PARITY_POSTGRES_URL=postgres://... go test ./benchmarks/parity -run '^TestPersistentDurableRecoveryDistributions$'"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("E20-T01 persistent application raw distributions provider=%s old_pid_and_new_pid_separate=true raw=%s", provider.URL, encoded)
}

func TestPersistentDurableRecoveryDistributions(t *testing.T) {
	if os.Getenv("BLOK_PARITY_RECOVERY") != "1" {
		t.Skip("set BLOK_PARITY_RECOVERY=1 with a disposable loopback PostgreSQL database to run durable restart recovery")
	}
	postgresURL := requireLoopbackPostgres(t)
	postgresImage := os.Getenv("BLOK_PARITY_POSTGRES_IMAGE")
	postgresImageDigest := os.Getenv("BLOK_PARITY_POSTGRES_IMAGE_DIGEST")
	digestHex := strings.TrimPrefix(postgresImageDigest, "postgres@sha256:")
	if postgresImage == "" || len(digestHex) != 64 || strings.Trim(digestHex, "0123456789abcdef") != "" {
		t.Fatal("set BLOK_PARITY_POSTGRES_IMAGE and BLOK_PARITY_POSTGRES_IMAGE_DIGEST to record the disposable server image identity")
	}
	provider := newProvider(t)
	sourceRevision, err := repositoryRevision()
	if err != nil {
		t.Fatalf("read new-framework source revision: %v", err)
	}
	schema := fmt.Sprintf("parity108_%d_%d", os.Getpid(), time.Now().UnixNano())
	oldRecovery := make([]int64, 0, persistentRecoveryRuns)
	newRecovery := make([]int64, 0, persistentRecoveryRuns)
	postgresVersion := ""
	sqliteVersion := ""
	for sample := 0; sample < persistentRecoveryRuns; sample++ {
		businessID := fmt.Sprintf("persistent-recovery-business-%d-%02d", os.Getpid(), sample)
		requestKey := fmt.Sprintf("persistent-old-recovery-%d-%02d", os.Getpid(), sample)
		queue := fmt.Sprintf("parity108_old_%d_%02d", os.Getpid(), sample)
		payload := map[string]any{"requestKey": requestKey, "jobId": businessID, "sku": "coffee", "quantity": 2}
		producer := startOldWorkerProcess(t, map[string]any{"mode": "durable-worker-produce", "schema": schema, "queue": queue, "payload": payload}, provider.URL, postgresURL)
		accepted, err := waitPersistentLine(producer.lines, "ACCEPTED ", 45*time.Second)
		if err != nil {
			producer.stop()
			t.Fatalf("old durable producer did not commit accepted job: %v; stderr=%s", err, producer.stderr.String())
		}
		var submitted struct {
			JobID           string `json:"jobId"`
			DuplicateJobID  string `json:"duplicateJobId"`
			PostgresVersion string `json:"postgresVersion"`
		}
		if err := json.Unmarshal([]byte(accepted), &submitted); err != nil || submitted.JobID == "" || submitted.DuplicateJobID == "" || submitted.PostgresVersion == "" || submitted.PostgresVersion == "unknown" {
			producer.stop()
			t.Fatalf("old adapter acceptance/duplicate submission result=%q err=%v", accepted, err)
		}
		if postgresVersion != "" && submitted.PostgresVersion != postgresVersion {
			t.Fatalf("PostgreSQL server version changed within recovery run: first=%q current=%q", postgresVersion, submitted.PostgresVersion)
		}
		postgresVersion = submitted.PostgresVersion
		producer.stop() // force-kill after pg-boss send committed, before any consumer starts
		recoveryStarted := time.Now()
		consumer := startOldWorkerProcess(t, map[string]any{"mode": "durable-worker-consume", "schema": schema, "queue": queue}, provider.URL, postgresURL)
		if _, err := waitPersistentLine(consumer.lines, "READY ", 45*time.Second); err != nil {
			consumer.stop()
			t.Fatalf("old durable consumer did not restart: %v; stderr=%s", err, consumer.stderr.String())
		}
		settled, err := waitPersistentLine(consumer.lines, "RECOVERED ", 60*time.Second)
		if err != nil {
			consumer.stop()
			t.Fatalf("old durable job was not redelivered after process kill: %v; stderr=%s", err, consumer.stderr.String())
		}
		oldRecovery = append(oldRecovery, time.Since(recoveryStarted).Nanoseconds())
		settled, err = waitPersistentLine(consumer.lines, "SETTLED ", 10*time.Second)
		if err != nil {
			consumer.stop()
			t.Fatalf("old durable delivery did not reach stable queue state: %v; stderr=%s", err, consumer.stderr.String())
		}
		var oldResult struct {
			Stats struct {
				Completed int `json:"completed"`
				Failed    int `json:"failed"`
			} `json:"stats"`
			Response json.RawMessage `json:"response"`
		}
		if err := json.Unmarshal([]byte(settled), &oldResult); err != nil {
			t.Fatalf("decode old durable result %q: %v", settled, err)
		}
		if oldResult.Stats.Completed != 1 || oldResult.Stats.Failed != 1 {
			t.Fatalf("old durable retry statistics=%+v, want one failed attempt and one completion", oldResult.Stats)
		}
		expectedOutput, err := json.Marshal(map[string]any{"status": http.StatusOK, "body": map[string]any{"jobId": businessID, "state": "completed", "totalCents": 3000}})
		if err != nil {
			t.Fatal(err)
		}
		if !equalJSON(expectedOutput, oldResult.Response) {
			t.Fatalf("old durable output=%s, want predeclared %s", oldResult.Response, expectedOutput)
		}
		consumer.stop()

		newKey := fmt.Sprintf("persistent-new-recovery-%d-%02d", os.Getpid(), sample)
		databasePath := filepath.Join(t.TempDir(), "worker.db")
		newProducer := startNativeWorkerProcess(t, "produce", databasePath, provider.URL, newKey, businessID)
		accepted, err = waitPersistentLine(newProducer.lines, "ACCEPTED ", 30*time.Second)
		if err != nil {
			newProducer.stop()
			t.Fatalf("native durable producer did not commit accepted job: %v; stderr=%s", err, newProducer.stderr.String())
		}
		fields := strings.Fields(accepted)
		if len(fields) != 4 || fields[1] != "accepted=true" || fields[2] != "duplicateAccepted=false" || !strings.HasPrefix(fields[3], "sqliteVersion=") {
			newProducer.stop()
			t.Fatalf("native queue acceptance/idempotency result=%q", accepted)
		}
		currentSQLiteVersion := strings.TrimPrefix(fields[3], "sqliteVersion=")
		if currentSQLiteVersion == "" || (sqliteVersion != "" && currentSQLiteVersion != sqliteVersion) {
			newProducer.stop()
			t.Fatalf("SQLite backend version changed or is missing: first=%q current=%q", sqliteVersion, currentSQLiteVersion)
		}
		sqliteVersion = currentSQLiteVersion
		newProducer.stop() // kill the accepting application after SQLite commit
		recoveryStarted = time.Now()
		newConsumer := startNativeWorkerProcess(t, "consume", databasePath, provider.URL, newKey, businessID)
		if _, err := waitPersistentLine(newConsumer.lines, "READY ", 30*time.Second); err != nil {
			newConsumer.stop()
			t.Fatalf("native durable consumer did not reopen the journal: %v; stderr=%s", err, newConsumer.stderr.String())
		}
		newSettled, err := waitPersistentLine(newConsumer.lines, "RECOVERED ", 60*time.Second)
		if err != nil {
			newConsumer.stop()
			t.Fatalf("native durable job was not redelivered after process kill: %v; stderr=%s", err, newConsumer.stderr.String())
		}
		newRecovery = append(newRecovery, time.Since(recoveryStarted).Nanoseconds())
		newSettled, err = waitPersistentLine(newConsumer.lines, "SETTLED ", 10*time.Second)
		if err != nil {
			newConsumer.stop()
			t.Fatalf("native durable delivery did not reach stable queue state: %v; stderr=%s", err, newConsumer.stderr.String())
		}
		var newResult struct {
			Stats struct {
				Completed int `json:"completed"`
				Failed    int `json:"failed"`
				Attempts  int `json:"attempts"`
			} `json:"stats"`
			Response json.RawMessage `json:"response"`
		}
		if err := json.Unmarshal([]byte(newSettled), &newResult); err != nil {
			t.Fatalf("decode native durable result %q: %v", newSettled, err)
		}
		if newResult.Stats.Completed != 1 || newResult.Stats.Failed != 1 || newResult.Stats.Attempts != 2 {
			t.Fatalf("native durable retry statistics=%+v, want one failed attempt and one completion", newResult.Stats)
		}
		expectedNewOutput, err := json.Marshal(map[string]any{"status": http.StatusOK, "body": map[string]any{"jobId": businessID, "state": "completed", "totalCents": 3000}})
		if err != nil {
			t.Fatal(err)
		}
		if !equalJSON(expectedNewOutput, newResult.Response) {
			t.Fatalf("native durable output=%s, want predeclared %s", newResult.Response, expectedNewOutput)
		}
		if !equalJSON(oldResult.Response, newResult.Response) {
			t.Fatalf("recovered worker outputs differ: old=%s new=%s", oldResult.Response, newResult.Response)
		}
		consumer.stop()
	}
	calls, effects := provider.ledger.snapshot()
	wantCalls, wantEffects := 4*persistentRecoveryRuns, 2*persistentRecoveryRuns
	if calls != wantCalls || effects != wantEffects {
		t.Fatalf("durable recovery provider calls=%d committed effects=%d, want %d calls and %d effects", calls, effects, wantCalls, wantEffects)
	}
	encoded, err := json.Marshal(map[string]any{
		"evidenceClass":   "persistent-durable-worker-recovery",
		"toolVersions":    map[string]string{"node": nodeVersion(t), "go": runtime.Version(), "pgBoss": "10.4.2", "pg": "8.23.1"},
		"sourceRevisions": map[string]string{"newBlok": sourceRevision, "oldBlok": "7611e434f716a5a8efbed26613e546893d5bcba7"},
		"host":            map[string]any{"os": runtime.GOOS, "arch": runtime.GOARCH, "logicalCPUs": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)},
		"old":             map[string]any{"framework": "published @blokjs packages 2.5.0", "adapter": "PgBossAdapter", "broker": "pg-boss 10.4.2 / PostgreSQL", "process": "producer force-killed after durable send; fresh WorkerTrigger consumer and published Runner recover job", "delivery": "two addJob calls with same jobId; pg-boss singleton-key behavior is checked by exactly-one completion; retryLimit=2; synthetic provider fails first call"},
		"new":             map[string]any{"framework": "native engine", "adapter": "trigger/worker.Queue", "broker": "SQLite WAL synchronous=FULL", "process": "producer force-killed after Enqueue commit; fresh process reopens SQLite and executes native Engine", "delivery": "two Enqueue calls with same request key yield accepted=true then accepted=false; MaxAttempts=3; synthetic provider fails first call"},
		"providerCalls":   calls, "committedEffects": effects,
		"backendVersions": map[string]string{"postgresql": postgresVersion, "postgresImage": postgresImage, "postgresImageDigest": postgresImageDigest, "sqlite": sqliteVersion + " (WAL/synchronous=FULL)"},
		"oldRecoveryNs":   distribution("kill-to-first-completion", "published Blok 2.5.0 + pg-boss 10.4.2", oldRecovery),
		"newRecoveryNs":   distribution("kill-to-first-completion", "native engine + SQLite WAL/FULL", newRecovery),
		"samples":         persistentRecoveryRuns,
		"limitations":     []string{"local single-host durability only", "PostgreSQL and SQLite are different storage backends", "not evidence of disk-loss or multi-host recovery"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("E20-T01 matched durable process-restart recovery raw=%s", encoded)
}

func nodeVersion(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("node", "--version").Output()
	if err != nil {
		t.Fatalf("read pinned old runtime version: %v", err)
	}
	return strings.TrimSpace(string(output))
}

func repositoryRevision() (string, error) {
	output, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func requireLoopbackPostgres(t *testing.T) string {
	t.Helper()
	value := os.Getenv("BLOK_PARITY_POSTGRES_URL")
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" {
		t.Fatal("BLOK_PARITY_POSTGRES_URL must be an explicit PostgreSQL URL for a disposable local test database")
	}
	host := parsed.Hostname()
	address := net.ParseIP(host)
	if host != "localhost" && (address == nil || !address.IsLoopback()) {
		t.Fatalf("refusing non-loopback PostgreSQL endpoint %q; recovery gate is synthetic/local only", host)
	}
	for _, configuredHost := range parsed.Query()["host"] {
		configuredIP := net.ParseIP(configuredHost)
		if configuredHost != "localhost" && (configuredIP == nil || !configuredIP.IsLoopback()) {
			t.Fatalf("refusing non-loopback PostgreSQL host override %q", configuredHost)
		}
	}
	databaseName := strings.TrimPrefix(parsed.Path, "/")
	if !strings.HasPrefix(databaseName, "parity108_test") {
		t.Fatalf("refusing PostgreSQL database %q; use a disposable database whose name begins parity108_test", databaseName)
	}
	return value
}

func assertPersistentHealth(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	response, err := client.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("persistent application health: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("persistent application health status=%d body=%s", response.StatusCode, body)
	}
}

func assertPersistentQuote(t *testing.T, client *http.Client, baseURL string, request map[string]any) json.RawMessage {
	t.Helper()
	actual, err := persistentQuote(client, baseURL, request)
	if err != nil {
		t.Fatalf("persistent quote request: %v", err)
	}
	var got struct {
		OK       bool            `json:"ok"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(actual, &got); err != nil || !got.OK {
		t.Fatalf("persistent quote failed: body=%s err=%v", actual, err)
	}
	return actual
}

func persistentQuote(client *http.Client, baseURL string, request map[string]any) (json.RawMessage, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	response, err := client.Post(baseURL+"/quote", "application/json", bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", response.StatusCode, body)
	}
	return body, nil
}

type persistentLoadResult struct {
	Latencies []int64
	Outputs   map[string]json.RawMessage
	PerWorker []int
	Resources []processResourceSample
}

func runPersistentLoad(t *testing.T, client *http.Client, baseURL string, pid int, request map[string]any, keyPrefix string, workers int, window time.Duration) persistentLoadResult {
	t.Helper()
	type observation struct {
		worker  int
		key     string
		latency int64
		body    json.RawMessage
		err     error
	}
	deadline := time.Now().Add(window)
	observations := make(chan observation, workers*2)
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for sequence := 0; time.Now().Before(deadline); sequence++ {
				key := fmt.Sprintf("%s-%02d-%05d", keyPrefix, worker, sequence)
				input := copyRequest(request, key)
				started := time.Now()
				body, err := persistentQuote(client, baseURL, input)
				if err == nil {
					var got struct {
						OK       bool            `json:"ok"`
						Response json.RawMessage `json:"response"`
					}
					err = json.Unmarshal(body, &got)
					if err == nil && !got.OK {
						err = fmt.Errorf("application response is not successful: %s", body)
					}
					if err == nil {
						quantity, _ := input["quantity"].(float64)
						expected, marshalErr := json.Marshal(map[string]any{"status": http.StatusOK, "body": map[string]any{"requestKey": key, "sku": input["sku"], "quantity": quantity, "totalCents": int(1500 * quantity)}})
						if marshalErr != nil {
							err = marshalErr
						} else if !equalJSON(expected, got.Response) {
							err = fmt.Errorf("business response %s does not match predeclared %s", got.Response, expected)
						}
					}
				}
				observations <- observation{worker: worker, key: key, latency: time.Since(started).Nanoseconds(), body: body, err: err}
				if err != nil {
					return
				}
			}
		}(worker)
	}
	go func() {
		wait.Wait()
		close(observations)
	}()
	result := persistentLoadResult{Outputs: map[string]json.RawMessage{}, PerWorker: make([]int, workers)}
	var firstErr error
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for observation := range observations {
			if observation.err != nil {
				if firstErr == nil {
					firstErr = observation.err
				}
				continue
			}
			result.Latencies = append(result.Latencies, observation.latency)
			result.Outputs[observation.key] = observation.body
			result.PerWorker[observation.worker]++
		}
	}()
	result.Resources = sampleProcessResources(t, pid, persistentLoadResourceSamples, persistentIdleWindow)
	<-collected
	if firstErr != nil {
		t.Fatalf("persistent load %s: %v", baseURL, firstErr)
	}
	return result
}

func sampleProcessResources(t *testing.T, pid int, samples int, window time.Duration) []processResourceSample {
	t.Helper()
	result := make([]processResourceSample, 0, samples)
	for range samples {
		_, beforeCPU := readProcessResources(t, pid)
		started := time.Now()
		time.Sleep(window)
		elapsed := time.Since(started)
		afterRSS, afterCPU := readProcessResources(t, pid)
		cpuDelta := afterCPU - beforeCPU
		result = append(result, processResourceSample{WindowNS: elapsed.Nanoseconds(), CPUTimeDelta: cpuDelta.Nanoseconds(), CPUPercent: 100 * float64(cpuDelta) / float64(elapsed), ResidentBytes: afterRSS})
	}
	return result
}

func readProcessResources(t *testing.T, pid int) (int64, time.Duration) {
	t.Helper()
	command := exec.Command("ps", "-o", "rss=", "-o", "cputime=", "-p", strconv.Itoa(pid))
	output, err := command.Output()
	if err != nil {
		t.Fatalf("read child process resources with ps: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 {
		t.Fatalf("unexpected ps output for pid %d: %q", pid, output)
	}
	rssKB, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		t.Fatalf("parse resident memory %q: %v", fields[0], err)
	}
	cpu, err := parsePSTime(fields[1])
	if err != nil {
		t.Fatalf("parse process CPU time %q: %v", fields[1], err)
	}
	return rssKB * 1024, cpu
}

func parsePSTime(value string) (time.Duration, error) {
	days := int64(0)
	if before, after, ok := strings.Cut(value, "-"); ok {
		parsed, err := strconv.ParseInt(before, 10, 64)
		if err != nil {
			return 0, err
		}
		days, value = parsed, after
	}
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("unsupported ps time value")
	}
	secondsPart := parts[len(parts)-1]
	seconds, err := strconv.ParseFloat(secondsPart, 64)
	if err != nil {
		return 0, err
	}
	minutes, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil {
		return 0, err
	}
	var hours int64
	if len(parts) == 3 {
		hours, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
	}
	total := float64(days*24*60*60+hours*60*60+minutes*60) + seconds
	return time.Duration(total * float64(time.Second)), nil
}

func cpuPercentSamples(samples []processResourceSample) []float64 {
	values := make([]float64, len(samples))
	for index, sample := range samples {
		values[index] = sample.CPUPercent
	}
	return values
}

func rssSamples(samples []processResourceSample) []float64 {
	values := make([]float64, len(samples))
	for index, sample := range samples {
		values[index] = float64(sample.ResidentBytes)
	}
	return values
}

func summarizeFloat(raw []float64) numericDistribution {
	values := append([]float64(nil), raw...)
	sort.Float64s(values)
	percentile := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(values)))) - 1
		return values[max(0, min(index, len(values)-1))]
	}
	return numericDistribution{Samples: append([]float64(nil), raw...), Min: values[0], P50: percentile(.50), P95: percentile(.95), Max: values[len(values)-1]}
}

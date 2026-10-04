package parity

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

const (
	startupSamples = 5
	idleSamples    = 20
	loadRounds     = 3
	loadWorkers    = 4
	recoveryRuns   = 5
)

type latencyDistribution struct {
	Name      string  `json:"name"`
	Engine    string  `json:"engine"`
	SamplesNS []int64 `json:"samplesNs"`
	MinNS     int64   `json:"minNs"`
	P50NS     int64   `json:"p50Ns"`
	P95NS     int64   `json:"p95Ns"`
	MaxNS     int64   `json:"maxNs"`
}

// This separately opt-in legacy sampler records limited, non-equivalent
// harness observations. It is retained only to reproduce the historical
// artifact; it is not the persistent-application acceptance gate. Old samples run
// the pinned Node process; new samples run the native engine in this Go test
// process. Their process topologies differ and the values are not an SLO or a
// normalized engine-speed comparison.
func TestLimitedHarnessPerformanceDistributions(t *testing.T) {
	if os.Getenv("BLOK_PARITY_LIMITED_PERF") != "1" {
		t.Skip("set BLOK_PARITY_LIMITED_PERF=1 to reproduce the historical limited harness samples")
	}
	quote := findWorkload(t, "quote-success")
	provider := newProvider(t)
	var distributions []latencyDistribution

	oldStartup := make([]int64, 0, startupSamples)
	newStartup := make([]int64, 0, startupSamples)
	for i := 0; i < startupSamples; i++ {
		oldCase := quote
		oldCase.Request = copyRequest(quote.Request, fmt.Sprintf("startup-old-%02d", i))
		started := time.Now()
		oldResult, err := runOldEngineRaw(provider.URL, oldCase)
		oldStartup = append(oldStartup, time.Since(started).Nanoseconds())
		if err != nil || !oldResult.OK {
			t.Fatalf("old startup/quote sample %d result=%+v err=%v", i, oldResult, err)
		}

		newCase := quote
		newCase.Request = copyRequest(quote.Request, fmt.Sprintf("startup-new-%02d", i))
		started = time.Now()
		newResult := runNewNativeEngine(t, provider.URL, newCase)
		newStartup = append(newStartup, time.Since(started).Nanoseconds())
		if !newResult.OK {
			t.Fatalf("new startup/quote sample %d result=%+v", i, newResult)
		}
	}
	distributions = append(distributions,
		distribution("cold-process-plus-quote", "published Blok 2.5.0 Node process + Runner", oldStartup),
		distribution("engine-construction-plus-quote", "new native Engine.Run in existing Go process", newStartup),
	)

	oldIdle := runOldIdleSamples(t, idleSamples)
	newIdle := runNewIdleSamples(t, idleSamples)
	distributions = append(distributions,
		distribution("idle-queue-inspection", "published WorkerTrigger.getQueueStats on live empty InMemoryAdapter", oldIdle),
		distribution("idle-queue-inspection", "new trigger/worker.Queue.Get missing key on live empty SQLite queue", newIdle),
	)

	oldLoad := make([]int64, 0, loadRounds*loadWorkers)
	newLoad := make([]int64, 0, loadRounds*loadWorkers)
	for round := 0; round < loadRounds; round++ {
		oldLoad = append(oldLoad, runConcurrentQuotes(t, provider.URL, quote, loadWorkers, round, "old")...)
		newLoad = append(newLoad, runConcurrentNativeQuotes(t, provider.URL, quote, loadWorkers, round)...)
	}
	distributions = append(distributions,
		distribution("concurrent-quote-request", "published Blok 2.5.0 Runner, fresh Node process per request", oldLoad),
		distribution("concurrent-quote-request", "new native engine, shared in-process Engine across goroutines", newLoad),
	)

	oldRecovery := make([]int64, 0, recoveryRuns)
	newRecovery := make([]int64, 0, recoveryRuns)
	order := findWorkload(t, "order-duplicate-delivery")
	for i := 0; i < recoveryRuns; i++ {
		started := time.Now()
		raw := runOldWorkerAdapterReset(t, map[string]any{"jobId": fmt.Sprintf("old-recovery-%02d", i), "orderId": "order-108"})
		oldRecovery = append(oldRecovery, time.Since(started).Nanoseconds())
		var old struct {
			Accepted      bool `json:"accepted"`
			BeforeRestart struct {
				Waiting int `json:"waiting"`
			} `json:"beforeRestart"`
			AfterRestart struct {
				Waiting int `json:"waiting"`
			} `json:"afterRestart"`
		}
		if err := json.Unmarshal(raw, &old); err != nil || !old.Accepted || old.BeforeRestart.Waiting != 1 || old.AfterRestart.Waiting != 0 {
			t.Fatalf("old adapter recovery sample %d raw=%s err=%v", i, raw, err)
		}

		newCase := order
		newCase.Request = copyRequest(order.Request, fmt.Sprintf("new-recovery-%02d", i))
		started = time.Now()
		if err := recoverNewWorkerJob(t, provider.URL, newCase, i); err != nil {
			t.Fatalf("new worker recovery sample %d: %v", i, err)
		}
		newRecovery = append(newRecovery, time.Since(started).Nanoseconds())
	}
	distributions = append(distributions,
		distribution("adapter-restart-recovery", "published InMemoryAdapter enqueue/disconnect/recreate (job lost)", oldRecovery),
		distribution("sqlite-worker-reopen-and-recover", "new SQLite queue reopen + real native engine + transactional worker ack", newRecovery),
	)

	calls, effects := provider.ledger.snapshot()
	if calls != startupSamples*2+loadRounds*loadWorkers*2+recoveryRuns || effects != calls {
		t.Fatalf("shared synthetic provider calls=%d effects=%d; every unique measured request should commit once", calls, effects)
	}
	encoded, err := json.Marshal(distributions)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("E20-T01 raw performance distributions topology=old fresh Node subprocess per Runner call; new in-process native engine; local httptest provider; GOMAXPROCS=%d loadConcurrency=%d rounds=%d provider_calls=%d provider_effects=%d raw=%s", runtimeGOMAXPROCS(), loadWorkers, loadRounds, calls, effects, encoded)
}

func runOldIdleSamples(t *testing.T, samples int) []int64 {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "worker-idle-samples", "samples": samples})
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("node", "run.mjs")
	command.Dir = filepath.Join("old-engine")
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("old idle worker sample failed: %v: %s", err, output)
	}
	var response struct {
		SamplesNS []int64 `json:"samplesNs"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode old idle sample %q: %v", output, err)
	}
	if len(response.SamplesNS) != samples {
		t.Fatalf("old idle samples=%d want=%d", len(response.SamplesNS), samples)
	}
	return response.SamplesNS
}

func runNewIdleSamples(t *testing.T, samples int) []int64 {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "idle.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]int64, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		_, err := queue.Get(context.Background(), fmt.Sprintf("absent-idle-probe-%02d", i))
		values = append(values, time.Since(started).Nanoseconds())
		if !errors.Is(err, worker.ErrNotFound) {
			t.Fatalf("idle queue lookup err=%v want ErrNotFound", err)
		}
	}
	return values
}

func runConcurrentQuotes(t *testing.T, providerURL string, quote workload, workers, round int, engineName string) []int64 {
	t.Helper()
	values := make([]int64, workers)
	errCh := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			caseData := quote
			caseData.Request = copyRequest(quote.Request, fmt.Sprintf("load-%s-%02d-%02d", engineName, round, i))
			<-start
			started := time.Now()
			result, err := runOldEngineRaw(providerURL, caseData)
			values[i] = time.Since(started).Nanoseconds()
			if err == nil && !result.OK {
				err = fmt.Errorf("old load run failed: %+v", result)
			}
			errCh <- err
		}(i)
	}
	close(start)
	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	return values
}

func runConcurrentNativeQuotes(t *testing.T, providerURL string, quote workload, workers, round int) []int64 {
	t.Helper()
	inputSchema, outputSchema := schemasForOperation(quote.Operation)
	providerNode, err := node.Define[map[string]any, map[string]any]("parity-load-call", "1.0.0", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		encoded, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/quote", bytes.NewReader(encoded))
		if err != nil {
			return nil, err
		}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("idempotency-key", input["requestKey"].(string))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			return nil, err
		}
		if response.StatusCode >= 400 {
			return nil, &node.DomainError{Code: "provider_error", Class: "provider", Err: errors.New("synthetic quote provider failure")}
		}
		return map[string]any{"status": response.StatusCode, "body": body}, nil
	}, node.Description("real native node calling the shared synthetic quote provider"), node.Schemas(inputSchema, outputSchema), node.Effects("network"))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := flow.Define(flow.Spec{Name: "parity-load", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[map[string]any]) flow.Ref[map[string]any] {
		return flow.Call(builder, "provider", providerNode, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := definition.Lower()
	if err != nil {
		t.Fatal(err)
	}
	runner := engine.New(map[string]node.Any{"parity-load-call": providerNode.Any()})
	values := make([]int64, workers)
	errCh := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			input := copyRequest(quote.Request, fmt.Sprintf("load-new-%02d-%02d", round, i))
			<-start
			started := time.Now()
			result, err := runner.Run(context.Background(), program, input)
			values[i] = time.Since(started).Nanoseconds()
			if err == nil && result.Output == nil {
				err = errors.New("new native load returned nil output")
			}
			errCh <- err
		}(i)
	}
	close(start)
	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	return values
}

func recoverNewWorkerJob(t *testing.T, providerURL string, tc workload, sample int) error {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "recovery.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		return err
	}
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		_ = database.Close()
		return err
	}
	payload, err := json.Marshal(map[string]any{"operation": tc.Operation, "request": tc.Request})
	if err != nil {
		_ = database.Close()
		return err
	}
	if _, err := queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: fmt.Sprintf("recovery-%02d", sample), Kind: "order.create", Payload: payload, MaxAttempts: 1}); err != nil {
		_ = database.Close()
		return err
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS recovered_effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		_ = database.Close()
		return err
	}
	if err := database.Close(); err != nil {
		return err
	}
	database, err = (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		return err
	}
	defer database.Close()
	queue, err = worker.New(context.Background(), database, nil)
	if err != nil {
		return err
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx worker.Tx, job worker.Job) error {
		var payload struct {
			Operation string         `json:"operation"`
			Request   map[string]any `json:"request"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		recovered := tc
		recovered.Operation, recovered.Request = payload.Operation, payload.Request
		result := runNewNativeEngine(t, providerURL, recovered)
		if !result.OK {
			return fmt.Errorf("recovered native execution failed: %+v", result)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO recovered_effects(request_key) VALUES (?)`, job.RequestKey)
		return err
	})
	if err != nil {
		return err
	}
	if !processed {
		return errors.New("reopened worker queue did not process the durable job")
	}
	job, err := queue.Get(context.Background(), fmt.Sprintf("recovery-%02d", sample))
	if err != nil {
		return err
	}
	if job.State != worker.StateCompleted || job.Attempt != 1 {
		return fmt.Errorf("recovered job state=%s attempt=%d", job.State, job.Attempt)
	}
	var effects int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM recovered_effects`).Scan(&effects)
	}); err != nil {
		return err
	}
	if effects != 1 {
		return fmt.Errorf("recovery committed effects=%d want=1", effects)
	}
	return nil
}

func copyRequest(source map[string]any, requestKey string) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	copy["requestKey"] = requestKey
	return copy
}

func distribution(name, engineName string, raw []int64) latencyDistribution {
	values := append([]int64(nil), raw...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) int64 {
		index := int(math.Ceil(p*float64(len(values)))) - 1
		return values[max(0, min(index, len(values)-1))]
	}
	return latencyDistribution{Name: name, Engine: engineName, SamplesNS: append([]int64(nil), raw...), MinNS: values[0], P50NS: percentile(.50), P95NS: percentile(.95), MaxNS: values[len(values)-1]}
}

func runtimeGOMAXPROCS() int { return runtime.GOMAXPROCS(0) }

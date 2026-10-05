package parity

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/compile"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

// TestNativePersistentAppProcess is the child-process entrypoint used by the
// opt-in benchmark. The engine, node registry, and program are constructed
// once, then reused by a long-lived HTTP application process.
func TestNativePersistentAppProcess(t *testing.T) {
	if os.Getenv("BLOK_PARITY_NATIVE_APP_PROCESS") != "1" {
		return
	}
	providerURL := os.Getenv("BLOK_PARITY_PROVIDER_URL")
	if providerURL == "" {
		t.Fatal("persistent native process requires the local synthetic provider")
	}
	definition := persistentProviderNode(t, providerURL)
	workflow, err := flow.Define(flow.Spec{Name: "parity-persistent-quote", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[map[string]any]) flow.Ref[map[string]any] {
		return flow.Call(builder, "provider", definition, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	reserve := persistentOperationNode(t, providerURL, "parity-persistent-reserve", "reserve")
	commit := persistentOperationNode(t, providerURL, "parity-persistent-commit", "commit")
	reserveProgram, err := compileReserveCommit(reserve, commit)
	if err != nil {
		t.Fatal(err)
	}
	businessNodes, businessQuote, businessOrder, err := businessNativePrograms(providerURL)
	if err != nil {
		t.Fatal(err)
	}
	registry := map[string]node.Any{
		definition.Descriptor().Name: definition.Any(),
		reserve.Descriptor().Name:    reserve.Any(),
		commit.Descriptor().Name:     commit.Any(),
	}
	maps.Copy(registry, businessNodes)
	runner := engine.New(registry)
	var stdout sync.Mutex
	mux := http.NewServeMux()
	// A caller that disconnects cancels request.Context(), which the engine
	// passes to every node call. The outcome line mirrors the old app's.
	mux.HandleFunc("POST /reserve-commit", func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var input map[string]any
		if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&input); err != nil {
			writePersistentJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid request"})
			return
		}
		result, err := runner.Run(request.Context(), reserveProgram, input)
		if request.Context().Err() != nil {
			message := ""
			if err != nil {
				message = err.Error()
			}
			outcome, _ := json.Marshal(map[string]any{"route": "/reserve-commit", "contextCanceled": errors.Is(err, context.Canceled), "success": err == nil, "error": message})
			stdout.Lock()
			fmt.Fprintf(os.Stdout, "CANCELLED %s\n", outcome)
			stdout.Unlock()
			return
		}
		if err != nil {
			writePersistentJSON(w, http.StatusUnprocessableEntity, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writePersistentJSON(w, http.StatusOK, map[string]any{"ok": true, "response": result.Output})
	})
	for route, program := range map[string]contract.InternalProgram{"POST /business-quote": businessQuote, "POST /business-order": businessOrder} {
		mux.HandleFunc(route, func(w http.ResponseWriter, request *http.Request) {
			defer request.Body.Close()
			var input map[string]any
			if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&input); err != nil {
				writePersistentJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid request"})
				return
			}
			result, err := runner.Run(request.Context(), program, input)
			if err != nil {
				code := ""
				var classified interface{ ErrorCode() string }
				if errors.As(err, &classified) {
					code = classified.ErrorCode()
				}
				writePersistentJSON(w, http.StatusUnprocessableEntity, map[string]any{"ok": false, "error": err.Error(), "code": code})
				return
			}
			writePersistentJSON(w, http.StatusOK, map[string]any{"ok": true, "response": result.Output})
		})
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writePersistentJSON(w, http.StatusOK, map[string]bool{"ready": true})
	})
	mux.HandleFunc("POST /quote", func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var input map[string]any
		decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20))
		if err := decoder.Decode(&input); err != nil {
			writePersistentJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid request"})
			return
		}
		result, err := runner.Run(request.Context(), program, input)
		if err != nil {
			writePersistentJSON(w, http.StatusUnprocessableEntity, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writePersistentJSON(w, http.StatusOK, map[string]any{"ok": true, "response": result.Output})
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stdout.Lock()
	fmt.Fprintf(os.Stdout, "READY http://%s\n", listener.Addr().String())
	stdout.Unlock()
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

// TestNativePersistentWorkerProcess is the child entrypoint for the opt-in
// durable recovery benchmark. The producer commits an accepted job then stays
// alive until its parent kills it; a distinct process reopens SQLite and runs
// the real native engine through trigger/worker.Queue.
func TestNativePersistentWorkerProcess(t *testing.T) {
	role := os.Getenv("BLOK_PARITY_NATIVE_WORKER_ROLE")
	if role == "" {
		return
	}
	databasePath := os.Getenv("BLOK_PARITY_NATIVE_WORKER_DB")
	providerURL := os.Getenv("BLOK_PARITY_PROVIDER_URL")
	requestKey := os.Getenv("BLOK_PARITY_REQUEST_KEY")
	jobID := os.Getenv("BLOK_PARITY_JOB_ID")
	// The provider operation the consumer calls: job-retry fails its first
	// attempt; job-hold holds its first attempt open until the caller dies.
	operation := os.Getenv("BLOK_PARITY_OPERATION")
	if operation == "" {
		operation = "job-retry"
	}
	if databasePath == "" || providerURL == "" || requestKey == "" || jobID == "" {
		t.Fatal("durable native worker requires database, local provider, request key, and job id")
	}
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(ctx, database, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sqliteVersion string
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&sqliteVersion)
	}); err != nil {
		t.Fatal(err)
	}
	inputSchema, outputSchema := schemasForOperation("job-retry")
	if err := queue.RegisterKind("parity.job-retry", inputSchema); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"requestKey": requestKey, "jobId": jobID, "sku": "coffee", "quantity": 2}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if role == "produce" {
		first, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: requestKey, Kind: "parity.job-retry", Payload: encodedPayload, MaxAttempts: 3})
		if err != nil {
			t.Fatal(err)
		}
		duplicate, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: requestKey, Kind: "parity.job-retry", Payload: encodedPayload, MaxAttempts: 3})
		if err != nil {
			t.Fatal(err)
		}
		if !first.Accepted || duplicate.Accepted || duplicate.Job.ID != first.Job.ID {
			t.Fatalf("durable duplicate submission mismatch: first=%+v duplicate=%+v", first, duplicate)
		}
		fmt.Fprintf(os.Stdout, "ACCEPTED %s accepted=%t duplicateAccepted=%t sqliteVersion=%s\n", first.Job.ID, first.Accepted, duplicate.Accepted, sqliteVersion)
		select {}
	}
	if role != "consume" {
		t.Fatalf("unknown persistent worker role %q", role)
	}
	definition, err := node.Define[map[string]any, map[string]any](
		"parity-durable-worker-provider",
		"1.0.0",
		func(callCtx context.Context, input map[string]any) (map[string]any, error) {
			body, err := json.Marshal(input)
			if err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(callCtx, http.MethodPost, providerURL+"/"+operation, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("idempotency-key", requestKey)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			var result map[string]any
			if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
				return nil, err
			}
			if response.StatusCode >= http.StatusBadRequest {
				code, _ := result["code"].(string)
				return nil, &node.DomainError{Code: code, Class: "provider", Err: fmt.Errorf("synthetic provider status %d", response.StatusCode)}
			}
			return map[string]any{"status": response.StatusCode, "body": result}, nil
		},
		node.Description("Shared synthetic provider call for durable worker recovery"),
		node.Schemas(inputSchema, outputSchema),
	)
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := flow.Define(flow.Spec{Name: "parity-durable-worker", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[map[string]any]) flow.Ref[map[string]any] {
		return flow.Call(builder, "provider", definition, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	runner := engine.New(map[string]node.Any{definition.Descriptor().Name: definition.Any()})
	fmt.Fprintln(os.Stdout, "READY worker")
	deadline := time.Now().Add(45 * time.Second)
	var output any
	for time.Now().Before(deadline) {
		job, err := queue.Get(ctx, requestKey)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == worker.StateCompleted {
			settled, err := json.Marshal(map[string]any{"stats": map[string]any{"completed": 1, "failed": max(0, job.Attempt-1), "attempts": job.Attempt}, "response": output})
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(os.Stdout, "RECOVERED %s\n", settled)
			fmt.Fprintf(os.Stdout, "SETTLED %s\n", settled)
			return
		}
		if job.State == worker.StateDead {
			t.Fatalf("durable native job dead-lettered after %d attempts: %s", job.Attempt, job.Error)
		}
		processed, err := queue.ProcessOnce(ctx, func(runCtx context.Context, _ worker.Tx, claimed worker.Job) error {
			if claimed.Kind != "parity.job-retry" {
				return &worker.HandlerError{Retryable: false, Message: "unexpected parity worker kind"}
			}
			var body map[string]any
			if err := json.Unmarshal(claimed.Payload, &body); err != nil {
				return &worker.HandlerError{Retryable: false, Message: err.Error()}
			}
			result, err := runner.Run(runCtx, program, body)
			if err != nil {
				return &worker.HandlerError{Retryable: true, Message: err.Error()}
			}
			output = result.Output
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for durable native job settlement")
}

// persistentHTTPClient keeps loopback connections alive across concurrent
// requests. Go's default transport retains only two idle connections per
// host, so four load workers would churn ephemeral ports until the host runs
// out of them; the published Node runtime's fetch pool reuses connections.
func persistentHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
}

func persistentProviderNode(t *testing.T, providerURL string) node.Definition[map[string]any, map[string]any] {
	t.Helper()
	client := persistentHTTPClient()
	inputSchema, outputSchema := schemasForOperation("quote")
	definition, err := node.Define[map[string]any, map[string]any](
		"parity-persistent-provider",
		"1.0.0",
		func(ctx context.Context, input map[string]any) (map[string]any, error) {
			encoded, err := json.Marshal(input)
			if err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/quote", bytes.NewReader(encoded))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			if key, ok := input["requestKey"].(string); ok {
				request.Header.Set("idempotency-key", key)
			}
			response, err := client.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				return nil, err
			}
			if response.StatusCode >= 400 {
				code, _ := body["code"].(string)
				if code == "" {
					code = "provider_error"
				}
				return nil, &node.DomainError{Code: code, Class: "provider", Err: fmt.Errorf("%s", code)}
			}
			return map[string]any{"status": response.StatusCode, "body": body}, nil
		},
		node.Description("Shared loopback provider call in a persistent parity application"),
		node.Schemas(inputSchema, outputSchema),
		node.Effects("network"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

// compileReserveCommit builds the two-step program through the canonical
// document compiler, with commit reading reserve's `body` by an explicit
// reference. flow.Definition.Lower is not used here: it currently drops call
// input references (#244), so a lowered commit would silently receive the
// workflow input instead of the reservation.
func compileReserveCommit(reserve, commit node.Definition[map[string]any, map[string]any]) (contract.InternalProgram, error) {
	reserveDescriptor, commitDescriptor := reserve.Descriptor(), commit.Descriptor()
	document := contract.Document{
		Version: contract.CurrentVersion,
		Workflow: contract.Workflow{
			ID:           "parity-persistent-reserve-commit",
			Name:         "parity-persistent-reserve-commit",
			Version:      "1.0.0",
			Digest:       runtimecontract.CanonicalDigest([]byte("issue108-reserve-commit-v1")),
			InputSchema:  reserveDescriptor.InputSchema,
			OutputSchema: commitDescriptor.OutputSchema,
			Instructions: []contract.Instruction{
				{ID: "reserve", Kind: "call", Node: reserveDescriptor.Name},
				{ID: "commit", Kind: "call", Node: commitDescriptor.Name, References: []contract.Reference{{Step: "reserve", Path: []string{"body"}}}},
				{ID: "output", Kind: "output", References: []contract.Reference{{Step: "commit"}}},
			},
		},
	}
	for _, descriptor := range []node.Descriptor{reserveDescriptor, commitDescriptor} {
		document.Nodes = append(document.Nodes, contract.NodeDescriptor{ID: descriptor.Name, Version: descriptor.Version, Digest: runtimecontract.CanonicalDigest(descriptor.InputSchema), InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema})
	}
	compiled, err := compile.Compile(document)
	return compiled.Program, err
}

// persistentOperationNode calls one provider operation and returns
// {status, body}; it mirrors the old app's reserve/commit nodes.
func persistentOperationNode(t *testing.T, providerURL, name, operation string) node.Definition[map[string]any, map[string]any] {
	t.Helper()
	client := persistentHTTPClient()
	inputSchema, outputSchema := schemasForOperation(operation)
	definition, err := node.Define[map[string]any, map[string]any](
		name,
		"1.0.0",
		func(ctx context.Context, input map[string]any) (map[string]any, error) {
			encoded, err := json.Marshal(input)
			if err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/"+operation, bytes.NewReader(encoded))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			key, _ := input["requestKey"].(string)
			if operation == "commit" {
				key += "-commit"
			}
			request.Header.Set("idempotency-key", key)
			response, err := client.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				return nil, err
			}
			if response.StatusCode >= http.StatusBadRequest {
				return nil, &node.DomainError{Code: "provider_error", Class: "provider", Err: fmt.Errorf("synthetic provider status %d", response.StatusCode)}
			}
			return map[string]any{"status": response.StatusCode, "body": body}, nil
		},
		node.Description("Shared loopback provider "+operation+" call in a persistent parity application"),
		node.Schemas(inputSchema, outputSchema),
		node.Effects("network"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func writePersistentJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type persistentAppProcess struct {
	command   *exec.Cmd
	url       string
	lines     <-chan string
	stderr    *lockedBuffer
	stopLines chan struct{}
	stopOnce  sync.Once
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startOldPersistentApp(t *testing.T, providerURL string) (*persistentAppProcess, time.Duration) {
	t.Helper()
	command := exec.Command("node", "run.mjs")
	command.Dir = "old-engine"
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL)
	stderr := &lockedBuffer{}
	command.Stderr = stderr
	started := time.Now()
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start pinned old application: %v", err)
	}
	stopLines := make(chan struct{})
	lines := persistentProcessLines(stdout, stopLines)
	if _, err := input.Write([]byte(`{"mode":"persistent-app-server"}`)); err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("configure pinned old application: %v", err)
	}
	if err := input.Close(); err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("finish old application configuration: %v", err)
	}
	ready, err := waitPersistentLine(lines, "READY ", 30*time.Second)
	if err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("old persistent application did not start: %v; stderr=%s", err, stderr.String())
	}
	baseURL, err := parseLoopbackReadyURL(ready)
	if err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("old persistent application emitted invalid readiness URL %q: %v", ready, err)
	}
	process := &persistentAppProcess{command: command, url: baseURL, lines: lines, stderr: stderr, stopLines: stopLines}
	t.Cleanup(func() { process.stop() })
	return process, time.Since(started)
}

func startNewPersistentApp(t *testing.T, providerURL string) (*persistentAppProcess, time.Duration) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestNativePersistentAppProcess$")
	command.Env = append(os.Environ(), "BLOK_PARITY_NATIVE_APP_PROCESS=1", "BLOK_PARITY_PROVIDER_URL="+providerURL)
	stderr := &lockedBuffer{}
	command.Stderr = stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := command.Start(); err != nil {
		t.Fatalf("start native application process: %v", err)
	}
	stopLines := make(chan struct{})
	lines := persistentProcessLines(stdout, stopLines)
	ready, err := waitPersistentLine(lines, "READY ", 30*time.Second)
	if err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("native persistent application did not start: %v; stderr=%s", err, stderr.String())
	}
	baseURL, err := parseLoopbackReadyURL(ready)
	if err != nil {
		abortPersistentProcess(command, stopLines)
		t.Fatalf("native persistent application emitted invalid readiness URL %q: %v", ready, err)
	}
	process := &persistentAppProcess{command: command, url: baseURL, lines: lines, stderr: stderr, stopLines: stopLines}
	t.Cleanup(func() { process.stop() })
	return process, time.Since(started)
}

func parseLoopbackReadyURL(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Hostname() == "" || parsed.Port() == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("expected an absolute HTTP URL with an explicit port")
	}
	address := net.ParseIP(parsed.Hostname())
	if parsed.Hostname() != "localhost" && (address == nil || !address.IsLoopback()) {
		return "", fmt.Errorf("readiness host %q is not loopback", parsed.Hostname())
	}
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

func TestParseLoopbackReadyURL(t *testing.T) {
	for _, test := range []struct {
		name string
		url  string
		want string
		bad  bool
	}{
		{name: "loopback IPv4", url: "http://127.0.0.1:43123", want: "http://127.0.0.1:43123"},
		{name: "loopback hostname", url: "http://localhost:43123", want: "http://localhost:43123"},
		{name: "double scheme", url: "http://http://127.0.0.1:43123", bad: true},
		{name: "remote host", url: "http://example.test:43123", bad: true},
		{name: "missing port", url: "http://127.0.0.1", bad: true},
		{name: "path", url: "http://127.0.0.1:43123/quote", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseLoopbackReadyURL(test.url)
			if test.bad {
				if err == nil {
					t.Fatalf("parseLoopbackReadyURL(%q)=%q, want error", test.url, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("parseLoopbackReadyURL(%q)=%q, %v; want %q", test.url, got, err, test.want)
			}
		})
	}
}

func persistentProcessLines(stdout interface{ Read([]byte) (int, error) }, stop <-chan struct{}) <-chan string {
	lines := make(chan string, 32)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-stop:
				return
			}
		}
	}()
	return lines
}

func abortPersistentProcess(command *exec.Cmd, stopLines chan struct{}) {
	close(stopLines)
	if command != nil && command.Process != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
	}
}

func waitPersistentLine(lines <-chan string, prefix string, timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return "", errors.New("child process exited before expected output")
			}
			if strings.HasPrefix(line, prefix) {
				return strings.TrimPrefix(line, prefix), nil
			}
		case <-timer.C:
			return "", fmt.Errorf("timed out waiting for child output %q", prefix)
		}
	}
}

func startOldWorkerProcess(t *testing.T, input map[string]any, providerURL, postgresURL string) *persistentAppProcess {
	t.Helper()
	command := exec.Command("node", "run.mjs")
	command.Dir = "old-engine"
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL, "BLOK_PARITY_POSTGRES_URL="+postgresURL)
	stderr := &lockedBuffer{}
	command.Stderr = stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start published durable worker process: %v", err)
	}
	stopLines := make(chan struct{})
	process := &persistentAppProcess{command: command, lines: persistentProcessLines(stdout, stopLines), stderr: stderr, stopLines: stopLines}
	encoded, err := json.Marshal(input)
	if err != nil {
		process.stop()
		t.Fatal(err)
	}
	if _, err := stdin.Write(encoded); err != nil {
		process.stop()
		t.Fatalf("configure published durable worker: %v", err)
	}
	if err := stdin.Close(); err != nil {
		process.stop()
		t.Fatalf("finish published durable worker configuration: %v", err)
	}
	t.Cleanup(process.stop)
	return process
}

func startNativeWorkerProcess(t *testing.T, role, databasePath, providerURL, requestKey, jobID string, extraEnv ...string) *persistentAppProcess {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestNativePersistentWorkerProcess$")
	command.Env = append(append(os.Environ(), extraEnv...),
		"BLOK_PARITY_NATIVE_WORKER_ROLE="+role,
		"BLOK_PARITY_NATIVE_WORKER_DB="+databasePath,
		"BLOK_PARITY_PROVIDER_URL="+providerURL,
		"BLOK_PARITY_REQUEST_KEY="+requestKey,
		"BLOK_PARITY_JOB_ID="+jobID,
	)
	stderr := &lockedBuffer{}
	command.Stderr = stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start native durable worker process: %v", err)
	}
	stopLines := make(chan struct{})
	process := &persistentAppProcess{command: command, lines: persistentProcessLines(stdout, stopLines), stderr: stderr, stopLines: stopLines}
	t.Cleanup(process.stop)
	return process
}

func (p *persistentAppProcess) stop() {
	if p == nil || p.command == nil || p.command.Process == nil {
		return
	}
	p.stopOnce.Do(func() {
		if p.stopLines != nil {
			close(p.stopLines)
		}
		_ = p.command.Process.Kill()
		_ = p.command.Wait()
	})
}

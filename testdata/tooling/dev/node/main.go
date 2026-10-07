// Command shop is blok dev's Go+Node fixture application (E11-T03, ADR
// 0026). It owns one persistent Node.js worker over the ADR 0004 protocol,
// uses BLOK_DEV_GENERATION as that worker's generation, and on SIGTERM
// drains HTTP requests, then its worker. Synthetic configuration only.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
)

type Input struct {
	RequestID string `json:"requestId"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DelayMS   int    `json:"delayMs"`
}

type Output struct {
	TotalCents int64  `json:"totalCents"`
	Marker     string `json:"marker"`
	WorkerPID  string `json:"workerPid"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	nodeDir := os.Getenv("BLOK_FIXTURE_NODE_DIR")
	framework := os.Getenv("BLOK_FIXTURE_FRAMEWORK")
	workerAddress := os.Getenv("BLOK_FIXTURE_WORKER_ADDRESS")
	token := os.Getenv("BLOK_FIXTURE_TOKEN")
	generation, err := strconv.ParseUint(os.Getenv("BLOK_DEV_GENERATION"), 10, 64)
	if err != nil || generation == 0 {
		generation = 1
	}
	raw, err := os.ReadFile(filepath.Join(nodeDir, "node.json"))
	if err != nil {
		return err
	}
	var descriptor node.Descriptor
	if err := json.Unmarshal(raw, &descriptor); err != nil {
		return err
	}
	source, err := os.ReadFile(filepath.Join(nodeDir, "index.mjs"))
	if err != nil {
		return err
	}
	// The worker artifact is the node's own source: an edit to it is a new
	// artifact, never the old one relabelled.
	sum := sha256.Sum256(append(append([]byte(nil), raw...), source...))
	artifact := "sha256:" + hex.EncodeToString(sum[:])
	catalog, err := runtime.CatalogDigest([]node.Descriptor{descriptor})
	if err != nil {
		return err
	}
	hello := runtime.Hello{Protocol: runtime.ProtocolName, Major: runtime.ProtocolMajor, Minor: runtime.ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: catalog, Generation: generation, Limits: runtime.DefaultLimits()}
	supervisor, err := worker.New(worker.Config{Hello: hello, Capacity: 8, Factory: worker.ProcessFactory{
		Command: "node", Args: []string{filepath.Join(framework, "runtime/nodejs/dist/runtime/nodejs/main.js"), filepath.Join(nodeDir, "index.mjs")},
		Address: workerAddress, Token: token, Principal: "dev-fixture", StartupTimeout: 5 * time.Second,
		Env: []string{"BLOK_WORKER_ADDRESS=" + workerAddress, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=dev-fixture", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=" + strconv.FormatUint(generation, 10), "BLOK_WORKER_CAPABILITIES=[]"},
	}})
	if err != nil {
		return err
	}
	quote, err := worker.Define[Input, Output](supervisor, descriptor)
	if err != nil {
		return err
	}
	workflow, err := flow.Define(flow.Spec{Name: "slow-quote", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[Input]) flow.Ref[Output] {
		return flow.Call(builder, "quote", quote, input)
	})
	if err != nil {
		return err
	}
	program, err := workflow.Lower()
	if err != nil {
		return err
	}
	application, err := app.New(app.Config{
		Workflows:    []app.Workflow{{Name: "app/slow-quote"}},
		Routes:       []app.Route{{Method: http.MethodPost, Path: "/quotes", Workflow: "app/slow-quote"}},
		Dependencies: []app.Dependency{{Name: "nodejs", Start: supervisor.Start, Close: supervisor.Shutdown}},
	})
	if err != nil {
		return err
	}
	runner := execution.NewRunner(application, map[string]node.Any{"app/slow-quote": quote.Any()})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /quotes", func(w http.ResponseWriter, r *http.Request) {
		var input Input
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input); err != nil {
			http.Error(w, "invalid quote", http.StatusBadRequest)
			return
		}
		result, err := runner.Run(r.Context(), program, input, inspection.Invocation{})
		if err != nil {
			http.Error(w, "quote failed", http.StatusServiceUnavailable)
			return
		}
		output, _ := result.Output.(Output)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"totalCents": output.TotalCents, "marker": output.Marker, "workerPid": output.WorkerPID, "generation": generation, "pid": os.Getpid()})
	})
	// POST /crash ends the process without draining anything: a crash.
	mux.HandleFunc("POST /crash", func(http.ResponseWriter, *http.Request) { os.Exit(3) })
	if err := application.Start(context.Background()); err != nil {
		return err
	}
	server := &http.Server{Addr: os.Getenv("ADDR"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	log.Printf("listening on %s (generation %d)", server.Addr, generation)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-signals:
		// Drain: requests already accepted finish on the running worker,
		// then the worker is stopped by its supervisor.
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		return application.Shutdown(ctx)
	}
	return nil
}

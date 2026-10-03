package workerbench

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/internal/engine"
	supervision "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
	worker "github.com/well-prado/new-blok/runtime/worker"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type observedFactory struct {
	supervision.ProcessFactory
	pid int
}

func (f *observedFactory) Connect(ctx context.Context, h contract.Hello) (supervision.Connection, contract.Ready, error) {
	c, r, e := f.ProcessFactory.Connect(ctx, h)
	if e == nil {
		f.pid = c.(interface{ PID() int }).PID()
	}
	return c, r, e
}
func selectedWorker(t *testing.T, p *Provider) (map[string]node.Any, func(), int, time.Duration) {
	t.Helper()
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("requires built Node worker: BLOK_NODE_INTEGRATION_ROOT")
	}
	native := NativeNodes(p.Server.URL)
	descriptors := []node.Descriptor{native["price"].Descriptor(), native["pay"].Descriptor(), native["receipt"].Descriptor()}
	raw, err := json.Marshal(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "descriptors.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	digest, err := contract.CatalogDigest(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	caps := []contract.Capability{"http:orders"}
	artifact := contract.CanonicalDigest([]byte("synthetic-benchmark-worker"))
	hello := contract.Hello{Protocol: contract.ProtocolName, Major: 1, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Limits: contract.DefaultLimits(), Capabilities: caps}
	f := &observedFactory{ProcessFactory: supervision.ProcessFactory{Command: "node", Args: []string{filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js"), filepath.Join(root, "benchmarks/worker/nodes.mjs")}, Address: address, Token: "synthetic-order-token-0000000000001", Principal: "bench-app", Capabilities: caps, StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=synthetic-order-token-0000000000001", "BLOK_WORKER_PRINCIPAL=bench-app", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1", `BLOK_WORKER_CAPABILITIES=["http:orders"]`, "BLOK_BENCH_SDK=" + filepath.Join(root, "runtime/nodejs/dist/sdk/nodejs/index.js"), "BLOK_BENCH_DESCRIPTORS=" + file, "BLOK_BENCH_PROVIDER=" + p.Server.URL}}}
	s, err := worker.New(worker.Config{Hello: hello, Factory: f, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	startup := time.Since(start)
	close := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(close)
	price, err := worker.Define[Order, Priced](s, descriptors[0])
	if err != nil {
		t.Fatal(err)
	}
	pay, err := worker.DefineScoped[Priced, Paid](s, descriptors[1], caps)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := worker.Define[Paid, Receipt](s, descriptors[2])
	if err != nil {
		t.Fatal(err)
	}
	return map[string]node.Any{"price": price.Any(), "pay": pay.Any(), "receipt": receipt.Any()}, close, f.pid, startup
}
func TestActualNodeEquivalentOrderFailureAndIdempotency(t *testing.T) {
	p := NewProvider()
	defer p.Close()
	nodes, _, _, _ := selectedWorker(t, p)
	w, err := NewWorkflow(nodes)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result, err := w.Run(context.Background(), Order{"same", "coffee", 2, "ok"})
		if err != nil || result.Output != (Receipt{"same", 3000, "receipt-same", "paid"}) {
			t.Fatalf("result %+v error %v", result, err)
		}
	}
	requests, effects := p.Counts()
	if requests != 2 || effects != 1 {
		t.Fatalf("counts %d/%d", requests, effects)
	}
	for _, fixture := range []struct {
		id, sku, mode, code string
		effects             int
	}{{"unknown", "wrong", "ok", "unknown_sku", 0}, {"reject", "coffee", "reject", "payment_declined", 0}, {"transient", "coffee", "transient", "provider_unavailable", 0}, {"uncertain", "coffee", "uncertain", "provider_uncertain", 1}} {
		_, before := p.Counts()
		result, err := w.Run(context.Background(), Order{fixture.id, fixture.sku, 2, fixture.mode})
		var failure *engine.Error
		class := "node_error"
		if fixture.mode == "uncertain" {
			class = "uncertain"
		}
		if fixture.mode == "transient" {
			class = "transient"
		}
		if !errors.As(err, &failure) || failure.Code != fixture.code || failure.Class != class || result.Output != nil {
			t.Fatalf("%s output %+v error %v", fixture.id, result, err)
		}
		_, after := p.Counts()
		if after-before != fixture.effects {
			t.Fatalf("%s effect count %d", fixture.id, after-before)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := w.Run(ctx, Order{"cancel", "coffee", 2, "delay"})
	if !errors.Is(err, context.DeadlineExceeded) || result.Output != nil {
		t.Fatalf("cancellation published %+v %v", result, err)
	}
}
func TestActualNodeKillBeforeAndAfterProviderEffect(t *testing.T) {
	for _, phase := range []string{"delay", "late"} {
		t.Run(phase, func(t *testing.T) {
			_ = observeWorkerCrash(t, phase)
		})
	}
}

type faultSample struct {
	Phase, ObservedAtUTC, ErrorClass                                         string
	StartupNanoseconds, KillToTerminalNanoseconds, NativeSurvivalNanoseconds int64
	Requests, Effects, PublishedReceipts                                     int
}

// Reuse the predeclared crash fixtures; timing does not change their outcome
// or effect expectations. The native survival probe is outside the crash count.
func observeWorkerCrash(t *testing.T, phase string) faultSample {
	t.Helper()
	p := NewProvider()
	defer p.Close()
	nodes, _, pid, startup := selectedWorker(t, p)
	w, err := NewWorkflow(nodes)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		result, err := w.Run(context.Background(), Order{phase, "coffee", 2, phase})
		if result.Output != nil {
			done <- errors.New("killed worker published receipt")
			return
		}
		done <- err
	}()
	if phase == "late" {
		select {
		case <-p.Committed():
		case <-time.After(2 * time.Second):
			t.Fatal("effect not committed")
		}
	} else {
		deadline := time.Now().Add(2 * time.Second)
		for {
			requests, _ := p.Counts()
			if requests > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("read not dispatched")
			}
			time.Sleep(time.Millisecond)
		}
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	killed := time.Now()
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	var failure *engine.Error
	select {
	case err := <-done:
		if !errors.As(err, &failure) || failure.Class != "uncertain" {
			t.Fatalf("lost worker effect not uncertain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost worker call hung")
	}
	s := faultSample{Phase: phase, ObservedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), ErrorClass: failure.Class, StartupNanoseconds: startup.Nanoseconds(), KillToTerminalNanoseconds: time.Since(killed).Nanoseconds()}
	s.Requests, s.Effects = p.Counts()
	want := 0
	if phase == "late" {
		want = 1
	}
	if s.Requests != 1 || s.Effects != want {
		t.Fatalf("automatic retry or unexpected effect: %d/%d", s.Requests, s.Effects)
	}
	native, err := NewWorkflow(NativeNodes(p.Server.URL))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result, err := native.Run(context.Background(), Order{"surviving-native", "coffee", 2, "ok"})
	s.NativeSurvivalNanoseconds = time.Since(start).Nanoseconds()
	if err != nil || result.Output != (Receipt{"surviving-native", 3000, "receipt-surviving-native", "paid"}) {
		t.Fatalf("worker crash affected native engine: %+v %v", result, err)
	}
	return s
}

type sample struct {
	Mode                    string  `json:"mode"`
	Nanoseconds             []int64 `json:"nanoseconds"`
	Requests, Effects       int
	ObservedAtUTC           string
	GoHarnessRSSSnapshotKiB int
	WorkerRSSSnapshotKiB    *int `json:",omitempty"`
}
type startupSample struct {
	ObservedAtUTC                                 string
	StartupNanoseconds                            int64
	WorkerRSSSnapshotKiB, GoHarnessRSSSnapshotKiB int
}
type report struct {
	Go, Node, OS, Arch, Guarantees                                string
	SourceRevision, StartedAtUTC, FinishedAtUTC, Topology, Kernel string
	Workload, OrderSequence, RSSMethod, StartupMethod             string
	SchemaVersion, CPUsVisible, GoMaxProcs                        int
	Warmup, Concurrency                                           int
	StartupSamples                                                []startupSample
	FaultSamples                                                  []faultSample
	Samples                                                       []sample
}

func rssSnapshot(t *testing.T, pid int) int {
	t.Helper()
	raw, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatalf("RSS snapshot unavailable for pid %d: %v", pid, err)
	}
	rss, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || rss <= 0 {
		t.Fatalf("invalid RSS snapshot %q: %v", raw, err)
	}
	return rss
}

func TestControlledEquivalentOrderSamples(t *testing.T) {
	if os.Getenv("BLOK_WORKLOAD_REPORT") == "" {
		t.Skip("explicit measured evidence gate: BLOK_WORKLOAD_REPORT")
	}
	version, err := exec.Command("node", "--version").Output()
	if err != nil {
		t.Fatal(err)
	}
	revision := os.Getenv("BLOK_BENCH_SOURCE_REVISION")
	if decoded, err := hex.DecodeString(revision); err != nil || len(decoded) != 20 {
		t.Fatal("BLOK_BENCH_SOURCE_REVISION must identify the committed measured source")
	}
	topology := os.Getenv("BLOK_BENCH_TOPOLOGY")
	if topology == "" {
		t.Fatal("BLOK_BENCH_TOPOLOGY must describe host/container and CPU constraints")
	}
	kernel, err := exec.Command("uname", "-srv").Output()
	if err != nil {
		t.Fatal(err)
	}
	r := report{SchemaVersion: 2, SourceRevision: revision, StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), Topology: topology, Kernel: strings.TrimSpace(string(kernel)), CPUsVisible: runtime.NumCPU(), GoMaxProcs: runtime.GOMAXPROCS(0), Go: runtime.Version(), Node: strings.TrimSpace(string(version)), OS: runtime.GOOS, Arch: runtime.GOARCH, Warmup: 20, Concurrency: 1, Workload: "issue53-order-v1: price -> HTTP payment -> receipt -> response", OrderSequence: "deterministic <mode>-1 through <mode>-270; no randomized seed", RSSMethod: "ps -o rss= -p PID: separate instantaneous resident KiB snapshots for Node child and Go harness; not peak or isolated native-node memory", StartupMethod: "five fresh Node processes, sequential on warm filesystem caches; process launch through authenticated negotiated readiness; fifth process reused for measured load", Guarantees: "memory-mode real engine, equivalent schemas and immutable engine copies, same actual loopback HTTP idempotent provider and 2-second provider timeout; Node additionally normalizes via SDK and crosses token/principal-authenticated gRPC; native nodes are trusted in-process; no durable or equivalent-cost claim"}
	for _, mode := range []string{"native", "node"} {
		p := NewProvider()
		t.Cleanup(p.Close)
		var nodes map[string]node.Any
		var close func()
		pid := 0
		if mode == "native" {
			nodes = NativeNodes(p.Server.URL)
		} else {
			for i := 0; i < 5; i++ {
				var startup time.Duration
				nodes, close, pid, startup = selectedWorker(t, p)
				r.StartupSamples = append(r.StartupSamples, startupSample{ObservedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartupNanoseconds: startup.Nanoseconds(), WorkerRSSSnapshotKiB: rssSnapshot(t, pid), GoHarnessRSSSnapshotKiB: rssSnapshot(t, os.Getpid())})
				if requests, effects := p.Counts(); requests != 0 || effects != 0 {
					t.Fatal("startup probe executed business effects")
				}
				if i < 4 {
					close()
				}
			}
		}
		w, err := NewWorkflow(nodes)
		if err != nil {
			t.Fatal(err)
		}
		sequence := 0
		run := func() int64 {
			sequence++
			start := time.Now()
			out, err := w.Run(context.Background(), Order{fmt.Sprintf("%s-%d", mode, sequence), "coffee", 2, "ok"})
			elapsed := time.Since(start).Nanoseconds()
			if err != nil || out.Output != (Receipt{fmt.Sprintf("%s-%d", mode, sequence), 3000, "receipt-" + fmt.Sprintf("%s-%d", mode, sequence), "paid"}) {
				t.Fatalf("%s %+v %v", mode, out, err)
			}
			return elapsed
		}
		for i := 0; i < r.Warmup; i++ {
			run()
		}
		for batch := 0; batch < 5; batch++ {
			s := sample{Mode: mode}
			for i := 0; i < 50; i++ {
				s.Nanoseconds = append(s.Nanoseconds, run())
			}
			s.Requests, s.Effects = p.Counts()
			s.ObservedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
			s.GoHarnessRSSSnapshotKiB = rssSnapshot(t, os.Getpid())
			if pid != 0 {
				rss := rssSnapshot(t, pid)
				s.WorkerRSSSnapshotKiB = &rss
			}
			r.Samples = append(r.Samples, s)
		}
		requests, effects := p.Counts()
		if requests != 270 || effects != 270 {
			t.Fatalf("not equivalent: %s %d/%d", mode, requests, effects)
		}
		if close != nil {
			close()
		}
		p.Close()
	}
	// Fault timing is separate from throughput, with unchanged 1/0 and 1/1
	// request/effect expectations and no receipt publication in either phase.
	for i := 0; i < 5; i++ {
		for _, phase := range []string{"delay", "late"} {
			t.Run(fmt.Sprintf("crash-%d-%s", i, phase), func(t *testing.T) { r.FaultSamples = append(r.FaultSamples, observeWorkerCrash(t, phase)) })
		}
	}
	if t.Failed() {
		t.Fatal("refusing to publish measured evidence after a failed fault fixture")
	}
	r.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("BLOK_WORKLOAD_REPORT"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

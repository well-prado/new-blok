package otel_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
)

const collectorConfig = `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: basic
service:
  telemetry:
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      exporters: [debug]
    logs:
      receivers: [otlp]
      exporters: [debug]
`

// realCollector is an actual OpenTelemetry Collector container with a
// unique name, removed when the test ends.
type realCollector struct {
	name, otlp, telemetry string
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func startRealCollector(t *testing.T) *realCollector {
	t.Helper()
	image := os.Getenv("BLOK_OTEL_COLLECTOR_IMAGE")
	if image == "" {
		t.Skip("set BLOK_OTEL_COLLECTOR_IMAGE (for example otel/opentelemetry-collector:0.162.0) to run against an actual collector container")
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	c := &realCollector{name: "blok-otel-e16t01-" + hex.EncodeToString(suffix)}
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte(collectorConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	otlpPort, telemetryPort := freePort(t), freePort(t)
	c.otlp = fmt.Sprintf("http://127.0.0.1:%d", otlpPort)
	c.telemetry = fmt.Sprintf("http://127.0.0.1:%d/metrics", telemetryPort)
	docker(t, "run", "-d", "--name", c.name, "-p", fmt.Sprintf("127.0.0.1:%d:4318", otlpPort), "-p", fmt.Sprintf("127.0.0.1:%d:8888", telemetryPort), "-v", config+":/etc/otelcol/config.yaml:ro", image, "--config", "/etc/otelcol/config.yaml")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", c.name).Run() })
	c.waitReady(t)
	return c
}

func (c *realCollector) waitReady(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if _, err := c.accepted("spans"); err == nil {
			return
		}
	}
	t.Fatalf("collector %s never became ready", c.name)
}

// accepted sums the collector's own otelcol_receiver_accepted_<signal>
// counters from its Prometheus self-telemetry.
func (c *realCollector) accepted(signal string) (float64, error) {
	response, err := http.Get(c.telemetry)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("telemetry status %d", response.StatusCode)
	}
	total := 0.0
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "otelcol_receiver_accepted_"+signal) {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			total += value
		}
	}
	return total, scanner.Err()
}

func (c *realCollector) waitAccepted(t *testing.T, signal string, want float64) {
	t.Helper()
	var got float64
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		got, _ = c.accepted(signal)
		if got >= want {
			break
		}
	}
	if got != want {
		t.Fatalf("collector accepted %v %s, want %v", got, signal, want)
	}
}

// TestActualCollectorContainerReceivesAndSurvivesOutages exports to an actual
// OpenTelemetry Collector, then stops it (connection refused), pauses it (a
// hang until the export timeout) and restarts it. Runs keep their results,
// failures are counted, and export resumes after the restart.
func TestActualCollectorContainerReceivesAndSurvivesOutages(t *testing.T) {
	collector := startRealCollector(t)
	ctx := context.Background()
	traces, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(collector.otlp+"/v1/traces"), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(collector.otlp+"/v1/metrics"), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
	if err != nil {
		t.Fatal(err)
	}
	logs, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(collector.otlp+"/v1/logs"), otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
	if err != nil {
		t.Fatal(err)
	}
	exporter, err := otel.New(otel.Config{Traces: traces, Metrics: metrics, Logs: logs, ExportTimeout: time.Second, FlushInterval: 20 * time.Millisecond, MetricInterval: 10 * time.Minute, LogAttributes: []string{"sku"}})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Shutdown(context.Background())
	h := &harness{exporter: exporter}
	runner, program := orderRunner(t, h, exporter)
	order := func(phase string, n int) {
		for i := 0; i < n; i++ {
			start := time.Now()
			result, err := runner.Run(ctx, program, orderInput{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: fmt.Sprintf("run-%s-%d", phase, i), Principal: principalSentinel, Tenant: "tenant-a"})
			if err != nil || result.Output.(orderOutput).TotalCents != 1500 {
				t.Fatalf("%s run %d: %v", phase, i, err)
			}
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				t.Fatalf("%s run %d took %v", phase, i, elapsed)
			}
		}
	}
	flush := func() {
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = exporter.Flush(flushCtx)
	}
	order("up", 10)
	flush()
	collector.waitAccepted(t, "spans", 40)
	collector.waitAccepted(t, "log_records", 10)
	if points, _ := collector.accepted("metric_points"); points == 0 {
		t.Fatal("collector accepted no metric points")
	}

	docker(t, "stop", collector.name)
	order("stopped", 5)
	flush()
	stopped := exporter.Stats()
	if stopped.SpansFailed != 20 || stopped.LogsFailed != 5 || stopped.MetricFailures == 0 {
		t.Fatalf("stopped collector: %+v", stopped)
	}

	docker(t, "start", collector.name)
	collector.waitReady(t)
	docker(t, "pause", collector.name)
	started := time.Now()
	order("paused", 3)
	flush()
	if elapsed := time.Since(started); elapsed > 8*time.Second {
		t.Fatalf("a paused collector held the pipeline for %v", elapsed)
	}
	docker(t, "unpause", collector.name)
	paused := exporter.Stats()
	if paused.SpansFailed != 32 {
		t.Fatalf("paused collector: %+v", paused)
	}

	// A batch that timed out against the paused container was already on the
	// wire: the collector may still accept it once it resumes. The exporter
	// counted it failed and never resends it, so the restarted collector
	// (which counts from zero) holds at most those late spans plus the new
	// ones, and never a replay of the refused phase.
	time.Sleep(time.Second)
	late, _ := collector.accepted("spans")
	if late != 0 && late != 12 {
		t.Fatalf("after unpause the collector holds %v spans; want 0 or the 12 timed-out spans", late)
	}
	order("restarted", 3)
	flush()
	collector.waitAccepted(t, "spans", late+12)
	t.Logf("spans delivered late from the timed-out batches: %v", late)
	if h.effects.Load() != 21 {
		t.Fatalf("effects %d, want 21", h.effects.Load())
	}
	t.Logf("actual collector %s: %+v", os.Getenv("BLOK_OTEL_COLLECTOR_IMAGE"), exporter.Stats())
}

// orderRunner composes the order workflow of newHarness around a given
// exporter, for callers that bring their own collector.
func orderRunner(t *testing.T, h *harness, exporter *otel.Exporter) (*execution.Runner, contract.InternalProgram) {
	t.Helper()
	return orderRunnerWith(t, h, exporter)
}

// orderRunnerWith is orderRunner with any observer in front of the exporter.
func orderRunnerWith(t *testing.T, h *harness, observer inspection.Observer) (*execution.Runner, contract.InternalProgram) {
	t.Helper()
	validate, charge, program := orderDefinitions(t, h)
	application, err := app.New(app.Config{Inspection: observer, Trace: observe.TracePolicy{Ratio: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return execution.NewRunner(application, map[string]node.Any{"shop/validate": validate.Any(), "shop/charge": charge.Any()}), program
}

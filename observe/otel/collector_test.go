package otel_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/well-prado/new-blok/observe/otel"
)

// collector is an in-process OTLP/HTTP receiver. It decodes the protobuf
// requests the real OTLP exporters send, keeps every raw body for leak
// scans, and can be switched into an outage (503, or a hang past the export
// timeout) to exercise the exporter-failure policy.
type collector struct {
	server *httptest.Server
	mu     sync.Mutex
	spans  []*tracepb.Span
	points []metricPoint
	logs   []*logspb.LogRecord
	bodies [][]byte
	paths  map[string]int
	mode   string // "", "unavailable" or "hang"
	failed int
}

type metricPoint struct {
	name  string
	attrs map[string]string
	value float64
	count uint64
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{paths: map[string]int{}}
	c.server = httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(c.server.Close)
	return c
}

func (c *collector) setMode(mode string) {
	c.mu.Lock()
	c.mode = mode
	c.mu.Unlock()
}

func (c *collector) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	mode := c.mode
	c.mu.Unlock()
	switch mode {
	case "unavailable":
		c.mu.Lock()
		c.failed++
		c.mu.Unlock()
		http.Error(w, "synthetic outage", http.StatusServiceUnavailable)
		return
	case "hang":
		c.mu.Lock()
		c.failed++
		c.mu.Unlock()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		return
	}
	var response proto.Message
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies = append(c.bodies, body)
	c.paths[r.URL.Path]++
	switch r.URL.Path {
	case "/v1/traces":
		var request coltrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		for _, resource := range request.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				c.spans = append(c.spans, scope.Spans...)
			}
		}
		response = &coltrace.ExportTraceServiceResponse{}
	case "/v1/metrics":
		var request colmetrics.ExportMetricsServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		for _, resource := range request.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				for _, m := range scope.Metrics {
					c.points = append(c.points, flatten(m)...)
				}
			}
		}
		response = &colmetrics.ExportMetricsServiceResponse{}
	case "/v1/logs":
		var request collogs.ExportLogsServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		for _, resource := range request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				c.logs = append(c.logs, scope.LogRecords...)
			}
		}
		response = &collogs.ExportLogsServiceResponse{}
	default:
		http.NotFound(w, r)
		return
	}
	encoded, _ := proto.Marshal(response)
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(encoded)
}

func flatten(m *metricspb.Metric) []metricPoint {
	var out []metricPoint
	switch data := m.Data.(type) {
	case *metricspb.Metric_Sum:
		for _, p := range data.Sum.DataPoints {
			out = append(out, metricPoint{name: m.Name, attrs: attrs(p.Attributes), value: float64(p.GetAsInt()) + p.GetAsDouble()})
		}
	case *metricspb.Metric_Histogram:
		for _, p := range data.Histogram.DataPoints {
			out = append(out, metricPoint{name: m.Name, attrs: attrs(p.Attributes), count: p.Count, value: p.GetSum()})
		}
	}
	return out
}

func attrs(values []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, kv := range values {
		out[kv.Key] = anyString(kv.Value)
	}
	return out
}

func anyString(v *commonpb.AnyValue) string {
	switch value := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return value.StringValue
	case *commonpb.AnyValue_BoolValue:
		if value.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(value.IntValue, 10)
	default:
		return v.String()
	}
}

func (c *collector) snapshot() ([]*tracepb.Span, []metricPoint, []*logspb.LogRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*tracepb.Span(nil), c.spans...), append([]metricPoint(nil), c.points...), append([]*logspb.LogRecord(nil), c.logs...)
}

// latest returns the most recent cumulative value of each metric series.
func (c *collector) latest(name string) map[string]metricPoint {
	_, points, _ := c.snapshot()
	out := map[string]metricPoint{}
	for _, p := range points {
		if p.name != name {
			continue
		}
		out[key(p.attrs)] = p
	}
	return out
}

func key(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString(k + "=" + values[k] + ";")
	}
	return b.String()
}

func (c *collector) contains(needle string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, body := range c.bodies {
		if bytes.Contains(body, []byte(needle)) {
			return true
		}
	}
	return false
}

func (c *collector) requests(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths[path]
}

type signals struct{ traces, metrics, logs bool }

var allSignals = signals{true, true, true}

// exporters returns real OTLP/HTTP exporters aimed at the collector, with
// exporter-level retry disabled so the pipeline's own policy is what runs.
func (c *collector) exporters(t *testing.T, selected signals) otel.Config {
	t.Helper()
	ctx := context.Background()
	var config otel.Config
	if selected.traces {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(c.server.URL+"/v1/traces"), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if err != nil {
			t.Fatal(err)
		}
		config.Traces = exporter
	}
	if selected.metrics {
		exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(c.server.URL+"/v1/metrics"), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
		if err != nil {
			t.Fatal(err)
		}
		config.Metrics = exporter
	}
	if selected.logs {
		exporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(c.server.URL+"/v1/logs"), otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
		if err != nil {
			t.Fatal(err)
		}
		config.Logs = exporter
	}
	config.MetricInterval = 10 * time.Minute // tests collect explicitly through Flush
	config.FlushInterval = 20 * time.Millisecond
	config.ExportTimeout = 500 * time.Millisecond
	return config
}

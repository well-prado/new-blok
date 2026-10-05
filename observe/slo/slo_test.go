package slo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/observe/slo"
)

var update = flag.Bool("update", false, "rewrite examples/monitoring/catalogue.json")

// TestClassifyLivenessIsTheAlertContract pins the paging semantics: waiting
// never needs an owner, uncertain is never stalled, and only work that needs
// an owner which is gone is stalled.
func TestClassifyLivenessIsTheAlertContract(t *testing.T) {
	for _, c := range []struct {
		name string
		in   slo.Observation
		want slo.Liveness
	}{
		{"unclaimed with an owner", slo.Observation{OwnerLive: true}, slo.Pending},
		{"claimed under a live lease", slo.Observation{Claimed: true, OwnerLive: true}, slo.Active},
		{"claimed, lease expired", slo.Observation{Claimed: true}, slo.Stalled},
		{"unclaimed, partition unowned", slo.Observation{}, slo.Stalled},
		{"suspended on a signal, owner live", slo.Observation{Waiting: true, OwnerLive: true}, slo.Waiting},
		{"suspended on a signal, owner died", slo.Observation{Waiting: true}, slo.Waiting},
		{"suspended and claimed, owner died", slo.Observation{Waiting: true, Claimed: true}, slo.Waiting},
		{"uncertain effect, owner died", slo.Observation{Uncertain: true, Claimed: true}, slo.Uncertain},
		{"uncertain while waiting", slo.Observation{Uncertain: true, Waiting: true}, slo.Uncertain},
	} {
		if got := slo.Classify(c.in); got != c.want {
			t.Errorf("%s: Classify(%+v) = %s, want %s", c.name, c.in, got, c.want)
		}
	}
}

func TestSnapshotValidateRefusesUnboundedLabels(t *testing.T) {
	many := make([]slo.Work, slo.MaxNamed+1)
	for i := range many {
		many[i].Source = fmt.Sprintf("queue-%d", i)
	}
	for name, s := range map[string]slo.Snapshot{
		"too many sources":   {Work: many},
		"label shape":        {Work: []slo.Work{{Source: "orders queue"}}},
		"too long":           {Workers: []slo.Worker{{Name: strings.Repeat("w", slo.MaxNameBytes+1)}}},
		"duplicate":          {Storage: []slo.Storage{{Name: "journal"}, {Name: "journal"}}},
		"unknown reason":     {Admission: &slo.Admission{Rejected: map[slo.RejectReason]uint64{"tenant-42": 1}}},
		"negative":           {Timers: []slo.Timers{{Source: "cluster", Lag: -time.Second}}},
		"owned over total":   {Partitions: &slo.Partitions{Total: 2, Owned: 3}},
		"dependency invalid": {Readiness: &slo.Readiness{Dependencies: []slo.Dependency{{Name: ""}}}},
	} {
		if err := s.Validate(); !errors.Is(err, slo.ErrInvalid) {
			t.Errorf("%s: Validate = %v", name, err)
		}
		if err := slo.WriteText(&bytes.Buffer{}, s); err == nil {
			t.Errorf("%s: WriteText accepted an invalid snapshot", name)
		}
	}
}

// TestSamplerBoundsEverySource: a stuck source is abandoned at the timeout
// and skipped while it is still running, a panicking, failing, invalid or
// conflicting source is counted, and every healthy part is kept.
func TestSamplerBoundsEverySource(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	var stuckCalls atomic.Int32
	sampler, err := slo.NewSampler(50*time.Millisecond,
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{Readiness: &slo.Readiness{Ready: true}, Work: []slo.Work{{Source: "orders", Pending: 2}}}, nil
		},
		func(context.Context) (slo.Snapshot, error) { stuckCalls.Add(1); <-stuck; return slo.Snapshot{}, nil },
		func(context.Context) (slo.Snapshot, error) { panic("source bug") },
		func(context.Context) (slo.Snapshot, error) { return slo.Snapshot{}, errors.New("store down") },
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{Work: []slo.Work{{Source: "bad source"}}}, nil
		},
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{Readiness: &slo.Readiness{}}, nil // a second readiness
		},
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{Storage: []slo.Storage{{Name: "journal", Used: 10}}}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s := sampler.Sample(context.Background())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a stuck source held the sample for %v", elapsed)
	}
	if s.Readiness == nil || !s.Readiness.Ready || len(s.Work) != 1 || s.Work[0].Pending != 2 || len(s.Storage) != 1 {
		t.Fatalf("healthy parts lost: %+v", s)
	}
	if s.SampleFailures != 5 {
		t.Fatalf("failures = %d, want 5 (stuck, panic, error, invalid, conflict)", s.SampleFailures)
	}
	again := sampler.Sample(context.Background())
	if again.SampleFailures != 10 || stuckCalls.Load() != 1 {
		t.Fatalf("second sample failures = %d, stuck source started %d times; want 10 and once: it is skipped, not restarted", again.SampleFailures, stuckCalls.Load())
	}
	if _, err := slo.NewSampler(time.Hour); err == nil {
		t.Fatal("an unbounded sample timeout was accepted")
	}
	if _, err := slo.NewSampler(0, nil); err == nil {
		t.Fatal("a nil source was accepted")
	}
}

func fullSnapshot() slo.Snapshot {
	return slo.Snapshot{
		Readiness:  &slo.Readiness{Ready: true, Dependencies: []slo.Dependency{{Name: "artifact", Ready: true}, {Name: "store", Ready: false}}},
		Admission:  &slo.Admission{Active: 3, Capacity: 4, Accepted: 10, Rejected: map[slo.RejectReason]uint64{slo.RejectCapacity: 2}},
		Work:       []slo.Work{{Source: "orders", Pending: 1, Stalled: 1, DeadLetters: 2, OldestPending: 1500 * time.Millisecond}, {Source: "cluster", Waiting: 4, Truncated: true}},
		Timers:     []slo.Timers{{Source: "cluster", Overdue: 1, Lag: 2 * time.Second}},
		Workers:    []slo.Worker{{Name: "node", Ready: true, InFlight: 3, Capacity: 64}},
		Partitions: &slo.Partitions{Total: 8, Owned: 7},
		Storage:    []slo.Storage{{Name: "journal", Used: 4096, Budget: 8192}, {Name: "queue", Used: 1}},
	}
}

// TestWriteTextIsTheCatalogue: every rendered series is a catalogued
// snapshot metric with exactly its catalogued labels, closed vocabularies
// are rendered in full (zeros included), and the values are the snapshot's.
func TestWriteTextIsTheCatalogue(t *testing.T) {
	var b bytes.Buffer
	if err := slo.WriteText(&b, fullSnapshot()); err != nil {
		t.Fatal(err)
	}
	samples, err := promrule.ParseText(&b)
	if err != nil {
		t.Fatal(err)
	}
	catalogue := map[string]slo.Metric{}
	for _, m := range slo.Catalogue() {
		catalogue[m.Prometheus] = m
	}
	values := map[string]float64{}
	for _, s := range samples {
		m, ok := catalogue[s.Labels["__name__"]]
		if !ok || m.Path != slo.PathSnapshot {
			t.Fatalf("uncatalogued series %s", s.Labels)
		}
		if len(s.Labels)-1 != len(m.Labels) {
			t.Fatalf("%s has labels %v, catalogue declares %v", m.Prometheus, s.Labels, m.Labels)
		}
		key := m.Prometheus
		for _, l := range m.Labels {
			v, ok := s.Labels[l.Prometheus]
			if !ok || len(l.Values) > 0 && !contains(l.Values, v) {
				t.Fatalf("%s label %s=%q outside the catalogue", m.Prometheus, l.Prometheus, v)
			}
			key += "," + v
		}
		values[key] = s.Value
	}
	for key, want := range map[string]float64{
		"blok_ready": 1, "blok_dependency_ready,store": 0, "blok_admission_active": 3, "blok_admission_capacity": 4,
		"blok_admission_requests_total,accepted,none": 10, "blok_admission_requests_total,rejected,capacity": 2, "blok_admission_requests_total,rejected,conflict": 0,
		"blok_work_items,orders,stalled": 1, "blok_work_items,orders,waiting": 0, "blok_work_items,cluster,waiting": 4, "blok_work_items,cluster,stalled": 0,
		"blok_work_oldest_pending_age_seconds,orders": 1.5, "blok_work_dead_letters,orders": 2, "blok_census_truncated,cluster": 1,
		"blok_timers_overdue,cluster": 1, "blok_timer_lag_seconds,cluster": 2, "blok_worker_capacity,node": 64,
		"blok_partitions,false": 1, "blok_partitions,true": 7, "blok_storage_budget_bytes,journal": 8192, "blok_operational_sample_failures_total": 0,
	} {
		if got, ok := values[key]; !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", key, got, ok, want)
		}
	}
	if _, ok := values["blok_storage_budget_bytes,queue"]; ok {
		t.Error("an undeclared budget was exported as zero")
	}
}

func contains(values []string, v string) bool {
	for _, value := range values {
		if value == v {
			return true
		}
	}
	return false
}

// TestCatalogueBoundsAndNames: names follow the documented translation,
// snapshot metrics have a finite series bound, the tenant label exists only
// on run counters and is bounded by the allowlist, and run, step and
// external outcomes keep uncertain apart from failed.
func TestCatalogueBoundsAndNames(t *testing.T) {
	seen := map[string]bool{}
	counts := map[slo.Counts]bool{}
	for _, m := range slo.Catalogue() {
		if seen[m.Name] || seen[m.Prometheus] {
			t.Fatalf("duplicate %s", m.Name)
		}
		seen[m.Name], seen[m.Prometheus] = true, true
		counts[m.Counts] = true
		if m.Prometheus != slo.PrometheusName(m.Name, m.Unit, m.Kind) {
			t.Errorf("%s: Prometheus name %s", m.Name, m.Prometheus)
		}
		if m.Path == slo.PathSnapshot && (m.SeriesBound == 0 || m.SeriesBound > slo.MaxNamed*len(slo.Livenesses)) {
			t.Errorf("%s: snapshot series bound %d", m.Name, m.SeriesBound)
		}
		for _, l := range m.Labels {
			if l.Name == slo.AttrTenant && l.Bound != slo.TenantBound {
				t.Errorf("%s: tenant bound %d", m.Name, l.Bound)
			}
			if l.Name == slo.AttrTenant && m.Name != slo.MetricRuns && m.Name != slo.MetricRunDuration {
				t.Errorf("%s carries the tenant label", m.Name)
			}
			if l.Name == slo.AttrOutcome && (!contains(l.Values, "uncertain") || !contains(l.Values, "failed")) {
				t.Errorf("%s: outcome vocabulary %v", m.Name, l.Values)
			}
		}
	}
	for _, c := range []slo.Counts{slo.CountsAdmission, slo.CountsCompletion, slo.CountsSteps, slo.CountsExternal} {
		if !counts[c] {
			t.Errorf("nothing counts %s", c)
		}
	}
	for name, want := range map[string]string{"blok.timer.lag": "blok_timer_lag_seconds", "blok.storage.used": "blok_storage_used_bytes", "blok.runs": "blok_runs_total", "blok.work.items": "blok_work_items"} {
		m, ok := slo.Lookup(name)
		if !ok || m.Prometheus != want {
			t.Errorf("%s -> %s, want %s", name, m.Prometheus, want)
		}
	}
}

// TestCatalogueFileHasNoDrift: examples/monitoring/catalogue.json is
// generated from Catalogue (go test ./observe/slo -run CatalogueFile -update).
func TestCatalogueFileHasNoDrift(t *testing.T) {
	encoded, err := json.MarshalIndent(map[string]any{
		"generated":       "by go test ./observe/slo -run TestCatalogueFileHasNoDrift -update; do not edit",
		"adr":             "docs/decisions/0022-operational-slo-metrics.md",
		"durationBuckets": slo.DurationBuckets,
		"livenesses":      slo.Livenesses,
		"rejectReasons":   slo.RejectReasons,
		"metrics":         slo.Catalogue(),
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("..", "..", "examples", "monitoring", "catalogue.json")
	if *update {
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, encoded) {
		t.Fatal("examples/monitoring/catalogue.json drifted from slo.Catalogue; rerun with -update")
	}
}

func TestFileStorageSumsFiles(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "db"), make([]byte, 100), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "db-wal"), make([]byte, 20), 0o600)
	s, err := slo.FileStorage("journal", 1000, filepath.Join(dir, "db"), filepath.Join(dir, "db-wal"), filepath.Join(dir, "db-shm"))(context.Background())
	if err != nil || len(s.Storage) != 1 || s.Storage[0].Used != 120 || s.Storage[0].Budget != 1000 {
		t.Fatalf("storage %+v %v", s, err)
	}
}

// TestSloLinksOnlyTheStandardLibrary: selecting the SLO port costs an
// application no network, telemetry or third-party package.
func TestSloLinksOnlyTheStandardLibrary(t *testing.T) {
	gotool := filepath.Join(runtime.GOROOT(), "bin", "go")
	output, err := exec.Command(gotool, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, output)
	}
	for _, path := range strings.Fields(string(output)) {
		switch path {
		case "github.com/well-prado/new-blok/observe/slo", "github.com/well-prado/new-blok/contract/observe":
		default:
			t.Errorf("observe/slo links %s", path)
		}
	}
	std, err := exec.Command(gotool, "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range strings.Fields(string(std)) {
		if path == "net" || path == "net/http" || path == "crypto/tls" {
			t.Errorf("observe/slo links %s", path)
		}
	}
}

// TestNoOptInFootprint: the packages that report operational state (the
// deployment endpoints, the worker queue census, the cluster census, worker
// availability) link no telemetry SDK, and the root module requires none, so
// an application that does not select observe/otel pays nothing for it
// (ADR 0020's footprint rule, extended by ADR 0022).
func TestNoOptInFootprint(t *testing.T) {
	gotool := filepath.Join(runtime.GOROOT(), "bin", "go")
	root := filepath.Join("..", "..")
	for _, target := range []string{"./app/deploy", "./trigger/worker", "./internal/cluster", "./runtime/worker", "./observe/slo", "./examples/monitoring"} {
		command := exec.Command(gotool, "list", "-deps", "-test", target)
		command.Dir = root
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("go list %s: %v\n%s", target, err, output)
		}
		for _, path := range strings.Fields(string(output)) {
			if strings.HasPrefix(path, "go.opentelemetry.io/") || strings.HasPrefix(path, "github.com/well-prado/new-blok/observe/otel") || strings.HasPrefix(path, "github.com/prometheus/") {
				t.Errorf("%s links %s", target, path)
			}
		}
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"opentelemetry", "prometheus"} {
		if strings.Contains(string(mod), forbidden) {
			t.Errorf("the root go.mod requires %s", forbidden)
		}
	}
}

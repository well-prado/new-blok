// Package monitoring holds the vendor-neutral monitoring examples of ADR 0022
// and the tests that validate them: the rule files parse, read only
// catalogued series, and raise exactly the predeclared alerts on the
// recorded scenario fixtures.
package monitoring

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/observe/slo"
)

func loadRules(t *testing.T) promrule.RuleFile {
	t.Helper()
	var file promrule.RuleFile
	for _, name := range []string{"recording.rules.yaml", "alerting.rules.yaml"} {
		data, err := os.ReadFile(filepath.Join("prometheus", name))
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := promrule.ParseRules(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		file.Groups = append(file.Groups, parsed.Groups...)
	}
	return file
}

// series returns every Prometheus series name the catalogue exposes,
// mapped to its metric.
func series() map[string]slo.Metric {
	out := map[string]slo.Metric{}
	for _, m := range slo.Catalogue() {
		if m.Kind == slo.Histogram {
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				out[m.Prometheus+suffix] = m
			}
			continue
		}
		out[m.Prometheus] = m
	}
	return out
}

// TestRulesReadOnlyCataloguedSeries: every rule reads catalogued series or
// a recording rule defined before it; every alert has a paging severity and
// a class; every required signal is covered.
func TestRulesReadOnlyCataloguedSeries(t *testing.T) {
	file := loadRules(t)
	catalogue := series()
	recorded := map[string]bool{}
	alerts := map[string]bool{}
	signals := map[string]bool{}
	for _, rule := range file.Rules() {
		for _, name := range promrule.MetricNames(rule.Parsed()) {
			switch m, ok := catalogue[name]; {
			case ok:
				signals[m.Signal] = true
			case recorded[name]:
			case name == "up": // written by the scraper itself for every target
			default:
				t.Errorf("rule %s%s reads %s, which is neither catalogued nor recorded earlier", rule.Record, rule.Alert, name)
			}
		}
		if rule.Record != "" {
			if strings.Count(rule.Record, ":") != 2 || recorded[rule.Record] {
				t.Errorf("recording rule %s must be a unique level:metric:operation name", rule.Record)
			}
			recorded[rule.Record] = true
			continue
		}
		if alerts[rule.Alert] {
			t.Errorf("duplicate alert %s", rule.Alert)
		}
		alerts[rule.Alert] = true
		switch rule.Labels["severity"] {
		case "page", "warn", "ticket":
		default:
			t.Errorf("alert %s severity %q", rule.Alert, rule.Labels["severity"])
		}
		if rule.Labels["blok_alert_class"] == "" || rule.Annotations["summary"] == "" {
			t.Errorf("alert %s needs blok_alert_class and a summary", rule.Alert)
		}
	}
	for _, signal := range []string{"readiness", "admission_saturation", "queue_depth", "latency", "errors", "uncertainty", "timer_lag", "worker_availability", "storage_growth", "exporter_loss"} {
		if !signals[signal] {
			t.Errorf("no rule reads a %s metric", signal)
		}
	}
}

// TestAlertSemantics pins what pages: stalled, an unowned partition, an
// unavailable worker, a not-ready deployment and timer lag page; uncertain
// outcomes and telemetry loss never do; nothing reads waiting work.
func TestAlertSemantics(t *testing.T) {
	file := loadRules(t)
	want := map[string]string{
		"BlokRunsStalled": "page", "BlokPartitionUnowned": "page", "BlokWorkerUnavailable": "page", "BlokNotReady": "page", "BlokTimerLag": "page",
		"BlokUncertainOutcomes": "ticket", "BlokTelemetryLoss": "ticket", "BlokOperationalSamplingFailing": "ticket", "BlokRunErrorRatioHigh": "warn",
		"BlokOperationalSourceStale": "page", "BlokOperationalSourceVanished": "page", "BlokTargetDown": "page", "BlokTargetAbsent": "page", "BlokStallWindowTooShort": "ticket",
	}
	seen := map[string]bool{}
	for _, rule := range file.Rules() {
		seen[rule.Alert] = true
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("alert %s is missing", name)
		}
	}
	for _, rule := range file.Rules() {
		if rule.Alert != "" {
			if severity, ok := want[rule.Alert]; ok && rule.Labels["severity"] != severity {
				t.Errorf("%s severity %s, want %s", rule.Alert, rule.Labels["severity"], severity)
			}
			if strings.Contains(rule.Expr, "waiting") {
				t.Errorf("alert %s reads waiting work", rule.Alert)
			}
		}
	}
}

func loadScenarios(t *testing.T) promrule.Scenarios {
	t.Helper()
	s, err := promrule.LoadScenarios(filepath.Join("testdata", "scenarios.json"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestAlertsOnRecordedScenarios evaluates every rule over each recorded
// scenario: the recording must still satisfy its predeclared deltas, every
// alert the scenario declares must fire, every alert it declares silent must
// never fire, and no undeclared alert may page.
func TestAlertsOnRecordedScenarios(t *testing.T) {
	file := loadRules(t)
	scenarios := loadScenarios(t)
	severity := map[string]string{}
	for _, rule := range file.Rules() {
		if rule.Alert != "" {
			severity[rule.Alert] = rule.Labels["severity"]
		}
	}
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	timeline := scenarios.Timeline
	end := start.Add(time.Duration(timeline.BaselineMinutes+timeline.FaultMinutes) * time.Minute)
	for _, scenario := range scenarios.Scenarios {
		t.Run(scenario.ID, func(t *testing.T) {
			recording, err := promrule.ReadRecording(filepath.Join("testdata", "recorded", scenario.ID+".prom"))
			if err != nil {
				t.Fatal(err)
			}
			before, err := promrule.ParseText(strings.NewReader(recording.Before))
			if err != nil {
				t.Fatal(err)
			}
			after, err := promrule.ParseText(strings.NewReader(recording.After))
			if err != nil {
				t.Fatal(err)
			}
			if err := scenario.Check(before, after); err != nil {
				t.Fatalf("recording no longer matches its predeclared deltas: %v", err)
			}
			scrapes, err := promrule.Replay(recording, scenario, promrule.Labels(scenario.Target), timeline, start)
			if err != nil {
				t.Fatal(err)
			}
			store := promrule.NewStore()
			if err := store.Load(scrapes); err != nil {
				t.Fatal(err)
			}
			result, err := store.Evaluate(file, start, end, time.Duration(timeline.EvaluateSeconds)*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			fired := result.FiredAlerts()
			for _, name := range scenario.Fires {
				if _, ok := severity[name]; !ok {
					t.Fatalf("scenario expects unknown alert %s", name)
				}
				if !fired[name] {
					t.Errorf("%s did not fire (pending: %v)", name, result.Pending[name])
				}
			}
			for _, name := range scenario.Silent {
				if _, ok := severity[name]; !ok {
					t.Fatalf("scenario silences unknown alert %s", name)
				}
				if fired[name] {
					t.Errorf("%s fired but the scenario declares it silent", name)
				}
			}
			paged := false
			for name := range fired {
				if severity[name] == "page" {
					paged = true
					if !contains(scenario.Fires, name) {
						t.Errorf("undeclared page %s", name)
					}
				}
			}
			if scenario.MustPage && !paged {
				t.Errorf("no page fired; this scenario must wake someone")
			}
			for _, f := range result.Fired {
				// The first fault scrape is at the end of the baseline.
				if f.FiredAt.Before(start.Add(time.Duration(timeline.BaselineMinutes) * time.Minute)) {
					t.Errorf("%s fired during the baseline, before the fault", f.Alert)
				}
			}
			t.Logf("fired %v", keys(fired))
		})
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

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestDashboardQueriesAreCatalogued: every dashboard query parses under the
// rule evaluator's subset and reads only catalogued or recorded series.
func TestDashboardQueriesAreCatalogued(t *testing.T) {
	data, err := os.ReadFile("dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatal(err)
	}
	catalogue := series()
	recorded := map[string]bool{}
	for _, rule := range loadRules(t).Rules() {
		if rule.Record != "" {
			recorded[rule.Record] = true
		}
	}
	queries := 0
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			expr, err := promrule.Parse(target.Expr)
			if err != nil {
				t.Errorf("panel %q: %v", panel.Title, err)
				continue
			}
			queries++
			for _, name := range promrule.MetricNames(expr) {
				if _, ok := catalogue[name]; !ok && !recorded[name] && name != "up" {
					t.Errorf("panel %q reads %s", panel.Title, name)
				}
			}
		}
	}
	if queries < 10 {
		t.Fatalf("dashboard has %d queries", queries)
	}
}

func syntheticScrapes(start time.Time, target promrule.Labels, minutes int, at func(minute float64) []promrule.ExpositionSample) []promrule.Scrape {
	var scrapes []promrule.Scrape
	for i := 0; i <= minutes*4; i++ {
		scrapes = append(scrapes, promrule.Scrape{At: start.Add(time.Duration(i) * 15 * time.Second), Target: target, Samples: at(float64(i) / 4)})
	}
	return scrapes
}

func series1(name string, value float64, labels ...string) promrule.ExpositionSample {
	l := promrule.Labels{"__name__": name}
	for i := 0; i+1 < len(labels); i += 2 {
		l[labels[i]] = labels[i+1]
	}
	return promrule.ExpositionSample{Labels: l, Value: value}
}

// TestUncertainOutcomesEachClauseAlone: every input of BlokUncertainOutcomes
// raises it on its own, so dropping any clause is caught: a run or an
// external call that becomes uncertain on a series created at zero
// (increase), one whose series is born at one (new series), and an uncertain
// item in a census.
func TestUncertainOutcomesEachClauseAlone(t *testing.T) {
	file := loadRules(t)
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	target := promrule.Labels{"job": "blok-app", "instance": "app-0"}
	run := func(outcome string, v float64) promrule.ExpositionSample {
		return series1("blok_runs_total", v, "blok_workflow", "shop/order", "blok_outcome", outcome)
	}
	call := func(outcome string, v float64) promrule.ExpositionSample {
		return series1("blok_external_calls_total", v, "blok_workflow", "shop/order", "blok_step", "charge", "blok_outcome", outcome)
	}
	cases := map[string]func(m float64) []promrule.ExpositionSample{
		"run on a zero series": func(m float64) []promrule.ExpositionSample {
			return []promrule.ExpositionSample{run("completed", 10), run("uncertain", boolFloat(m >= 20))}
		},
		"run born at one": func(m float64) []promrule.ExpositionSample {
			out := []promrule.ExpositionSample{run("completed", 10)}
			if m >= 20 {
				out = append(out, run("uncertain", 1))
			}
			return out
		},
		"external call on a zero series": func(m float64) []promrule.ExpositionSample {
			return []promrule.ExpositionSample{call("completed", 10), call("uncertain", boolFloat(m >= 20))}
		},
		"external call born at one": func(m float64) []promrule.ExpositionSample {
			out := []promrule.ExpositionSample{call("completed", 10)}
			if m >= 20 {
				out = append(out, call("uncertain", 1))
			}
			return out
		},
		"census": func(m float64) []promrule.ExpositionSample {
			return []promrule.ExpositionSample{series1("blok_work_items", boolFloat(m >= 20), "blok_source", "journal", "blok_liveness", "uncertain")}
		},
	}
	for name, at := range cases {
		t.Run(name, func(t *testing.T) {
			store := promrule.NewStore()
			if err := store.Load(syntheticScrapes(start, target, 40, at)); err != nil {
				t.Fatal(err)
			}
			result, err := store.Evaluate(file, start, start.Add(40*time.Minute), 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			fired := result.FiredAlerts()
			if !fired["BlokUncertainOutcomes"] {
				t.Fatalf("one uncertain outcome (%s) was not ticketed", name)
			}
			if fired["BlokRunErrorRatioHigh"] || fired["BlokRunsStalled"] {
				t.Fatalf("uncertain counted as an error or a stall: %v", fired)
			}
			for _, f := range result.Fired {
				if f.Alert == "BlokUncertainOutcomes" && f.FiredAt.Before(start.Add(20*time.Minute)) {
					t.Fatalf("fired before the outcome: %+v", f)
				}
			}
		})
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// TestTargetAbsentPagesWhenNoBlokTargetExists: a Prometheus that scrapes no
// blok target at all pages after five minutes; one that scrapes a healthy
// target does not.
func TestTargetAbsentPagesWhenNoBlokTargetExists(t *testing.T) {
	file := loadRules(t)
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		job   string
		pages bool
	}{{"node-exporter", true}, {"blok-app", false}} {
		store := promrule.NewStore()
		if err := store.Load(syntheticScrapes(start, promrule.Labels{"job": c.job, "instance": "x"}, 20, func(float64) []promrule.ExpositionSample { return nil })); err != nil {
			t.Fatal(err)
		}
		result, err := store.Evaluate(file, start, start.Add(20*time.Minute), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if result.FiredAlerts()["BlokTargetAbsent"] != c.pages {
			t.Fatalf("job %s: BlokTargetAbsent fired=%v, want %v", c.job, result.FiredAlerts()["BlokTargetAbsent"], c.pages)
		}
	}
}

// TestStallWindowTooShortTickets: a census whose normal takeover is longer
// than the 2m stall window (a cluster with a long owner TTL) is ticketed so
// the stall alerts' for can be raised; a short one is not.
func TestStallWindowTooShortTickets(t *testing.T) {
	file := loadRules(t)
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		takeover float64
		tickets  bool
	}{{300.25, true}, {5.25, false}} {
		store := promrule.NewStore()
		if err := store.Load(syntheticScrapes(start, promrule.Labels{"job": "blok-cluster", "instance": "r0"}, 10, func(float64) []promrule.ExpositionSample {
			return []promrule.ExpositionSample{series1("blok_census_takeover_seconds", c.takeover, "blok_source", "cluster")}
		})); err != nil {
			t.Fatal(err)
		}
		result, err := store.Evaluate(file, start, start.Add(10*time.Minute), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if result.FiredAlerts()["BlokStallWindowTooShort"] != c.tickets {
			t.Fatalf("takeover %vs: ticket=%v, want %v", c.takeover, result.FiredAlerts()["BlokStallWindowTooShort"], c.tickets)
		}
	}
}

// TestNormalTakeoverDoesNotPage: a partition unowned for one minute while a
// successor takes over, with its running run stalled meanwhile, is a normal
// takeover inside the 2m stall window and pages nobody.
func TestNormalTakeoverDoesNotPage(t *testing.T) {
	file := loadRules(t)
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	store := promrule.NewStore()
	if err := store.Load(syntheticScrapes(start, promrule.Labels{"job": "blok-cluster", "instance": "r0"}, 30, func(m float64) []promrule.ExpositionSample {
		lost := boolFloat(m >= 10 && m < 11)
		return []promrule.ExpositionSample{
			series1("blok_partitions", 8-lost, "blok_owned", "true"), series1("blok_partitions", lost, "blok_owned", "false"),
			series1("blok_work_items", lost, "blok_source", "cluster", "blok_liveness", "stalled"),
			series1("blok_source_up", 1, "blok_source", "cluster", "blok_pages", "true"), series1("blok_source_age_seconds", 0, "blok_source", "cluster", "blok_pages", "true"),
		}
	})); err != nil {
		t.Fatal(err)
	}
	result, err := store.Evaluate(file, start, start.Add(30*time.Minute), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if fired := result.FiredAlerts(); len(fired) != 0 {
		t.Fatalf("a one-minute takeover paged: %v", keys(fired))
	}
	if !result.Pending["BlokRunsStalled"] || !result.Pending["BlokPartitionUnowned"] {
		t.Fatalf("the takeover was not even pending: %+v", result.Pending)
	}
}

package promrule

import (
	"math"
	"strings"
	"testing"
	"time"
)

var epoch = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func at(seconds int) time.Time { return epoch.Add(time.Duration(seconds) * time.Second) }

func mustStore(t *testing.T, add func(*Store)) *Store {
	t.Helper()
	s := NewStore()
	add(s)
	return s
}

func eval1(t *testing.T, s *Store, query string, when time.Time) Vector {
	t.Helper()
	expr, err := Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	vector, _, _, err := s.Eval(expr, when)
	if err != nil {
		t.Fatalf("eval %q: %v", query, err)
	}
	return vector
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// A counter growing by one per second, scraped every 15s: Prometheus'
// extrapolation makes rate exactly 1 over a window whose first sample is one
// interval after its start, and increase exactly 300.
func TestRateAndIncreaseFollowPrometheusExtrapolation(t *testing.T) {
	s := mustStore(t, func(s *Store) {
		for i := 0; i <= 40; i++ {
			_ = s.Add(Labels{"__name__": "c_total", "job": "a"}, at(i*15), float64(i*15))
		}
	})
	if v := eval1(t, s, `rate(c_total[5m])`, at(600)); len(v) != 1 || !near(v[0].V, 1) || v[0].Labels["__name__"] != "" {
		t.Fatalf("rate = %+v, want 1 without a metric name", v)
	}
	if v := eval1(t, s, `increase(c_total[5m])`, at(600)); len(v) != 1 || !near(v[0].V, 300) {
		t.Fatalf("increase = %+v, want 300", v)
	}
	// One sample in the window: no result, as in Prometheus.
	if v := eval1(t, s, `rate(c_total[10s])`, at(600)); len(v) != 0 {
		t.Fatalf("rate over one sample = %+v", v)
	}
}

// A counter reset adds the pre-reset value, so the increase stays positive.
func TestCounterResetIsCompensated(t *testing.T) {
	s := mustStore(t, func(s *Store) {
		for i, v := range []float64{0, 10, 20, 5, 15} {
			_ = s.Add(Labels{"__name__": "c_total"}, at(i*60), v)
		}
	})
	// raw increase 15 - 0 + 20 = 35 over 240s sampled; window 300s with the
	// first sample at the window start + 60s > 1.1 * 60s average? no: equal
	// to the average, below the threshold, so extrapolation reaches the
	// start only as far as the zero point (first value 0): factor 240/240.
	v := eval1(t, s, `increase(c_total[5m])`, at(240))
	if len(v) != 1 || !near(v[0].V, 35) {
		t.Fatalf("increase across reset = %+v, want 35", v)
	}
}

func TestHistogramQuantileInterpolatesWithinBuckets(t *testing.T) {
	s := mustStore(t, func(s *Store) {
		for le, count := range map[string]float64{"0.1": 0, "0.5": 50, "1": 90, "+Inf": 100} {
			_ = s.Add(Labels{"__name__": "d_bucket", "le": le, "step": "a"}, at(0), count)
		}
	})
	for q, want := range map[string]float64{"0.5": 0.5, "0.7": 0.75, "0.99": 1} {
		v := eval1(t, s, `histogram_quantile(`+q+`, d_bucket)`, at(0))
		if len(v) != 1 || !near(v[0].V, want) || v[0].Labels["step"] != "a" || v[0].Labels["le"] != "" {
			t.Fatalf("q%s = %+v, want %v", q, v, want)
		}
	}
	empty := mustStore(t, func(s *Store) {
		_ = s.Add(Labels{"__name__": "d_bucket", "le": "1"}, at(0), 0)
		_ = s.Add(Labels{"__name__": "d_bucket", "le": "+Inf"}, at(0), 0)
	})
	if v := eval1(t, empty, `histogram_quantile(0.99, d_bucket)`, at(0)); len(v) != 1 || !math.IsNaN(v[0].V) {
		t.Fatalf("empty histogram quantile = %+v, want NaN", v)
	}
}

func TestMatchingAggregationAndSetOperators(t *testing.T) {
	s := mustStore(t, func(s *Store) {
		_ = s.Add(Labels{"__name__": "active", "job": "a", "instance": "1"}, at(0), 9)
		_ = s.Add(Labels{"__name__": "capacity", "job": "a", "instance": "1"}, at(0), 10)
		_ = s.Add(Labels{"__name__": "ready", "job": "a", "instance": "1"}, at(0), 0)
		_ = s.Add(Labels{"__name__": "draining", "job": "a", "instance": "1"}, at(0), 1)
		_ = s.Add(Labels{"__name__": "ready", "job": "b", "instance": "2"}, at(0), 0)
		_ = s.Add(Labels{"__name__": "draining", "job": "b", "instance": "2"}, at(0), 0)
		_ = s.Add(Labels{"__name__": "items", "job": "a", "src": "q", "liveness": "stalled"}, at(0), 2)
		_ = s.Add(Labels{"__name__": "items", "job": "a", "src": "q", "liveness": "waiting"}, at(0), 5)
	})
	if v := eval1(t, s, `max by (job) (active) / max by (job) (capacity > 0)`, at(0)); len(v) != 1 || !near(v[0].V, 0.9) {
		t.Fatalf("ratio = %+v", v)
	}
	notReady := eval1(t, s, `max by (job, instance) (ready) == 0 unless on (job, instance) max by (job, instance) (draining) == 1`, at(0))
	if len(notReady) != 1 || notReady[0].Labels["job"] != "b" {
		t.Fatalf("not ready unless draining = %+v, want only job b", notReady)
	}
	if v := eval1(t, s, `max by (job, src) (items{liveness="stalled"}) > 0`, at(0)); len(v) != 1 || v[0].V != 2 {
		t.Fatalf("stalled = %+v", v)
	}
	if v := eval1(t, s, `items{liveness=~"stall.*"}`, at(0)); len(v) != 1 {
		t.Fatalf("regex matcher = %+v", v)
	}
	if v := eval1(t, s, `sum without (liveness) (items)`, at(0)); len(v) != 1 || v[0].V != 7 {
		t.Fatalf("sum without = %+v", v)
	}
	// The instant selector looks back five minutes and no further.
	if v := eval1(t, s, `active`, at(299)); len(v) != 1 {
		t.Fatalf("lookback inside 5m = %+v", v)
	}
	if v := eval1(t, s, `active`, at(301)); len(v) != 0 {
		t.Fatalf("lookback past 5m = %+v", v)
	}
}

func TestDerivAndPredictLinearUseLeastSquares(t *testing.T) {
	s := mustStore(t, func(s *Store) {
		for i := 0; i <= 10; i++ {
			_ = s.Add(Labels{"__name__": "used_bytes"}, at(i*60), 1000+float64(i*60)*2)
		}
	})
	if v := eval1(t, s, `deriv(used_bytes[10m])`, at(600)); len(v) != 1 || !near(v[0].V, 2) {
		t.Fatalf("deriv = %+v, want 2 bytes/s", v)
	}
	if v := eval1(t, s, `predict_linear(used_bytes[10m], 3600)`, at(600)); len(v) != 1 || !near(v[0].V, 1000+1200+7200) {
		t.Fatalf("predict_linear = %+v, want %v", v, 1000+1200+7200)
	}
}

func TestAlertFiresOnlyAfterForAndResetsWhenAbsent(t *testing.T) {
	rules, err := ParseRules([]byte(`groups:
  - name: g
    rules:
      - alert: Stalled
        expr: x > 0
        for: 2m
        labels:
          severity: page
      - alert: Flapping
        expr: y > 0
        for: 2m
`))
	if err != nil {
		t.Fatal(err)
	}
	s := mustStore(t, func(s *Store) {
		for i := 0; i <= 10; i++ {
			v := 0.0
			if i >= 2 {
				v = 1
			}
			_ = s.Add(Labels{"__name__": "x"}, at(i*30), v)
			_ = s.Add(Labels{"__name__": "y"}, at(i*30), float64(i%2))
		}
	})
	result, err := s.Evaluate(rules, at(0), at(300), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Fired) != 1 || result.Fired[0].Alert != "Stalled" || !result.Fired[0].ActiveAt.Equal(at(60)) || !result.Fired[0].FiredAt.Equal(at(180)) || result.Fired[0].Labels["severity"] != "page" {
		t.Fatalf("fired = %+v, want Stalled active at 60s firing at 180s", result.Fired)
	}
	if !result.Pending["Flapping"] || result.FiredAlerts()["Flapping"] {
		t.Fatalf("a condition that never holds for 2m must stay pending: %+v", result)
	}
}

func TestRecordingRuleOutputIsVisibleToLaterRules(t *testing.T) {
	rules, err := ParseRules([]byte(`groups:
  - name: g
    interval: 30s
    rules:
      - record: job:x:sum
        expr: >-
          sum by (job)
          (x)
        labels:
          tier: slo
      - alert: High
        expr: |
          job:x:sum{tier="slo"} > 3
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := rules.Groups[0].Rules[0].Expr; got != "sum by (job) (x)" {
		t.Fatalf("folded block scalar = %q", got)
	}
	s := mustStore(t, func(s *Store) {
		_ = s.Add(Labels{"__name__": "x", "job": "a", "i": "1"}, at(0), 2)
		_ = s.Add(Labels{"__name__": "x", "job": "a", "i": "2"}, at(0), 2)
	})
	result, err := s.Evaluate(rules, at(0), at(0), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.FiringAtEnd["High"] {
		t.Fatalf("recorded series not visible: %+v", result)
	}
}

func TestUnsupportedSyntaxIsRefused(t *testing.T) {
	for _, query := range []string{`a * on (x) group_left b`, `rate(a[5m] offset 1m)`, `quantile_over_time(0.9, a[5m])`, `a > 1 >`, `{}`} {
		if _, err := Parse(query); err == nil {
			t.Errorf("Parse(%q) accepted unsupported syntax", query)
		}
	}
	for _, file := range []string{
		"groups:\n  - name: g\n    rules:\n      - alert: A\n        expr: a\n        keep_firing_for: 5m\n",
		"groups:\n  - name: g\n    rules:\n      - record: r\n        alert: A\n        expr: a\n",
		"groups:\n  - name: g\n    rules: [ {record: r} ]\n",
		"groups:\n\t- name: g\n",
	} {
		if _, err := ParseRules([]byte(file)); err == nil {
			t.Errorf("ParseRules accepted %q", file)
		}
	}
}

func TestParseTextExposition(t *testing.T) {
	samples, err := ParseText(strings.NewReader(`# HELP blok_ready 1 when ready
# TYPE blok_ready gauge
blok_ready 1
blok_work_items{blok_source="orders",blok_liveness="stalled"} 2
esc{a="x\"y\\z"} +Inf 1700000000000
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 3 || samples[1].Labels["blok_liveness"] != "stalled" || samples[1].Value != 2 || samples[2].Labels["a"] != `x"y\z` || !math.IsInf(samples[2].Value, 1) {
		t.Fatalf("samples = %+v", samples)
	}
	if _, err := ParseText(strings.NewReader("bad{a=1} 2\n")); err == nil {
		t.Fatal("malformed labels accepted")
	}
}

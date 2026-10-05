package promrule

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// Expectation is one predeclared metric outcome of a scenario. Exactly one
// of Value (the value after the fault), Delta (after minus before) or Min (a
// lower bound on the value after) is set. Labels select every sample that
// carries them, and the selected samples are summed, so a collector's
// added labels (job, instance, otel_scope_*) do not matter.
type Expectation struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  *float64          `json:"value,omitempty"`
	Delta  *float64          `json:"delta,omitempty"`
	Min    *float64          `json:"min,omitempty"`
}

// Scenario is one synthetic operational scenario.
type Scenario struct {
	ID       string            `json:"id"`
	Producer string            `json:"producer"`
	Fault    string            `json:"fault"`
	Target   map[string]string `json:"target"`
	Expect   []Expectation     `json:"expect"`
	Fires    []string          `json:"fires"`
	Silent   []string          `json:"silent"`
}

// Timeline is how a recording is replayed for rule evaluation.
type Timeline struct {
	ScrapeSeconds   int `json:"scrapeSeconds"`
	BaselineMinutes int `json:"baselineMinutes"`
	FaultMinutes    int `json:"faultMinutes"`
	EvaluateSeconds int `json:"evaluateSeconds"`
}

// Scenarios is the predeclared scenario file.
type Scenarios struct {
	SchemaVersion int        `json:"schemaVersion"`
	Roadmap       string     `json:"roadmap"`
	Purpose       string     `json:"purpose"`
	Timeline      Timeline   `json:"timeline"`
	Scenarios     []Scenario `json:"scenarios"`
}

// LoadScenarios reads and checks a scenario file.
func LoadScenarios(path string) (Scenarios, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenarios{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var s Scenarios
	if err := decoder.Decode(&s); err != nil {
		return Scenarios{}, err
	}
	if s.SchemaVersion != 1 || s.Timeline.ScrapeSeconds < 1 || s.Timeline.EvaluateSeconds < 1 || s.Timeline.FaultMinutes < 1 {
		return Scenarios{}, fmt.Errorf("scenario file schema or timeline invalid")
	}
	seen := map[string]bool{}
	for _, sc := range s.Scenarios {
		if sc.ID == "" || seen[sc.ID] || len(sc.Expect) == 0 {
			return Scenarios{}, fmt.Errorf("scenario %q: missing or duplicate id, or no expectations", sc.ID)
		}
		seen[sc.ID] = true
		for _, e := range sc.Expect {
			set := 0
			for _, p := range []*float64{e.Value, e.Delta, e.Min} {
				if p != nil {
					set++
				}
			}
			if e.Metric == "" || set != 1 {
				return Scenarios{}, fmt.Errorf("scenario %s: expectation %+v needs a metric and exactly one of value, delta, min", sc.ID, e)
			}
		}
	}
	return s, nil
}

// Find returns the scenario with id.
func (s Scenarios) Find(id string) (Scenario, bool) {
	for _, sc := range s.Scenarios {
		if sc.ID == id {
			return sc, true
		}
	}
	return Scenario{}, false
}

func selectSum(samples []ExpositionSample, metric string, labels map[string]string) (float64, bool) {
	total, found := 0.0, false
	for _, s := range samples {
		if s.Labels["__name__"] != metric {
			continue
		}
		ok := true
		for k, v := range labels {
			if s.Labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			total += s.Value
			found = true
		}
	}
	return total, found
}

// Check compares a scenario's expectations with the expositions recorded
// before and after its fault. A metric name absent after the fault is always
// an error (a misspelled name must not pass as zero). A value or bound needs
// a selected series; for a delta an unselected series counts as zero, since
// a counter that was never incremented may not exist yet.
func (sc Scenario) Check(before, after []ExpositionSample) error {
	var errs []error
	for _, e := range sc.Expect {
		if _, named := selectSum(after, e.Metric, nil); !named {
			errs = append(errs, fmt.Errorf("%s: no %s series after the fault", sc.ID, e.Metric))
			continue
		}
		got, found := selectSum(after, e.Metric, e.Labels)
		if !found && e.Delta == nil {
			errs = append(errs, fmt.Errorf("%s: %s%v absent after the fault", sc.ID, e.Metric, e.Labels))
			continue
		}
		switch {
		case e.Value != nil && got != *e.Value:
			errs = append(errs, fmt.Errorf("%s: %s%v = %v, want %v", sc.ID, e.Metric, e.Labels, got, *e.Value))
		case e.Min != nil && !(got >= *e.Min):
			errs = append(errs, fmt.Errorf("%s: %s%v = %v, want at least %v", sc.ID, e.Metric, e.Labels, got, *e.Min))
		case e.Delta != nil:
			was, _ := selectSum(before, e.Metric, e.Labels)
			if got-was != *e.Delta {
				errs = append(errs, fmt.Errorf("%s: %s%v delta %v, want %v", sc.ID, e.Metric, e.Labels, got-was, *e.Delta))
			}
		}
	}
	return errors.Join(errs...)
}

// Recording is the exposition a producer observed before and after a
// scenario's fault.
type Recording struct {
	Scenario string
	Source   string
	Before   string
	After    string
}

const recordingHeader = "# blok-fixture v1"

// WriteRecording writes r in the recorded-fixture format: a header naming
// the scenario and its source, then the two expositions after "# phase:"
// markers.
func WriteRecording(path string, r Recording) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n# scenario: %s\n# source: %s\n# phase: before\n%s", recordingHeader, r.Scenario, r.Source, ensureNewline(r.Before))
	fmt.Fprintf(&b, "# phase: after\n%s", ensureNewline(r.After))
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func ensureNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

// ReadRecording reads a recorded fixture.
func ReadRecording(path string) (Recording, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Recording{}, err
	}
	var r Recording
	var phase string
	var before, after strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	first := true
	for scanner.Scan() {
		line := scanner.Text()
		if first {
			if line != recordingHeader {
				return Recording{}, fmt.Errorf("%s: not a blok fixture", path)
			}
			first = false
			continue
		}
		switch {
		case strings.HasPrefix(line, "# scenario: "):
			r.Scenario = strings.TrimPrefix(line, "# scenario: ")
		case strings.HasPrefix(line, "# source: "):
			r.Source = strings.TrimPrefix(line, "# source: ")
		case strings.HasPrefix(line, "# phase: "):
			phase = strings.TrimPrefix(line, "# phase: ")
		case phase == "before":
			before.WriteString(line + "\n")
		case phase == "after":
			after.WriteString(line + "\n")
		}
	}
	r.Before, r.After = before.String(), after.String()
	if r.Scenario == "" || r.After == "" {
		return Recording{}, fmt.Errorf("%s: incomplete fixture", path)
	}
	return r, scanner.Err()
}

// counterFamilies returns the series names the TYPE comments of text declare
// cumulative: counters and the _bucket, _sum and _count series of
// histograms and summaries. An untyped name ending in _total is a counter.
func counterFamilies(text string) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 4 && fields[0] == "#" && fields[1] == "TYPE" {
			switch fields[3] {
			case "counter":
				out[fields[2]] = true
			case "histogram", "summary":
				for _, suffix := range []string{"_bucket", "_sum", "_count"} {
					out[fields[2]+suffix] = true
				}
			}
		}
	}
	return out
}

// Replay turns a recording into scrapes: the "before" exposition held for
// the baseline, then the measured interval replayed as a steady state for
// the fault window. At the k-th fault scrape a cumulative series is
// before + k*(after-before) (so its rate stays what was measured) and a gauge
// is its after value. Series only present after the fault start from zero.
func Replay(r Recording, target Labels, t Timeline, start time.Time) ([]Scrape, error) {
	before, err := ParseText(strings.NewReader(r.Before))
	if err != nil {
		return nil, fmt.Errorf("before: %w", err)
	}
	after, err := ParseText(strings.NewReader(r.After))
	if err != nil {
		return nil, fmt.Errorf("after: %w", err)
	}
	cumulative := counterFamilies(r.Before + "\n" + r.After)
	isCounter := func(name string) bool { return cumulative[name] || strings.HasSuffix(name, "_total") }
	was := map[string]float64{}
	for _, s := range before {
		was[s.Labels.key()] = s.Value
	}
	step := time.Duration(t.ScrapeSeconds) * time.Second
	var scrapes []Scrape
	at := start
	for i := 0; i < t.BaselineMinutes*60/t.ScrapeSeconds; i++ {
		scrapes = append(scrapes, Scrape{At: at, Target: target, Samples: before})
		at = at.Add(step)
	}
	for k := 1; k <= t.FaultMinutes*60/t.ScrapeSeconds; k++ {
		samples := make([]ExpositionSample, 0, len(after))
		for _, s := range after {
			v := s.Value
			if isCounter(s.Labels["__name__"]) && !math.IsInf(v, 0) && !math.IsNaN(v) {
				base := was[s.Labels.key()]
				v = base + float64(k)*(s.Value-base)
			}
			samples = append(samples, ExpositionSample{Labels: s.Labels, Value: v})
		}
		scrapes = append(scrapes, Scrape{At: at, Target: target, Samples: samples})
		at = at.Add(step)
	}
	return scrapes, nil
}

// MetricNames returns every metric selector name an expression reads.
func MetricNames(expr Expr) []string {
	seen := map[string]bool{}
	var walk func(Expr)
	walk = func(e Expr) {
		switch v := e.(type) {
		case selectorExpr:
			if v.name != "" {
				seen[v.name] = true
			}
		case callExpr:
			for _, a := range v.args {
				walk(a)
			}
		case aggregateExpr:
			walk(v.inner)
		case binaryExpr:
			walk(v.left)
			walk(v.right)
		case parenExpr:
			walk(v.inner)
		}
	}
	walk(expr)
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Parsed returns the parsed expression of a rule.
func (r Rule) Parsed() Expr { return r.parsed }

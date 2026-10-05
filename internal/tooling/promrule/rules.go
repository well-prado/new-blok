package promrule

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Rule is one recording or alerting rule.
type Rule struct {
	Record, Alert string
	Expr          string
	For           time.Duration
	Labels        map[string]string
	Annotations   map[string]string
	parsed        Expr
}

// Group is a rule group.
type Group struct {
	Name     string
	Interval time.Duration
	Rules    []Rule
}

// RuleFile is a parsed Prometheus rule file.
type RuleFile struct {
	Groups []Group
}

// ParseRules parses a Prometheus rule file written in the block-style YAML
// subset documented on parseYAML, checks the rule-file schema strictly
// (unknown keys are errors) and parses every expression.
func ParseRules(data []byte) (RuleFile, error) {
	root, err := parseYAML(string(data))
	if err != nil {
		return RuleFile{}, err
	}
	top, ok := root.(map[string]any)
	if !ok || len(top) != 1 || top["groups"] == nil {
		return RuleFile{}, fmt.Errorf("rule file must contain only groups")
	}
	groups, ok := top["groups"].([]any)
	if !ok {
		return RuleFile{}, fmt.Errorf("groups must be a list")
	}
	var file RuleFile
	names := map[string]bool{}
	for _, raw := range groups {
		g, ok := raw.(map[string]any)
		if !ok {
			return RuleFile{}, fmt.Errorf("group must be a mapping")
		}
		if err := onlyKeys(g, "name", "interval", "rules"); err != nil {
			return RuleFile{}, err
		}
		group := Group{Name: str(g["name"])}
		if group.Name == "" || names[group.Name] {
			return RuleFile{}, fmt.Errorf("group name %q missing or duplicate", group.Name)
		}
		names[group.Name] = true
		if interval := str(g["interval"]); interval != "" {
			if group.Interval, err = ParseDuration(interval); err != nil {
				return RuleFile{}, err
			}
		}
		rules, ok := g["rules"].([]any)
		if !ok || len(rules) == 0 {
			return RuleFile{}, fmt.Errorf("group %s has no rules", group.Name)
		}
		for _, rawRule := range rules {
			r, ok := rawRule.(map[string]any)
			if !ok {
				return RuleFile{}, fmt.Errorf("rule must be a mapping in %s", group.Name)
			}
			if err := onlyKeys(r, "record", "alert", "expr", "for", "labels", "annotations"); err != nil {
				return RuleFile{}, err
			}
			rule := Rule{Record: str(r["record"]), Alert: str(r["alert"]), Expr: strings.TrimSpace(str(r["expr"]))}
			if (rule.Record == "") == (rule.Alert == "") || rule.Expr == "" {
				return RuleFile{}, fmt.Errorf("rule in %s needs exactly one of record or alert, and an expr", group.Name)
			}
			if rule.Record != "" && (r["for"] != nil || r["annotations"] != nil) {
				return RuleFile{}, fmt.Errorf("recording rule %s cannot have for or annotations", rule.Record)
			}
			if f := str(r["for"]); f != "" {
				if rule.For, err = ParseDuration(f); err != nil {
					return RuleFile{}, err
				}
			}
			if rule.Labels, err = stringMap(r["labels"]); err != nil {
				return RuleFile{}, err
			}
			if rule.Annotations, err = stringMap(r["annotations"]); err != nil {
				return RuleFile{}, err
			}
			if rule.parsed, err = Parse(rule.Expr); err != nil {
				return RuleFile{}, fmt.Errorf("rule %s%s: %w", rule.Record, rule.Alert, err)
			}
			group.Rules = append(group.Rules, rule)
		}
		file.Groups = append(file.Groups, group)
	}
	return file, nil
}

func onlyKeys(m map[string]any, allowed ...string) error {
	for key := range m {
		ok := false
		for _, a := range allowed {
			ok = ok || key == a
		}
		if !ok {
			return fmt.Errorf("unknown key %q", key)
		}
	}
	return nil
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func stringMap(v any) (map[string]string, error) {
	if v == nil {
		return map[string]string{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("labels and annotations must be mappings")
	}
	out := map[string]string{}
	for k, raw := range m {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("value of %s must be a string", k)
		}
		out[k] = s
	}
	return out, nil
}

// Rules returns every rule in file order.
func (f RuleFile) Rules() []Rule {
	var out []Rule
	for _, g := range f.Groups {
		out = append(out, g.Rules...)
	}
	return out
}

// Firing is one alert instance.
type Firing struct {
	Alert    string
	Labels   Labels
	ActiveAt time.Time
	FiredAt  time.Time
}

// Evaluation is the outcome of evaluating a rule file over a time range.
type Evaluation struct {
	// Fired is every alert instance that reached firing, in order.
	Fired []Firing
	// FiringAtEnd is the set of alert names firing at the last evaluation.
	FiringAtEnd map[string]bool
	// Pending lists alert names that were active but never fired.
	Pending map[string]bool
}

// FiredAlerts returns the names of alerts that fired at any time.
func (e Evaluation) FiredAlerts() map[string]bool {
	out := map[string]bool{}
	for _, f := range e.Fired {
		out[f.Alert] = true
	}
	return out
}

// Evaluate runs every rule at each step from start to end inclusive.
// Recording rules write their samples back into the store.
func (s *Store) Evaluate(file RuleFile, start, end time.Time, step time.Duration) (Evaluation, error) {
	if step <= 0 || end.Before(start) {
		return Evaluation{}, fmt.Errorf("invalid evaluation range")
	}
	result := Evaluation{FiringAtEnd: map[string]bool{}, Pending: map[string]bool{}}
	type active struct {
		labels   Labels
		since    time.Time
		firing   bool
		lastSeen time.Time
	}
	alerts := map[string]map[string]*active{}
	for t := start; !t.After(end); t = t.Add(step) {
		for _, group := range file.Groups {
			for _, rule := range group.Rules {
				vector, scalar, isScalar, err := s.Eval(rule.parsed, t)
				if err != nil {
					return Evaluation{}, fmt.Errorf("rule %s%s at %s: %w", rule.Record, rule.Alert, t.Format(time.RFC3339), err)
				}
				if isScalar {
					vector = Vector{{Labels: Labels{}, V: scalar}}
				}
				if rule.Record != "" {
					for _, sample := range vector {
						if math.IsNaN(sample.V) {
							continue
						}
						labels := dropName(sample.Labels)
						for k, v := range rule.Labels {
							labels[k] = v
						}
						labels["__name__"] = rule.Record
						if err := s.Add(labels, t, sample.V); err != nil {
							return Evaluation{}, fmt.Errorf("record %s: %w", rule.Record, err)
						}
					}
					continue
				}
				instances := alerts[rule.Alert]
				if instances == nil {
					instances = map[string]*active{}
					alerts[rule.Alert] = instances
				}
				present := map[string]bool{}
				for _, sample := range vector {
					labels := dropName(sample.Labels)
					for k, v := range rule.Labels {
						labels[k] = v
					}
					labels["alertname"] = rule.Alert
					key := labels.key()
					present[key] = true
					a := instances[key]
					if a == nil {
						a = &active{labels: labels, since: t}
						instances[key] = a
					}
					a.lastSeen = t
					if !a.firing && t.Sub(a.since) >= rule.For {
						a.firing = true
						result.Fired = append(result.Fired, Firing{Alert: rule.Alert, Labels: labels, ActiveAt: a.since, FiredAt: t})
					}
				}
				for key, a := range instances {
					if !present[key] {
						if !a.firing {
							result.Pending[rule.Alert] = true
						}
						delete(instances, key)
					}
				}
			}
		}
	}
	for name, instances := range alerts {
		for _, a := range instances {
			if a.firing {
				result.FiringAtEnd[name] = true
			} else {
				result.Pending[name] = true
			}
		}
	}
	for name := range result.FiredAlerts() {
		delete(result.Pending, name)
	}
	return result, nil
}

// ExpositionSample is one sample of a text exposition.
type ExpositionSample struct {
	Labels Labels
	Value  float64
}

// ParseText parses the Prometheus text exposition format (0.0.4): HELP and
// TYPE comments are skipped, every other line is name{labels} value with an
// optional timestamp that is ignored.
func ParseText(r io.Reader) ([]ExpositionSample, error) {
	var out []ExpositionSample
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		sample, err := parseSampleLine(text)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		out = append(out, sample)
	}
	return out, scanner.Err()
}

func parseSampleLine(text string) (ExpositionSample, error) {
	labels := Labels{}
	i := strings.IndexAny(text, "{ ")
	if i <= 0 {
		return ExpositionSample{}, fmt.Errorf("malformed sample %q", text)
	}
	labels["__name__"] = text[:i]
	rest := text[i:]
	if rest[0] == '{' {
		j := 1
		for {
			for j < len(rest) && rest[j] == ' ' {
				j++
			}
			if j < len(rest) && rest[j] == '}' {
				j++
				break
			}
			eq := strings.IndexByte(rest[j:], '=')
			if eq < 0 || j+eq+1 >= len(rest) || rest[j+eq+1] != '"' {
				return ExpositionSample{}, fmt.Errorf("malformed labels in %q", text)
			}
			name := strings.TrimSpace(rest[j : j+eq])
			k := j + eq + 2
			var b strings.Builder
			for ; k < len(rest) && rest[k] != '"'; k++ {
				if rest[k] == '\\' && k+1 < len(rest) {
					k++
					switch rest[k] {
					case 'n':
						b.WriteByte('\n')
					default:
						b.WriteByte(rest[k])
					}
					continue
				}
				b.WriteByte(rest[k])
			}
			if k >= len(rest) {
				return ExpositionSample{}, fmt.Errorf("unterminated label value in %q", text)
			}
			labels[name] = b.String()
			j = k + 1
			if j < len(rest) && rest[j] == ',' {
				j++
			}
		}
		rest = rest[j:]
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return ExpositionSample{}, fmt.Errorf("malformed value in %q", text)
	}
	v, err := parseValue(fields[0])
	if err != nil {
		return ExpositionSample{}, err
	}
	return ExpositionSample{Labels: labels, Value: v}, nil
}

func parseValue(text string) (float64, error) {
	switch text {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(text, 64)
}

// Scrape is one recorded exposition at a time.
type Scrape struct {
	At      time.Time
	Target  Labels
	Samples []ExpositionSample
}

// Load adds scrapes in time order, adding each scrape's target labels (job,
// instance) to its samples as a Prometheus scrape would.
func (s *Store) Load(scrapes []Scrape) error {
	sort.SliceStable(scrapes, func(i, j int) bool { return scrapes[i].At.Before(scrapes[j].At) })
	for _, scrape := range scrapes {
		for _, sample := range scrape.Samples {
			labels := sample.Labels.clone()
			for k, v := range scrape.Target {
				if _, ok := labels[k]; !ok {
					labels[k] = v
				}
			}
			if err := s.Add(labels, scrape.At, sample.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

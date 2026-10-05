package promrule

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Lookback is how far back an instant selector looks for a sample.
const Lookback = 5 * time.Minute

// Labels is a label set; the metric name is the label "__name__".
type Labels map[string]string

func (l Labels) key() string {
	names := make([]string, 0, len(l))
	for name := range l {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte('\xff')
		b.WriteString(l[name])
		b.WriteByte('\xfe')
	}
	return b.String()
}

func (l Labels) clone() Labels {
	out := make(Labels, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}

// String renders l like a Prometheus series.
func (l Labels) String() string {
	names := make([]string, 0, len(l))
	for name := range l {
		if name != "__name__" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + "=" + strconv.Quote(l[name])
	}
	return l["__name__"] + "{" + strings.Join(parts, ",") + "}"
}

// Point is one sample of a series. A stale point is a staleness marker: the
// series ended at T.
type Point struct {
	T     time.Time
	V     float64
	Stale bool
}

type series struct {
	labels Labels
	points []Point
}

// Store holds series sampled over time.
type Store struct {
	series map[string]*series
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{series: map[string]*series{}} }

// Add appends one sample. Samples of a series must be added in time order.
func (s *Store) Add(labels Labels, t time.Time, v float64) error {
	return s.add(labels, Point{T: t, V: v})
}

// AddStale appends a staleness marker: from t the series has no value.
func (s *Store) AddStale(labels Labels, t time.Time) error {
	return s.add(labels, Point{T: t, V: math.NaN(), Stale: true})
}

func (s *Store) add(labels Labels, p Point) error {
	t := p.T
	if labels["__name__"] == "" {
		return fmt.Errorf("sample without a metric name")
	}
	key := labels.key()
	existing := s.series[key]
	if existing == nil {
		existing = &series{labels: labels.clone()}
		s.series[key] = existing
	}
	if n := len(existing.points); n > 0 && !t.After(existing.points[n-1].T) {
		return fmt.Errorf("out-of-order sample for %s", labels)
	}
	existing.points = append(existing.points, p)
	return nil
}

// fresh drops staleness markers from points.
func fresh(points []Point) []Point {
	out := points[:0:0]
	for _, p := range points {
		if !p.Stale {
			out = append(out, p)
		}
	}
	return out
}

// Sample is one element of an instant vector.
type Sample struct {
	Labels Labels
	V      float64
}

// Vector is an instant vector.
type Vector []Sample

type value struct {
	isScalar bool
	scalar   float64
	vector   Vector
}

func (m matcher) matches(v string) (bool, error) {
	switch m.op {
	case "=":
		return v == m.value, nil
	case "!=":
		return v != m.value, nil
	case "=~", "!~":
		re, err := regexp.Compile("^(?:" + m.value + ")$")
		if err != nil {
			return false, err
		}
		return re.MatchString(v) == (m.op == "=~"), nil
	}
	return false, fmt.Errorf("matcher %q", m.op)
}

func (s *Store) selectSeries(sel selectorExpr) ([]*series, error) {
	var out []*series
	for _, candidate := range s.series {
		if sel.name != "" && candidate.labels["__name__"] != sel.name {
			continue
		}
		ok := true
		for _, m := range sel.matchers {
			matched, err := m.matches(candidate.labels[m.name])
			if err != nil {
				return nil, err
			}
			if !matched {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, candidate)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].labels.key() < out[j].labels.key() })
	return out, nil
}

// window returns the points in (t-d, t].
func window(points []Point, t time.Time, d time.Duration) []Point {
	start := t.Add(-d)
	lo := sort.Search(len(points), func(i int) bool { return points[i].T.After(start) })
	hi := sort.Search(len(points), func(i int) bool { return points[i].T.After(t) })
	return points[lo:hi]
}

// Eval evaluates expr at t.
func (s *Store) Eval(expr Expr, t time.Time) (Vector, float64, bool, error) {
	v, err := s.eval(expr, t)
	if err != nil {
		return nil, 0, false, err
	}
	return v.vector, v.scalar, v.isScalar, nil
}

func (s *Store) eval(expr Expr, t time.Time) (value, error) {
	switch e := expr.(type) {
	case numberExpr:
		return value{isScalar: true, scalar: e.value}, nil
	case parenExpr:
		return s.eval(e.inner, t)
	case selectorExpr:
		if e.window > 0 {
			return value{}, fmt.Errorf("range selector %s outside a function", e)
		}
		matched, err := s.selectSeries(e)
		if err != nil {
			return value{}, err
		}
		var vector Vector
		for _, candidate := range matched {
			points := window(candidate.points, t.Add(-e.offset), Lookback)
			if len(points) > 0 && !points[len(points)-1].Stale {
				vector = append(vector, Sample{Labels: candidate.labels.clone(), V: points[len(points)-1].V})
			}
		}
		return value{vector: vector}, nil
	case callExpr:
		return s.call(e, t)
	case aggregateExpr:
		inner, err := s.eval(e.inner, t)
		if err != nil {
			return value{}, err
		}
		if inner.isScalar {
			return value{}, fmt.Errorf("%s over a scalar", e.op)
		}
		return value{vector: aggregate(e, inner.vector)}, nil
	case binaryExpr:
		return s.binary(e, t)
	}
	return value{}, fmt.Errorf("unsupported expression %T", expr)
}

func (s *Store) rangeArg(arg Expr) (selectorExpr, error) {
	sel, ok := arg.(selectorExpr)
	if !ok || sel.window == 0 {
		return selectorExpr{}, fmt.Errorf("expected a range selector, found %s", arg)
	}
	return sel, nil
}

func (s *Store) scalarArg(arg Expr, t time.Time) (float64, error) {
	v, err := s.eval(arg, t)
	if err != nil {
		return 0, err
	}
	if !v.isScalar {
		return 0, fmt.Errorf("expected a scalar, found %s", arg)
	}
	return v.scalar, nil
}

func dropName(l Labels) Labels {
	out := l.clone()
	delete(out, "__name__")
	return out
}

func (s *Store) call(e callExpr, t time.Time) (value, error) {
	switch e.fn {
	case "rate", "increase", "delta", "deriv", "max_over_time", "min_over_time", "predict_linear":
		sel, err := s.rangeArg(e.args[0])
		if err != nil {
			return value{}, err
		}
		horizon := 0.0
		if e.fn == "predict_linear" {
			if horizon, err = s.scalarArg(e.args[1], t); err != nil {
				return value{}, err
			}
		}
		matched, err := s.selectSeries(sel)
		if err != nil {
			return value{}, err
		}
		var vector Vector
		for _, candidate := range matched {
			at := t.Add(-sel.offset)
			points := fresh(window(candidate.points, at, sel.window))
			var result float64
			ok := true
			switch e.fn {
			case "rate":
				result, ok = extrapolated(points, at, sel.window, true, true)
			case "increase":
				result, ok = extrapolated(points, at, sel.window, true, false)
			case "delta":
				result, ok = extrapolated(points, at, sel.window, false, false)
			case "deriv":
				result, _, ok = regression(points, at)
			case "predict_linear":
				var slope, intercept float64
				slope, intercept, ok = regression(points, at)
				result = intercept + slope*horizon
			case "max_over_time", "min_over_time":
				ok = len(points) > 0
				for i, p := range points {
					if i == 0 || e.fn == "max_over_time" && p.V > result || e.fn == "min_over_time" && p.V < result {
						result = p.V
					}
				}
			}
			if ok {
				vector = append(vector, Sample{Labels: dropName(candidate.labels), V: result})
			}
		}
		return value{vector: vector}, nil
	case "histogram_quantile":
		q, err := s.scalarArg(e.args[0], t)
		if err != nil {
			return value{}, err
		}
		inner, err := s.eval(e.args[1], t)
		if err != nil {
			return value{}, err
		}
		return value{vector: histogramQuantile(q, inner.vector)}, nil
	case "clamp_min", "clamp_max":
		inner, err := s.eval(e.args[0], t)
		if err != nil {
			return value{}, err
		}
		bound, err := s.scalarArg(e.args[1], t)
		if err != nil {
			return value{}, err
		}
		var out Vector
		for _, sample := range inner.vector {
			v := sample.V
			if e.fn == "clamp_min" {
				v = math.Max(v, bound)
			} else {
				v = math.Min(v, bound)
			}
			out = append(out, Sample{Labels: dropName(sample.Labels), V: v})
		}
		return value{vector: out}, nil
	case "absent":
		inner, err := s.eval(e.args[0], t)
		if err != nil {
			return value{}, err
		}
		if inner.isScalar {
			return value{}, fmt.Errorf("absent needs a vector")
		}
		if len(inner.vector) > 0 {
			return value{}, nil
		}
		labels := Labels{}
		if sel, ok := e.args[0].(selectorExpr); ok {
			for _, m := range sel.matchers {
				if m.op == "=" {
					labels[m.name] = m.value
				}
			}
		}
		return value{vector: Vector{{Labels: labels, V: 1}}}, nil
	case "abs":
		inner, err := s.eval(e.args[0], t)
		if err != nil {
			return value{}, err
		}
		var out Vector
		for _, sample := range inner.vector {
			out = append(out, Sample{Labels: dropName(sample.Labels), V: math.Abs(sample.V)})
		}
		return value{vector: out}, nil
	}
	return value{}, fmt.Errorf("function %s is not supported", e.fn)
}

// extrapolated is Prometheus' extrapolatedRate: the increase (or delta) over
// the samples in the window, extrapolated towards the window edges by at most
// half the average sample interval, and not below zero for a counter.
func extrapolated(points []Point, t time.Time, window time.Duration, counter, rate bool) (float64, bool) {
	if len(points) < 2 {
		return 0, false
	}
	first, last := points[0], points[len(points)-1]
	result := last.V - first.V
	if counter {
		for i := 1; i < len(points); i++ {
			if points[i].V < points[i-1].V {
				result += points[i-1].V
			}
		}
	}
	rangeStart := t.Add(-window)
	durationToStart := first.T.Sub(rangeStart).Seconds()
	durationToEnd := t.Sub(last.T).Seconds()
	sampled := last.T.Sub(first.T).Seconds()
	average := sampled / float64(len(points)-1)
	if counter && result > 0 && first.V >= 0 {
		if toZero := sampled * (first.V / result); toZero < durationToStart {
			durationToStart = toZero
		}
	}
	threshold := average * 1.1
	interval := sampled
	if durationToStart < threshold {
		interval += durationToStart
	} else {
		interval += average / 2
	}
	if durationToEnd < threshold {
		interval += durationToEnd
	} else {
		interval += average / 2
	}
	factor := interval / sampled
	if rate {
		factor /= window.Seconds()
	}
	return result * factor, true
}

// regression is Prometheus' linearRegression with the intercept at t.
func regression(points []Point, t time.Time) (slope, intercept float64, ok bool) {
	if len(points) < 2 {
		return 0, 0, false
	}
	var n, sumX, sumY, sumXY, sumX2 float64
	for _, p := range points {
		x := p.T.Sub(t).Seconds()
		n++
		sumX += x
		sumY += p.V
		sumXY += x * p.V
		sumX2 += x * x
	}
	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n
	if varX == 0 {
		return 0, 0, false
	}
	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n
	return slope, intercept, true
}

func histogramQuantile(q float64, vector Vector) Vector {
	type bucket struct {
		upper, count float64
	}
	groups := map[string][]bucket{}
	labels := map[string]Labels{}
	for _, sample := range vector {
		le, ok := sample.Labels["le"]
		if !ok {
			continue
		}
		upper, err := strconv.ParseFloat(le, 64)
		if err != nil {
			continue
		}
		group := dropName(sample.Labels)
		delete(group, "le")
		key := group.key()
		groups[key] = append(groups[key], bucket{upper, sample.V})
		labels[key] = group
	}
	var out Vector
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		buckets := groups[key]
		sort.Slice(buckets, func(i, j int) bool { return buckets[i].upper < buckets[j].upper })
		result := math.NaN()
		switch {
		case q < 0:
			result = math.Inf(-1)
		case q > 1:
			result = math.Inf(1)
		case len(buckets) >= 2 && math.IsInf(buckets[len(buckets)-1].upper, 1):
			for i := 1; i < len(buckets); i++ {
				if buckets[i].count < buckets[i-1].count {
					buckets[i].count = buckets[i-1].count
				}
			}
			total := buckets[len(buckets)-1].count
			if total == 0 {
				break
			}
			rank := q * total
			b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })
			switch {
			case b == len(buckets)-1:
				result = buckets[len(buckets)-2].upper
			case b == 0 && buckets[0].upper <= 0:
				result = buckets[0].upper
			default:
				start, end, count := 0.0, buckets[b].upper, buckets[b].count
				if b > 0 {
					start = buckets[b-1].upper
					count -= buckets[b-1].count
					rank -= buckets[b-1].count
				}
				result = start + (end-start)*(rank/count)
			}
		}
		out = append(out, Sample{Labels: labels[key], V: result})
	}
	return out
}

func aggregate(e aggregateExpr, vector Vector) Vector {
	type group struct {
		labels Labels
		values []float64
	}
	groups := map[string]*group{}
	var order []string
	for _, sample := range vector {
		key := Labels{}
		if e.by {
			for _, name := range e.labels {
				if v, ok := sample.Labels[name]; ok {
					key[name] = v
				}
			}
		} else {
			key = dropName(sample.Labels)
			for _, name := range e.labels {
				delete(key, name)
			}
		}
		k := key.key()
		if groups[k] == nil {
			groups[k] = &group{labels: key}
			order = append(order, k)
		}
		groups[k].values = append(groups[k].values, sample.V)
	}
	sort.Strings(order)
	var out Vector
	for _, k := range order {
		g := groups[k]
		var result float64
		switch e.op {
		case "sum", "avg":
			for _, v := range g.values {
				result += v
			}
			if e.op == "avg" {
				result /= float64(len(g.values))
			}
		case "max", "min":
			result = g.values[0]
			for _, v := range g.values[1:] {
				if e.op == "max" && (v > result || math.IsNaN(result)) || e.op == "min" && (v < result || math.IsNaN(result)) {
					result = v
				}
			}
		case "count":
			result = float64(len(g.values))
		}
		out = append(out, Sample{Labels: g.labels, V: result})
	}
	return out
}

func comparison(op string) bool {
	return op == "==" || op == "!=" || op == ">" || op == "<" || op == ">=" || op == "<="
}

func apply(op string, l, r float64) (float64, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		return l / r, true
	case "%":
		return math.Mod(l, r), true
	case "==":
		return boolFloat(l == r), l == r
	case "!=":
		return boolFloat(l != r), l != r
	case ">":
		return boolFloat(l > r), l > r
	case "<":
		return boolFloat(l < r), l < r
	case ">=":
		return boolFloat(l >= r), l >= r
	case "<=":
		return boolFloat(l <= r), l <= r
	}
	return math.NaN(), false
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (b binaryExpr) signature(l Labels) string {
	key := Labels{}
	if b.matching && b.on {
		for _, name := range b.labels {
			if v, ok := l[name]; ok {
				key[name] = v
			}
		}
	} else {
		key = dropName(l)
		for _, name := range b.labels {
			delete(key, name)
		}
	}
	return key.key()
}

func (s *Store) binary(e binaryExpr, t time.Time) (value, error) {
	left, err := s.eval(e.left, t)
	if err != nil {
		return value{}, err
	}
	right, err := s.eval(e.right, t)
	if err != nil {
		return value{}, err
	}
	setOp := e.op == "and" || e.op == "or" || e.op == "unless"
	if setOp && (left.isScalar || right.isScalar) {
		return value{}, fmt.Errorf("%s needs two vectors", e.op)
	}
	switch {
	case left.isScalar && right.isScalar:
		if comparison(e.op) && !e.boolMod {
			return value{}, fmt.Errorf("comparison between scalars needs bool")
		}
		v, _ := apply(e.op, left.scalar, right.scalar)
		return value{isScalar: true, scalar: v}, nil
	case left.isScalar || right.isScalar:
		vector, scalar, scalarLeft := right.vector, left.scalar, true
		if right.isScalar {
			vector, scalar, scalarLeft = left.vector, right.scalar, false
		}
		var out Vector
		for _, sample := range vector {
			l, r := sample.V, scalar
			if scalarLeft {
				l, r = scalar, sample.V
			}
			v, keep := apply(e.op, l, r)
			labels := sample.Labels.clone()
			switch {
			case comparison(e.op) && !e.boolMod:
				if !keep {
					continue
				}
				v = sample.V
			default:
				delete(labels, "__name__")
			}
			out = append(out, Sample{Labels: labels, V: v})
		}
		return value{vector: out}, nil
	}
	rightBySig := map[string][]Sample{}
	for _, sample := range right.vector {
		sig := e.signature(sample.Labels)
		rightBySig[sig] = append(rightBySig[sig], sample)
	}
	var out Vector
	switch e.op {
	case "and", "unless":
		for _, sample := range left.vector {
			_, found := rightBySig[e.signature(sample.Labels)]
			if found == (e.op == "and") {
				out = append(out, sample)
			}
		}
		return value{vector: out}, nil
	case "or":
		leftSigs := map[string]bool{}
		for _, sample := range left.vector {
			leftSigs[e.signature(sample.Labels)] = true
			out = append(out, sample)
		}
		for _, sample := range right.vector {
			if !leftSigs[e.signature(sample.Labels)] {
				out = append(out, sample)
			}
		}
		return value{vector: out}, nil
	}
	seen := map[string]bool{}
	for _, sample := range left.vector {
		sig := e.signature(sample.Labels)
		matches := rightBySig[sig]
		if len(matches) == 0 {
			continue
		}
		if len(matches) > 1 {
			return value{}, fmt.Errorf("many-to-many matching in %s: several right-hand series for %s", e, sample.Labels)
		}
		if seen[sig] {
			return value{}, fmt.Errorf("many-to-one matching in %s needs group_left: %s", e, sample.Labels)
		}
		seen[sig] = true
		v, keep := apply(e.op, sample.V, matches[0].V)
		if comparison(e.op) && !e.boolMod {
			if !keep {
				continue
			}
			v = sample.V
		}
		labels := sample.Labels.clone()
		if !comparison(e.op) || e.boolMod {
			delete(labels, "__name__")
		}
		if e.matching && e.on {
			kept := Labels{}
			for _, name := range append(slices.Clone(e.labels), "__name__") {
				if v, ok := labels[name]; ok {
					kept[name] = v
				}
			}
			labels = kept
		} else if e.matching {
			for _, name := range e.labels {
				delete(labels, name)
			}
		}
		out = append(out, Sample{Labels: labels, V: v})
	}
	return value{vector: out}, nil
}

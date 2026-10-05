// Package promrule evaluates Prometheus recording and alerting rules over
// recorded series without Prometheus. It exists because promtool is not part
// of this repository's toolchain: the monitoring examples (ADR 0022) are
// validated by parsing their rule files and evaluating every rule over
// recorded metric fixtures with the semantics below.
//
// It implements a documented PromQL subset, and refuses everything else
// rather than guess:
//
//   - instant and range selectors with =, !=, =~ and !~ matchers, a five
//     minute lookback for instant selectors, and no offset or @;
//   - rate, increase (Prometheus' extrapolated counter algorithm with reset
//     handling), delta, deriv and predict_linear (least squares),
//     max_over_time, min_over_time, histogram_quantile (linear interpolation
//     within buckets), clamp_min, clamp_max and abs;
//   - sum, min, max, avg and count with by or without;
//   - + - * / and comparisons (with bool) between scalars and vectors,
//     one-to-one vector matching with on or ignoring, and the set operators
//     and, or and unless. group_left and group_right are refused.
//
// Rule evaluation follows Prometheus: rules run in file order at each
// evaluation time, a recording rule's output is visible to later rules at
// the same time, and an alert fires once its expression has returned the
// same label set at every evaluation for at least its for duration.
package promrule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokDuration
	tokOp
	tokLParen
	tokRParen
	tokLBrace
	tokRBrace
	tokLBracket
	tokRBracket
	tokComma
)

type token struct {
	kind tokenKind
	text string
}

func lex(input string) ([]token, error) {
	var tokens []token
	for i := 0; i < len(input); {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#':
			for i < len(input) && input[i] != '\n' {
				i++
			}
		case c == '(':
			tokens = append(tokens, token{tokLParen, "("})
			i++
		case c == ')':
			tokens = append(tokens, token{tokRParen, ")"})
			i++
		case c == '{':
			tokens = append(tokens, token{tokLBrace, "{"})
			i++
		case c == '}':
			tokens = append(tokens, token{tokRBrace, "}"})
			i++
		case c == ',':
			tokens = append(tokens, token{tokComma, ","})
			i++
		case c == '[':
			end := strings.IndexByte(input[i:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated range at %d", i)
			}
			tokens = append(tokens, token{tokLBracket, "["}, token{tokDuration, strings.TrimSpace(input[i+1 : i+end])}, token{tokRBracket, "]"})
			i += end + 1
		case c == '"' || c == '\'':
			j := i + 1
			var b strings.Builder
			for ; j < len(input) && input[j] != c; j++ {
				if input[j] == '\\' && j+1 < len(input) {
					j++
					switch input[j] {
					case 'n':
						b.WriteByte('\n')
					default:
						b.WriteByte(input[j])
					}
					continue
				}
				b.WriteByte(input[j])
			}
			if j >= len(input) {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			tokens = append(tokens, token{tokString, b.String()})
			i = j + 1
		case strings.ContainsRune("=!<>", rune(c)):
			op := string(c)
			if i+1 < len(input) && (input[i+1] == '=' || input[i+1] == '~') {
				op += string(input[i+1])
			}
			tokens = append(tokens, token{tokOp, op})
			i += len(op)
		case strings.ContainsRune("+-*/%^", rune(c)):
			tokens = append(tokens, token{tokOp, string(c)})
			i++
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(input) && (input[j] >= '0' && input[j] <= '9' || input[j] == '.' || input[j] == 'e' || input[j] == 'E' || (input[j] == '+' || input[j] == '-') && j > i && (input[j-1] == 'e' || input[j-1] == 'E')) {
				j++
			}
			tokens = append(tokens, token{tokNumber, input[i:j]})
			i = j
		case c == '_' || c == ':' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(input) && (input[j] == '_' || input[j] == ':' || input[j] >= 'a' && input[j] <= 'z' || input[j] >= 'A' && input[j] <= 'Z' || input[j] >= '0' && input[j] <= '9') {
				j++
			}
			tokens = append(tokens, token{tokIdent, input[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected %q at %d", c, i)
		}
	}
	return append(tokens, token{kind: tokEOF}), nil
}

// Expr is a parsed expression.
type Expr interface{ String() string }

type numberExpr struct{ value float64 }

type matcher struct {
	name, op, value string
}

type selectorExpr struct {
	name     string
	matchers []matcher
	window   time.Duration // zero for an instant selector
}

type callExpr struct {
	fn   string
	args []Expr
}

type aggregateExpr struct {
	op      string
	by      bool
	labels  []string
	grouped bool
	inner   Expr
}

type binaryExpr struct {
	op          string
	boolMod     bool
	matching    bool // on or ignoring given
	on          bool
	labels      []string
	left, right Expr
}

type parenExpr struct{ inner Expr }

func (e numberExpr) String() string { return strconv.FormatFloat(e.value, 'g', -1, 64) }
func (e selectorExpr) String() string {
	parts := make([]string, len(e.matchers))
	for i, m := range e.matchers {
		parts[i] = m.name + m.op + strconv.Quote(m.value)
	}
	s := e.name + "{" + strings.Join(parts, ",") + "}"
	if e.window > 0 {
		s += "[" + e.window.String() + "]"
	}
	return s
}
func (e callExpr) String() string {
	parts := make([]string, len(e.args))
	for i, a := range e.args {
		parts[i] = a.String()
	}
	return e.fn + "(" + strings.Join(parts, ", ") + ")"
}
func (e aggregateExpr) String() string {
	mod := "without"
	if e.by {
		mod = "by"
	}
	return fmt.Sprintf("%s %s (%s) (%s)", e.op, mod, strings.Join(e.labels, ","), e.inner)
}
func (e binaryExpr) String() string { return fmt.Sprintf("(%s %s %s)", e.left, e.op, e.right) }
func (e parenExpr) String() string  { return "(" + e.inner.String() + ")" }

var (
	functions  = map[string]int{"rate": 1, "increase": 1, "delta": 1, "deriv": 1, "predict_linear": 2, "max_over_time": 1, "min_over_time": 1, "histogram_quantile": 2, "clamp_min": 2, "clamp_max": 2, "abs": 1}
	aggregates = map[string]bool{"sum": true, "min": true, "max": true, "avg": true, "count": true}
)

type parser struct {
	tokens []token
	pos    int
}

// Parse parses a PromQL expression in the supported subset.
func Parse(input string) (Expr, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	expr, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected %q after expression", p.peek().text)
	}
	return expr, nil
}

func (p *parser) peek() token { return p.tokens[p.pos] }
func (p *parser) next() token {
	t := p.tokens[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) expect(kind tokenKind, what string) (token, error) {
	t := p.next()
	if t.kind != kind {
		return t, fmt.Errorf("expected %s, found %q", what, t.text)
	}
	return t, nil
}

// precedence of binary operators; higher binds tighter.
func precedence(t token) int {
	switch {
	case t.kind == tokIdent && t.text == "or":
		return 1
	case t.kind == tokIdent && (t.text == "and" || t.text == "unless"):
		return 2
	case t.kind == tokOp && (t.text == "==" || t.text == "!=" || t.text == ">" || t.text == "<" || t.text == ">=" || t.text == "<="):
		return 3
	case t.kind == tokOp && (t.text == "+" || t.text == "-"):
		return 4
	case t.kind == tokOp && (t.text == "*" || t.text == "/" || t.text == "%"):
		return 5
	}
	return 0
}

func (p *parser) expr(min int) (Expr, error) {
	left, err := p.unary()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		prec := precedence(op)
		if prec == 0 || prec < min {
			return left, nil
		}
		p.next()
		b := binaryExpr{op: op.text, left: left}
		if p.peek().kind == tokIdent && p.peek().text == "bool" {
			if prec != 3 {
				return nil, fmt.Errorf("bool modifier on %s", op.text)
			}
			p.next()
			b.boolMod = true
		}
		if t := p.peek(); t.kind == tokIdent && (t.text == "on" || t.text == "ignoring") {
			p.next()
			b.matching, b.on = true, t.text == "on"
			if b.labels, err = p.labelList(); err != nil {
				return nil, err
			}
		}
		if t := p.peek(); t.kind == tokIdent && (t.text == "group_left" || t.text == "group_right") {
			return nil, fmt.Errorf("%s is not supported", t.text)
		}
		// Left associative: the right operand binds operators tighter than op.
		right, err := p.expr(prec + 1)
		if err != nil {
			return nil, err
		}
		b.right = right
		left = b
	}
}

func (p *parser) labelList() ([]string, error) {
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	var labels []string
	for p.peek().kind != tokRParen {
		t, err := p.expect(tokIdent, "label name")
		if err != nil {
			return nil, err
		}
		labels = append(labels, t.text)
		if p.peek().kind == tokComma {
			p.next()
		}
	}
	p.next()
	return labels, nil
}

func (p *parser) unary() (Expr, error) {
	if t := p.peek(); t.kind == tokOp && t.text == "-" {
		p.next()
		inner, err := p.unary()
		if err != nil {
			return nil, err
		}
		return binaryExpr{op: "*", left: numberExpr{-1}, right: inner}, nil
	}
	return p.primary()
}

func (p *parser) primary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tokNumber:
		value, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, err
		}
		return numberExpr{value}, nil
	case tokLParen:
		inner, err := p.expr(0)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
		return parenExpr{inner}, nil
	case tokIdent:
		if aggregates[t.text] {
			return p.aggregate(t.text)
		}
		if p.peek().kind == tokLParen {
			arity, ok := functions[t.text]
			if !ok {
				return nil, fmt.Errorf("function %s is not supported", t.text)
			}
			p.next()
			var args []Expr
			for p.peek().kind != tokRParen {
				arg, err := p.expr(0)
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if p.peek().kind == tokComma {
					p.next()
				}
			}
			p.next()
			if len(args) != arity {
				return nil, fmt.Errorf("%s takes %d arguments, got %d", t.text, arity, len(args))
			}
			return callExpr{fn: t.text, args: args}, nil
		}
		return p.selector(t.text)
	case tokLBrace:
		p.pos--
		return p.selector("")
	}
	return nil, fmt.Errorf("unexpected %q", t.text)
}

func (p *parser) aggregate(op string) (Expr, error) {
	a := aggregateExpr{op: op}
	modifier := func() error {
		if t := p.peek(); t.kind == tokIdent && (t.text == "by" || t.text == "without") {
			if a.grouped {
				return fmt.Errorf("two grouping clauses on %s", op)
			}
			p.next()
			labels, err := p.labelList()
			if err != nil {
				return err
			}
			a.by, a.labels, a.grouped = t.text == "by", labels, true
		}
		return nil
	}
	if err := modifier(); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	inner, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return nil, err
	}
	a.inner = inner
	if err := modifier(); err != nil {
		return nil, err
	}
	if !a.grouped {
		a.by = true
	}
	return a, nil
}

func (p *parser) selector(name string) (Expr, error) {
	s := selectorExpr{name: name}
	if p.peek().kind == tokLBrace {
		p.next()
		for p.peek().kind != tokRBrace {
			label, err := p.expect(tokIdent, "label name")
			if err != nil {
				return nil, err
			}
			op, err := p.expect(tokOp, "matcher")
			if err != nil {
				return nil, err
			}
			if op.text != "=" && op.text != "!=" && op.text != "=~" && op.text != "!~" {
				return nil, fmt.Errorf("matcher %q", op.text)
			}
			value, err := p.expect(tokString, "label value")
			if err != nil {
				return nil, err
			}
			s.matchers = append(s.matchers, matcher{label.text, op.text, value.text})
			if p.peek().kind == tokComma {
				p.next()
			}
		}
		p.next()
	}
	if s.name == "" && len(s.matchers) == 0 {
		return nil, fmt.Errorf("empty selector")
	}
	if p.peek().kind == tokLBracket {
		p.next()
		d, err := p.expect(tokDuration, "range")
		if err != nil {
			return nil, err
		}
		window, err := ParseDuration(d.text)
		if err != nil {
			return nil, err
		}
		s.window = window
		if _, err := p.expect(tokRBracket, "]"); err != nil {
			return nil, err
		}
	}
	if t := p.peek(); t.kind == tokIdent && (t.text == "offset" || t.text == "@") {
		return nil, fmt.Errorf("offset is not supported")
	}
	return s, nil
}

// ParseDuration parses a Prometheus duration (1h30m, 5m, 30s, 7d).
func ParseDuration(text string) (time.Duration, error) {
	if text == "" {
		return 0, fmt.Errorf("empty duration")
	}
	units := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour, "y": 365 * 24 * time.Hour}
	var total time.Duration
	for rest := text; rest != ""; {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("duration %q", text)
		}
		n, _ := strconv.Atoi(rest[:i])
		rest = rest[i:]
		unit := ""
		for _, u := range []string{"ms", "s", "m", "h", "d", "w", "y"} {
			if strings.HasPrefix(rest, u) {
				unit = u
				break
			}
		}
		if unit == "" {
			return 0, fmt.Errorf("duration %q", text)
		}
		rest = rest[len(unit):]
		total += time.Duration(n) * units[unit]
	}
	return total, nil
}

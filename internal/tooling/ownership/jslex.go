package ownership

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// This file is a JavaScript/TypeScript lexer and import recognizer. It is
// not a regular expression over source text: it tokenizes the whole file
// following the ECMAScript lexical grammar (comments, string and template
// literals with escapes, identifier escapes, and the regular-expression
// versus division goal decided from the paren/brace context), and then
// recognizes every import form from the token stream. Any lexical error or
// unbalanced bracket fails the file. The one decision the lexical grammar
// cannot make without a parser, whether a "/" starts a regular expression
// or divides, is made from context; wherever that context is ambiguous
// (after "}", after a control-statement head ")", after ">", or after a
// contextual keyword that may be an identifier) the line is recorded and
// the file is reported unverified, because a wrong guess could hide an
// import inside a mis-read literal. Nothing here evaluates or runs the
// source.

type tokenKind uint8

const (
	tokEOF tokenKind = iota
	tokIdent
	tokString         // '…' or "…", value decoded
	tokTemplate       // `…` without substitutions, value cooked
	tokTemplateHead   // `…${
	tokTemplateMiddle // }…${
	tokTemplateTail   // }…`
	tokNumber
	tokRegex
	tokPunct
	tokPrivate // #name
)

type jsToken struct {
	kind  tokenKind
	text  string // identifier (escapes decoded), punctuator or raw text
	value string // decoded string or cooked template value
	line  int
	brace braceKind // for "{": what the brace opens
}

// braceKind is what an open "{" belongs to; it decides whether a "/" after
// the matching "}" starts a regular expression.
type braceKind uint8

const (
	braceBlock braceKind = iota
	braceExpr
	braceClass
	braceTemplate
)

type lexError struct {
	line   int
	reason string
}

func (e *lexError) Error() string { return fmt.Sprintf("line %d: %s", e.line, e.reason) }

// reference is a triple-slash <reference path|types> directive.
type reference struct {
	line  int
	value string
	types bool
}

type lexer struct {
	src  []byte
	pos  int
	line int
	toks []jsToken
	refs []reference
	// ambiguous lists the lines of every "/" whose lexical goal the
	// context cannot decide; any entry makes the file unverified.
	ambiguous []int

	parens     []bool // per open "(": preceded by if/while/for/with
	brackets   int
	braces     []braceKind
	lastParen  bool      // the last ")" closed a control-statement head
	lastBrace  braceKind // what the last "}" closed
	classAhead bool      // a class keyword awaits its body "{"
}

// maxTokens bounds one file's token stream (files are at most 1 MiB).
const maxTokens = 1 << 20

func lex(src []byte) ([]jsToken, []reference, []int, error) {
	l := &lexer{src: src, line: 1}
	if len(src) >= 3 && src[0] == 0xef && src[1] == 0xbb && src[2] == 0xbf { // byte order mark
		l.pos = 3
	}
	if len(src) >= l.pos+2 && src[l.pos] == '#' && src[l.pos+1] == '!' { // hashbang
		for l.pos < len(src) && !isLineTerminator(src[l.pos]) {
			l.pos++
		}
	}
	for {
		if err := l.skipSpaceAndComments(); err != nil {
			return nil, nil, nil, err
		}
		if l.pos >= len(l.src) {
			break
		}
		if len(l.toks) >= maxTokens {
			return nil, nil, nil, &lexError{l.line, "too many tokens"}
		}
		if err := l.next(); err != nil {
			return nil, nil, nil, err
		}
	}
	if len(l.parens) != 0 || l.brackets != 0 || len(l.braces) != 0 {
		return nil, nil, nil, &lexError{l.line, "unbalanced brackets at end of file"}
	}
	l.toks = append(l.toks, jsToken{kind: tokEOF, line: l.line})
	return l.toks, l.refs, l.ambiguous, nil
}

func (l *lexer) emit(kind tokenKind, text, value string, line int) {
	l.toks = append(l.toks, jsToken{kind: kind, text: text, value: value, line: line})
}

func (l *lexer) prev() *jsToken {
	if len(l.toks) == 0 {
		return nil
	}
	return &l.toks[len(l.toks)-1]
}

func isLineTerminator(c byte) bool { return c == '\n' || c == '\r' }

func (l *lexer) newline(c byte) {
	if c == '\n' || (c == '\r' && (l.pos+1 >= len(l.src) || l.src[l.pos+1] != '\n')) {
		l.line++
	}
}

func (l *lexer) skipSpaceAndComments() error {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\v' || c == '\f':
			l.pos++
		case isLineTerminator(c):
			l.newline(c)
			l.pos++
		case c == '/' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '/':
			start, line := l.pos, l.line
			for l.pos < len(l.src) && !isLineTerminator(l.src[l.pos]) {
				l.pos++
			}
			l.directive(string(l.src[start:l.pos]), line)
		case c == '/' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '*':
			end := strings.Index(string(l.src[l.pos+2:]), "*/")
			if end < 0 {
				return &lexError{l.line, "unterminated comment"}
			}
			for _, b := range l.src[l.pos : l.pos+2+end] {
				if b == '\n' {
					l.line++
				}
			}
			l.pos += end + 4
		case c >= 0x80:
			r, size := utf8.DecodeRune(l.src[l.pos:])
			switch r {
			case '\u00a0', '\ufeff', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u202f', '\u205f', '\u3000':
				l.pos += size
			case '\u2028', '\u2029':
				l.line++
				l.pos += size
			default:
				return nil
			}
		default:
			return nil
		}
	}
	return nil
}

// directive records a TypeScript triple-slash reference, which pulls another
// file or package into the program like an import.
func (l *lexer) directive(comment string, line int) {
	body, ok := strings.CutPrefix(comment, "///")
	if !ok {
		return
	}
	body = strings.TrimSpace(body)
	if !strings.HasPrefix(body, "<reference") {
		return
	}
	for _, attribute := range []string{"path", "types"} {
		if value, found := attributeValue(body, attribute); found {
			l.refs = append(l.refs, reference{line: line, value: value, types: attribute == "types"})
		}
	}
}

func attributeValue(tag, name string) (string, bool) {
	for rest := tag; ; {
		index := strings.Index(rest, name)
		if index < 0 {
			return "", false
		}
		after := strings.TrimLeft(rest[index+len(name):], " \t")
		before := rest[:index]
		rest = rest[index+len(name):]
		if before != "" && !strings.ContainsAny(before[len(before)-1:], " \t") {
			continue
		}
		after, ok := strings.CutPrefix(after, "=")
		if !ok {
			continue
		}
		after = strings.TrimLeft(after, " \t")
		if after == "" || (after[0] != '"' && after[0] != '\'') {
			continue
		}
		end := strings.IndexByte(after[1:], after[0])
		if end < 0 {
			return "", false
		}
		return after[1 : 1+end], true
	}
}

var regexKeywords = map[string]bool{
	"return": true, "typeof": true, "instanceof": true, "in": true, "of": true, "new": true,
	"delete": true, "void": true, "throw": true, "case": true, "do": true, "else": true,
	"yield": true, "await": true, "extends": true,
}

// contextualKeywords may be identifiers (var of = 4) or operators
// (yield /re/), so a "/" after one cannot be decided lexically.
var contextualKeywords = map[string]bool{"of": true, "yield": true, "await": true, "let": true, "async": true}

// propertyName reports whether the token before the previous one is "." or
// "?.", making the previous identifier a property name (o.if, x.return).
func (l *lexer) propertyName() bool {
	if len(l.toks) < 2 {
		return false
	}
	before := l.toks[len(l.toks)-2]
	return before.kind == tokPunct && (before.text == "." || before.text == "?.")
}

// regexAllowed decides the lexical goal for "/" from the previous token, the
// way the grammar does: after an expression a "/" divides, elsewhere it
// starts a regular expression. Where the token alone cannot decide, the
// line is recorded as ambiguous (and the file becomes unverified); the
// guess made there only affects which imports are still reported.
func (l *lexer) regexAllowed() bool {
	p := l.prev()
	if p == nil {
		return true
	}
	switch p.kind {
	case tokIdent:
		if l.propertyName() {
			return false
		}
		if contextualKeywords[p.text] {
			l.ambiguous = append(l.ambiguous, l.line)
		}
		return regexKeywords[p.text]
	case tokPunct:
		switch p.text {
		case ")":
			if l.lastParen {
				// if (x) /re/ is a regular expression, but a head
				// misjudged as control would hide a division.
				l.ambiguous = append(l.ambiguous, l.line)
			}
			return l.lastParen
		case "]", "++", "--":
			return false
		case "}":
			// A block, class body, function or object literal: which
			// one a "}" closes needs a parser (function () {} / 1,
			// L: {} /re/), so it is never guessed silently.
			l.ambiguous = append(l.ambiguous, l.line)
			return l.lastBrace == braceBlock || l.lastBrace == braceClass
		case ">":
			// a > /re/, or TypeScript's instantiation f<T> / 2
			l.ambiguous = append(l.ambiguous, l.line)
			return true
		case "!":
			// TypeScript's postfix non-null assertion (a! / b): a prefix
			// "!" cannot follow an operand, so after one it divides.
			if len(l.toks) >= 2 {
				before := l.toks[len(l.toks)-2]
				operand := before.kind == tokNumber || before.kind == tokString || before.kind == tokTemplate || before.kind == tokTemplateTail || before.kind == tokRegex ||
					(before.kind == tokIdent && !regexKeywords[before.text]) || (before.kind == tokPunct && (before.text == ")" || before.text == "]"))
				return !operand
			}
			return true
		}
		return true
	}
	return false
}

// openBraceKind classifies a "{" from the previous token.
func (l *lexer) openBraceKind() braceKind {
	if l.classAhead {
		l.classAhead = false
		return braceClass
	}
	p := l.prev()
	if p == nil {
		return braceBlock
	}
	switch p.kind {
	case tokIdent:
		if regexKeywords[p.text] && p.text != "else" && p.text != "do" {
			return braceExpr
		}
		return braceBlock
	case tokPunct:
		switch p.text {
		case ")", ";", "}", "{", "=>":
			return braceBlock
		}
		return braceExpr
	case tokTemplateHead, tokTemplateMiddle:
		return braceExpr
	}
	return braceBlock
}

var punctuators = []string{
	">>>=", "...", "===", "!==", "**=", "<<=", ">>=", ">>>", "&&=", "||=", "??=",
	"=>", "==", "!=", "<=", ">=", "&&", "||", "??", "?.", "++", "--", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=", "<<", ">>", "**",
}

func (l *lexer) next() error {
	c := l.src[l.pos]
	line := l.line
	switch {
	case c == '"' || c == '\'':
		value, err := l.quoted(c)
		if err != nil {
			return err
		}
		l.emit(tokString, "", value, line)
	case c == '`':
		l.pos++
		return l.template(line, false)
	case isDigit(c) || (c == '.' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])):
		l.number()
		l.emit(tokNumber, "", "", line)
	case isIdentStart(c) || c == '\\' || c >= 0x80:
		name, err := l.identifier()
		if err != nil {
			return err
		}
		if p := l.prev(); name == "class" && (p == nil || !(p.kind == tokPunct && (p.text == "." || p.text == "?."))) {
			l.classAhead = true
		}
		l.emit(tokIdent, name, "", line)
	case c == '#':
		l.pos++
		name, err := l.identifier()
		if err != nil {
			return err
		}
		l.emit(tokPrivate, name, "", line)
	case c == '/':
		if l.regexAllowed() {
			return l.regex(line)
		}
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '=' {
			l.pos += 2
			l.emit(tokPunct, "/=", "", line)
			return nil
		}
		l.pos++
		l.emit(tokPunct, "/", "", line)
	case c == '(':
		p := l.prev()
		l.parens = append(l.parens, p != nil && p.kind == tokIdent && (p.text == "if" || p.text == "while" || p.text == "for" || p.text == "with") && !l.propertyName())
		l.pos++
		l.emit(tokPunct, "(", "", line)
	case c == ')':
		if len(l.parens) == 0 {
			return &lexError{line, "unbalanced )"}
		}
		l.lastParen = l.parens[len(l.parens)-1]
		l.parens = l.parens[:len(l.parens)-1]
		l.pos++
		l.emit(tokPunct, ")", "", line)
	case c == '[':
		l.brackets++
		l.pos++
		l.emit(tokPunct, "[", "", line)
	case c == ']':
		if l.brackets == 0 {
			return &lexError{line, "unbalanced ]"}
		}
		l.brackets--
		l.pos++
		l.emit(tokPunct, "]", "", line)
	case c == '{':
		kind := l.openBraceKind()
		l.braces = append(l.braces, kind)
		l.pos++
		l.toks = append(l.toks, jsToken{kind: tokPunct, text: "{", line: line, brace: kind})
	case c == '}':
		if len(l.braces) == 0 {
			return &lexError{line, "unbalanced }"}
		}
		kind := l.braces[len(l.braces)-1]
		l.braces = l.braces[:len(l.braces)-1]
		if kind == braceTemplate {
			l.pos++
			return l.template(line, true)
		}
		l.lastBrace = kind
		l.pos++
		l.emit(tokPunct, "}", "", line)
	default:
		for _, p := range punctuators {
			if strings.HasPrefix(string(l.src[l.pos:min(len(l.src), l.pos+len(p))]), p) {
				if p == "?." && l.pos+2 < len(l.src) && isDigit(l.src[l.pos+2]) {
					continue // a ? .5 conditional
				}
				l.pos += len(p)
				l.emit(tokPunct, p, "", line)
				return nil
			}
		}
		if strings.IndexByte("=<>!+-*%&|^~?:;,.@", c) < 0 {
			return &lexError{line, fmt.Sprintf("unexpected character %q", c)}
		}
		l.pos++
		l.emit(tokPunct, string(c), "", line)
	}
	return nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }

// identifier reads an IdentifierName, decoding \u escapes: require is
// the identifier require.
func (l *lexer) identifier() (string, error) {
	var b strings.Builder
	start := l.pos
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case isIdentPart(c):
			b.WriteByte(c)
			l.pos++
		case c == '\\':
			if l.pos+1 >= len(l.src) || l.src[l.pos+1] != 'u' {
				return "", &lexError{l.line, "invalid identifier escape"}
			}
			l.pos += 2
			r, err := l.unicodeEscape()
			if err != nil {
				return "", err
			}
			b.WriteRune(r)
		case c >= 0x80:
			r, size := utf8.DecodeRune(l.src[l.pos:])
			if r == utf8.RuneError || isUnicodeSpace(r) {
				if l.pos == start {
					return "", &lexError{l.line, "unexpected character"}
				}
				return b.String(), nil
			}
			b.WriteRune(r)
			l.pos += size
		default:
			if l.pos == start {
				return "", &lexError{l.line, "empty identifier"}
			}
			return b.String(), nil
		}
	}
	return b.String(), nil
}

func isUnicodeSpace(r rune) bool {
	switch r {
	case '\u00a0', '\ufeff', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u202f', '\u205f', '\u3000', '\u2028', '\u2029':
		return true
	}
	return false
}

// unicodeEscape reads the part after \u: XXXX or {X…}.
func (l *lexer) unicodeEscape() (rune, error) {
	if l.pos < len(l.src) && l.src[l.pos] == '{' {
		end := strings.IndexByte(string(l.src[l.pos:]), '}')
		if end < 2 || end > 7 {
			return 0, &lexError{l.line, "invalid unicode escape"}
		}
		value, err := strconv.ParseUint(string(l.src[l.pos+1:l.pos+end]), 16, 32)
		if err != nil || value > 0x10ffff {
			return 0, &lexError{l.line, "invalid unicode escape"}
		}
		l.pos += end + 1
		return rune(value), nil
	}
	if l.pos+4 > len(l.src) {
		return 0, &lexError{l.line, "invalid unicode escape"}
	}
	value, err := strconv.ParseUint(string(l.src[l.pos:l.pos+4]), 16, 32)
	if err != nil {
		return 0, &lexError{l.line, "invalid unicode escape"}
	}
	l.pos += 4
	return rune(value), nil
}

func (l *lexer) number() {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case isIdentPart(c) || c == '.':
			l.pos++
		case (c == '+' || c == '-') && l.pos > 0 && (l.src[l.pos-1] == 'e' || l.src[l.pos-1] == 'E') && !isHexNumber(l.src, l.pos):
			l.pos++
		default:
			return
		}
	}
}

// isHexNumber reports whether the number ending before pos is hexadecimal,
// where "e" is a digit and a following sign is an operator.
func isHexNumber(src []byte, pos int) bool {
	start := pos - 1
	for start > 0 && (isIdentPart(src[start-1]) || src[start-1] == '.') {
		start--
	}
	return pos-start >= 2 && src[start] == '0' && (src[start+1] == 'x' || src[start+1] == 'X')
}

// escape decodes one escape sequence after the backslash. legacyOctal
// allows sloppy-mode octal escapes (\56 is "."), which strings accept and
// templates do not.
func (l *lexer) escape(b *strings.Builder, legacyOctal bool) error {
	if l.pos >= len(l.src) {
		return &lexError{l.line, "unterminated escape"}
	}
	c := l.src[l.pos]
	l.pos++
	switch c {
	case 'n':
		b.WriteByte('\n')
	case 't':
		b.WriteByte('\t')
	case 'r':
		b.WriteByte('\r')
	case 'b':
		b.WriteByte('\b')
	case 'f':
		b.WriteByte('\f')
	case 'v':
		b.WriteByte('\v')
	case '\r':
		if l.pos < len(l.src) && l.src[l.pos] == '\n' {
			l.pos++
		}
		l.line++
	case '\n':
		l.line++
	case 'x':
		if l.pos+2 > len(l.src) {
			return &lexError{l.line, "invalid hex escape"}
		}
		value, err := strconv.ParseUint(string(l.src[l.pos:l.pos+2]), 16, 8)
		if err != nil {
			return &lexError{l.line, "invalid hex escape"}
		}
		l.pos += 2
		b.WriteRune(rune(value))
	case 'u':
		r, err := l.unicodeEscape()
		if err != nil {
			return err
		}
		b.WriteRune(r)
	default:
		if c >= '0' && c <= '7' {
			if !legacyOctal && !(c == '0' && (l.pos >= len(l.src) || !isDigit(l.src[l.pos]))) {
				return &lexError{l.line, "octal escape in template"}
			}
			value := int(c - '0')
			for count := 1; count < 3 && l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '7' && value*8+int(l.src[l.pos]-'0') <= 0377; count++ {
				value = value*8 + int(l.src[l.pos]-'0')
				l.pos++
			}
			b.WriteRune(rune(value))
			return nil
		}
		if c >= 0x80 {
			l.pos--
			r, size := utf8.DecodeRune(l.src[l.pos:])
			l.pos += size
			if r == '\u2028' || r == '\u2029' {
				l.line++
				return nil
			}
			b.WriteRune(r)
			return nil
		}
		b.WriteByte(c)
	}
	return nil
}

func (l *lexer) quoted(quote byte) (string, error) {
	l.pos++
	var b strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case c == quote:
			l.pos++
			return b.String(), nil
		case c == '\\':
			l.pos++
			if err := l.escape(&b, true); err != nil {
				return "", err
			}
		case isLineTerminator(c):
			return "", &lexError{l.line, "unterminated string"}
		default:
			b.WriteByte(c)
			l.pos++
		}
	}
	return "", &lexError{l.line, "unterminated string"}
}

// template reads template characters up to the closing backtick or the next
// "${". resumed is true after the "}" that ends a substitution.
func (l *lexer) template(line int, resumed bool) error {
	var b strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case c == '`':
			l.pos++
			if resumed {
				l.emit(tokTemplateTail, "", b.String(), line)
			} else {
				l.emit(tokTemplate, "", b.String(), line)
			}
			return nil
		case c == '$' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '{':
			l.pos += 2
			if resumed {
				l.emit(tokTemplateMiddle, "", b.String(), line)
			} else {
				l.emit(tokTemplateHead, "", b.String(), line)
			}
			l.braces = append(l.braces, braceTemplate)
			return nil
		case c == '\\':
			l.pos++
			if err := l.escape(&b, false); err != nil {
				return err
			}
		default:
			if c == '\n' || (c == '\r' && (l.pos+1 >= len(l.src) || l.src[l.pos+1] != '\n')) {
				l.line++
			}
			b.WriteByte(c)
			l.pos++
		}
	}
	return &lexError{line, "unterminated template"}
}

func (l *lexer) regex(line int) error {
	l.pos++ // opening /
	inClass := false
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case isLineTerminator(c):
			return &lexError{l.line, "unterminated regular expression"}
		case c == '\\':
			l.pos += 2
			continue
		case c == '[':
			inClass = true
		case c == ']':
			inClass = false
		case c == '/' && !inClass:
			l.pos++
			for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
				l.pos++
			}
			l.emit(tokRegex, "", "", line)
			return nil
		}
		l.pos++
	}
	return &lexError{l.line, "unterminated regular expression"}
}

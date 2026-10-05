package ownership

import "strings"

// jsImport is one static module reference found in a JavaScript or
// TypeScript file.
type jsImport struct {
	line      int
	form      string // import, export-from, import-equals, require, require.resolve, import(), import.meta.resolve, reference
	specifier string
	types     bool // a <reference types> package name
}

// jsUnverified is a form that loads code by a computed name, or evaluates
// source, so no static graph can account for it.
type jsUnverified struct {
	line int
	code string // CodeDynamicImport or CodeUnsupportedForm
	form string
}

type jsScan struct {
	imports    []jsImport
	unverified []jsUnverified
}

// The recognizer is an allowlist (ADR 0025). JavaScript can reach its
// module loader, evaluation and the Function constructor through the object
// graph in more ways than any list of forbidden forms can name, so a file is
// verified only when every occurrence of a sensitive name is one of a few
// enumerated safe forms:
//
//	import … from "s"   import "s"   export … from "s"   import("s")
//	import x = require("s")   require("s")   require.resolve("s")
//	import.meta.url|dirname|filename   import.meta.resolve("s")
//	module.exports   process.<one of processProperties>   this.<name>
//	constructor(…) { (a method definition)
//
// Any other occurrence of a sensitive word — as an identifier, a property,
// an object or destructuring key, a string or a template — any computed
// member access whose key is not a number or a non-sensitive string
// literal, and AMD define(…), makes the file unverified.

// sensitiveWords reach the loader, the global object, evaluation or the
// Function constructor.
var sensitiveWords = setOf(`require module process this globalThis global self window eval Function
	Reflect Proxy import arguments constructor __proto__ prototype
	createRequire getBuiltinModule mainModule _load dlopen importScripts ShadowRealm _linkedBinding Worker binding`)

// processProperties are the process members a node may read without
// reaching the loader or native bindings.
var processProperties = setOf(`env argv cwd platform arch version versions pid exit exitCode nextTick hrtime
	stdout stderr stdin uptime memoryUsage emitWarning on once`)

// importMetaProperties are the data members of import.meta.
var importMetaProperties = setOf(`url dirname filename`)

// loaderModules are built-in modules whose purpose is loading, evaluating
// or running code by computed name or path.
var loaderModules = map[string]bool{"vm": true, "module": true, "worker_threads": true, "child_process": true, "cluster": true}

func setOf(words string) map[string]bool {
	out := map[string]bool{}
	for _, word := range strings.Fields(words) {
		out[word] = true
	}
	return out
}

// scanJS lexes src and returns every import form it contains and every
// form that keeps the file from being verified. declarations is true for a
// .d.ts/.d.mts/.d.cts file, which holds no run-time code: its imports are
// still edges, but the allowlist does not apply to it.
func scanJS(src []byte, declarations bool) (jsScan, error) {
	toks, refs, irregular, err := lex(src)
	if err != nil {
		return jsScan{}, err
	}
	r := &recognizer{toks: toks, safe: map[int]bool{}}
	r.out.unverified = append(r.out.unverified, irregular...)
	for _, ref := range refs {
		r.out.imports = append(r.out.imports, jsImport{line: ref.line, form: "reference", specifier: ref.value, types: ref.types})
	}
	r.scan()
	if declarations {
		kept := r.out.unverified[:0]
		for _, form := range r.out.unverified {
			if form.code == CodeAmbiguousSyntax || form.form == "HTML-like comment" {
				kept = append(kept, form)
			}
		}
		r.out.unverified = kept
	}
	return r.out, nil
}

type recognizer struct {
	toks []jsToken
	out  jsScan
	// safe marks the tokens a recognized safe form owns; the allowlist
	// checks skip them.
	safe map[int]bool
}

func (r *recognizer) at(i int) jsToken {
	if i < 0 || i >= len(r.toks) {
		return jsToken{kind: tokEOF}
	}
	return r.toks[i]
}

func isPunct(t jsToken, text string) bool { return t.kind == tokPunct && t.text == text }

func isIdent(t jsToken, text string) bool { return t.kind == tokIdent && t.text == text }

func isStatic(t jsToken) bool { return t.kind == tokString || t.kind == tokTemplate }

// member reports a token used as a property name: after "." or "?.".
func (r *recognizer) member(i int) bool {
	p := r.at(i - 1)
	return isPunct(p, ".") || isPunct(p, "?.")
}

func (r *recognizer) unverified(line int, code, form string) {
	r.out.unverified = append(r.out.unverified, jsUnverified{line: line, code: code, form: form})
}

// add records an edge whose specifier is the token at index; the token is
// part of a safe form.
func (r *recognizer) add(line int, form string, index int) {
	r.safe[index] = true
	r.out.imports = append(r.out.imports, jsImport{line: line, form: form, specifier: r.toks[index].value})
}

func (r *recognizer) markSafe(indexes ...int) {
	for _, index := range indexes {
		r.safe[index] = true
	}
}

func (r *recognizer) scan() {
	// First pass: recognize the safe forms, so the second pass sees which
	// tokens they own.
	for i := 0; i < len(r.toks); i++ {
		t := r.toks[i]
		if t.kind != tokIdent || r.member(i) {
			continue
		}
		switch t.text {
		case "import":
			i = r.importAt(i)
		case "export":
			i = r.exportAt(i)
		case "require":
			r.requireAt(i)
		case "module":
			if isPunct(r.at(i+1), ".") && isIdent(r.at(i+2), "exports") {
				r.markSafe(i)
			}
		case "process":
			if isPunct(r.at(i+1), ".") && processProperties[r.at(i+2).text] && r.at(i+2).kind == tokIdent {
				r.markSafe(i)
			}
		case "this":
			if (isPunct(r.at(i+1), ".") || isPunct(r.at(i+1), "?.")) && (r.at(i+2).kind == tokIdent || r.at(i+2).kind == tokPrivate) {
				r.markSafe(i)
			}
		case "constructor":
			// constructor(…) { or constructor(…); — a method definition
			if isPunct(r.at(i+1), "(") {
				if close := r.matching(i + 1); close > 0 && (isPunct(r.at(close+1), "{") || isPunct(r.at(close+1), ";")) {
					r.markSafe(i)
				}
			}
		}
	}
	// Second pass: every remaining sensitive token, sensitive string and
	// unsafe computed member access.
	for i, t := range r.toks {
		if r.safe[i] {
			continue
		}
		switch {
		case t.kind == tokIdent && t.text == "binding" && !r.member(i) && !isPunct(r.at(i+1), "("):
			// a variable named binding; process.binding and binding(…) are not
		case t.kind == tokIdent && sensitiveWords[t.text]:
			form := t.text
			if r.member(i) {
				form = "." + form
			}
			r.unverified(t.line, CodeUnsupportedForm, form)
		case t.kind == tokIdent && t.text == "define" && !r.member(i) && isPunct(r.at(i+1), "("):
			r.unverified(t.line, CodeUnsupportedForm, "AMD define")
		case isStatic(t) && sensitiveWords[strings.TrimSpace(t.value)]:
			r.unverified(t.line, CodeUnsupportedForm, "string "+strings.TrimSpace(t.value))
		case isPunct(t, "[") && r.memberAccess(i) && !r.safeKey(i):
			r.unverified(t.line, CodeUnsupportedForm, "computed member access")
		}
	}
}

// operandEnd reports whether a token ends an operand, so a "[" after it is
// a member access rather than an array literal or pattern.
var notOperand = setOf(`return typeof instanceof in of new delete void throw case do else yield await
	extends let const var export default`)

func (r *recognizer) memberAccess(i int) bool {
	p := r.at(i - 1)
	switch p.kind {
	case tokIdent:
		return !notOperand[p.text]
	case tokString, tokTemplate, tokTemplateTail, tokNumber, tokRegex, tokPrivate:
		return true
	case tokPunct:
		switch p.text {
		case ")", "]", "}", "?.":
			return true
		case "!":
			before := r.at(i - 2)
			return before.kind == tokIdent && !notOperand[before.text] || isPunct(before, ")") || isPunct(before, "]")
		}
	}
	return false
}

// safeKey reports a computed key whose value cannot name a property such as
// constructor: empty (a TypeScript array type), a number, a string literal
// that is not a sensitive word, or an arithmetic expression (see
// numericKey).
func (r *recognizer) safeKey(open int) bool {
	key, after := r.at(open+1), r.at(open+2)
	switch {
	case isPunct(key, "]"):
		return true
	case isPunct(after, "]") && key.kind == tokNumber:
		return true
	case isPunct(after, "]") && isStatic(key):
		return !sensitiveWords[strings.TrimSpace(key.value)]
	}
	return r.numericKey(open)
}

// arithmetic operators always produce a number (or a BigInt): unlike "+",
// none of them can build a string.
var arithmetic = setOf(`- * / % ** ++ --`)

// numericKey reports a key built only from identifiers, numbers, property
// reads, grouping parentheses and arithmetic operators other than "+", with
// at least one such operator: text[at - 1], days[mo - 1], s[i++]. Its value
// is a number, which cannot name a sensitive property. A call, "+", a
// string, a comparison, a conditional or a comma anywhere in it fails.
func (r *recognizer) numericKey(open int) bool {
	operator := false
	depth := 0
	for j := open + 1; j < len(r.toks); j++ {
		t := r.toks[j]
		switch {
		case isPunct(t, "]") && depth == 0:
			return operator
		case t.kind == tokIdent || t.kind == tokNumber:
		case isPunct(t, "."):
		case isPunct(t, "("):
			// grouping only: a "(" after an operand is a call
			if p := r.at(j - 1); p.kind == tokIdent || p.kind == tokNumber || isPunct(p, ")") || isPunct(p, "]") {
				return false
			}
			depth++
		case isPunct(t, ")"):
			if depth == 0 {
				return false
			}
			depth--
		case t.kind == tokPunct && arithmetic[t.text]:
			operator = true
		default:
			return false
		}
	}
	return false
}

// matching returns the index of the ")" matching the "(" at open, or -1.
func (r *recognizer) matching(open int) int {
	depth := 0
	for j := open; j < len(r.toks); j++ {
		switch {
		case isPunct(r.toks[j], "("):
			depth++
		case isPunct(r.toks[j], ")"):
			depth--
			if depth == 0 {
				return j
			}
		}
	}
	return -1
}

// staticArgument returns the index of the only argument of the call whose
// "(" is at open when it is a string literal, or -1.
func (r *recognizer) staticArgument(open int) int {
	if isStatic(r.at(open+1)) && isPunct(r.at(open+2), ")") {
		return open + 1
	}
	return -1
}

func (r *recognizer) importAt(i int) int {
	t := r.toks[i]
	next := r.at(i + 1)
	switch {
	case isPunct(next, "."):
		if !isIdent(r.at(i+2), "meta") || !isPunct(r.at(i+3), ".") {
			return i
		}
		property := r.at(i + 4)
		switch {
		case property.kind == tokIdent && importMetaProperties[property.text]:
			r.markSafe(i)
		case isIdent(property, "resolve") && isPunct(r.at(i+5), "("):
			if argument := r.staticArgument(i + 5); argument > 0 {
				r.markSafe(i)
				r.add(t.line, "import.meta.resolve", argument)
			} else {
				r.markSafe(i)
				r.unverified(t.line, CodeDynamicImport, "import.meta.resolve(expression)")
			}
		}
		return i
	case isPunct(next, "("):
		r.markSafe(i)
		if argument := r.at(i + 2); isStatic(argument) && (isPunct(r.at(i+3), ")") || isPunct(r.at(i+3), ",")) {
			r.add(t.line, "import()", i+2)
		} else {
			r.unverified(t.line, CodeDynamicImport, "import(expression)")
		}
		return i
	case isStatic(next):
		r.markSafe(i)
		r.add(t.line, "import", i+1)
		return i + 1
	}
	j := i + 1
	hasDefault := false
	// Modifiers: import type …, import typeof …, import defer * as …,
	// import source x from …. "type" alone may also be a default binding.
	if tok := r.at(j); tok.kind == tokIdent && (tok.text == "type" || tok.text == "typeof" || tok.text == "defer" || tok.text == "source") {
		after := r.at(j + 1)
		if (after.kind == tokIdent && after.text != "from") || isPunct(after, "{") || isPunct(after, "*") {
			j++
		}
	}
	if tok := r.at(j); tok.kind == tokIdent && tok.text != "from" || tok.kind == tokIdent && isIdent(r.at(j+1), "from") {
		j++
		hasDefault = true
		if isPunct(r.at(j), "=") { // TypeScript import x = require("y") or import x = A.B
			if isIdent(r.at(j+1), "require") && isPunct(r.at(j+2), "(") {
				if argument := r.staticArgument(j + 2); argument > 0 {
					r.markSafe(i, j-1, j+1)
					r.add(t.line, "import-equals", argument)
				} else {
					r.markSafe(i, j-1, j+1)
					r.unverified(t.line, CodeDynamicImport, "import = require(expression)")
				}
				return j + 2
			}
			return j // import A = B.C: the import token is not safe
		}
		if isPunct(r.at(j), ",") {
			j++
			hasDefault = false // a named or namespace clause follows
		}
	}
	switch tok := r.at(j); {
	case isPunct(tok, "{"):
		for j++; j < len(r.toks) && !isPunct(r.at(j), "}"); j++ {
			if k := r.at(j); k.kind != tokIdent && k.kind != tokString && !isPunct(k, ",") {
				r.unverified(t.line, CodeUnsupportedForm, "unrecognized import clause")
				return j
			}
		}
		j++
	case isPunct(tok, "*"):
		if !isIdent(r.at(j+1), "as") || r.at(j+2).kind != tokIdent {
			r.unverified(t.line, CodeUnsupportedForm, "unrecognized import clause")
			return j
		}
		j += 3
	case hasDefault:
		// default binding only: import x from "y"
	default:
		r.unverified(t.line, CodeUnsupportedForm, "unrecognized import declaration")
		return j
	}
	if isIdent(r.at(j), "from") && isStatic(r.at(j+1)) {
		// The declaration owns its bindings: import process from
		// "node:process" declares a name; its later uses are checked.
		for k := i; k <= j; k++ {
			r.safe[k] = true
		}
		r.add(t.line, "import", j+1)
		return j + 1
	}
	r.unverified(t.line, CodeUnsupportedForm, "unrecognized import declaration")
	return j
}

func (r *recognizer) exportAt(i int) int {
	t := r.toks[i]
	j := i + 1
	if isIdent(r.at(j), "type") && (isPunct(r.at(j+1), "{") || isPunct(r.at(j+1), "*")) {
		j++
	}
	switch tok := r.at(j); {
	case isPunct(tok, "*"):
		j++
		if isIdent(r.at(j), "as") {
			j += 2
		}
	case isPunct(tok, "{"):
		depth := 0
		for ; j < len(r.toks); j++ {
			if isPunct(r.at(j), "{") {
				depth++
			} else if isPunct(r.at(j), "}") {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		j++
		if !isIdent(r.at(j), "from") {
			return i // a local export list
		}
	default:
		return i // export declarations, export default, export =, export import
	}
	if isIdent(r.at(j), "from") && isStatic(r.at(j+1)) {
		r.add(t.line, "export-from", j+1)
		return j + 1
	}
	r.unverified(t.line, CodeUnsupportedForm, "unrecognized export declaration")
	return j
}

// requireAt recognizes require("s") and require.resolve("s"); a call of
// require with anything else is a dynamic import. Every other use of the
// name is left to the allowlist check.
func (r *recognizer) requireAt(i int) {
	t := r.toks[i]
	switch next := r.at(i + 1); {
	case isPunct(next, "("):
		r.markSafe(i)
		if argument := r.staticArgument(i + 1); argument > 0 {
			r.add(t.line, "require", argument)
		} else {
			r.unverified(t.line, CodeDynamicImport, "require(expression)")
		}
	case isPunct(next, ".") && isIdent(r.at(i+2), "resolve") && isPunct(r.at(i+3), "("):
		r.markSafe(i)
		if argument := r.staticArgument(i + 3); argument > 0 {
			r.add(t.line, "require.resolve", argument)
		} else {
			r.unverified(t.line, CodeDynamicImport, "require.resolve(expression)")
		}
	}
}

// specifierModule returns a bare specifier's built-in module name, without
// a node: prefix, or "".
func builtinName(specifier string) (string, bool) {
	name, prefixed := strings.CutPrefix(specifier, "node:")
	if prefixed {
		return name, true
	}
	base, _, _ := strings.Cut(name, "/")
	if builtinModules[base] {
		return name, true
	}
	return "", false
}

// builtinModules is Node.js 22/24 module.builtinModules without the node:
// prefix (subpaths such as fs/promises share their base name).
var builtinModules = map[string]bool{}

func init() {
	for _, name := range strings.Fields(`_http_agent _http_client _http_common _http_incoming _http_outgoing _http_server
		_stream_duplex _stream_passthrough _stream_readable _stream_transform _stream_wrap _stream_writable
		_tls_common _tls_wrap assert async_hooks buffer child_process cluster console constants crypto dgram
		diagnostics_channel dns domain events fs http http2 https inspector module net os path perf_hooks process
		punycode querystring readline repl stream string_decoder sys timers tls trace_events tty url util v8 vm
		wasi worker_threads zlib`) {
		builtinModules[name] = true
	}
}

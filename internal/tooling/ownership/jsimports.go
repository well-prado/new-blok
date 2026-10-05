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

// loaderNames are identifiers that reach Node's module loader, a native
// binding, or evaluate source without a static specifier. Any appearance,
// as a name or as a property, makes the file unverified (ADR 0025).
var loaderNames = map[string]bool{
	"createRequire": true, "getBuiltinModule": true, "mainModule": true, "_load": true,
	"dlopen": true, "importScripts": true, "ShadowRealm": true,
	"_linkedBinding": true, "eval": true, "Function": true, "Worker": true,
}

// memberLoaderNames are loaders reached as a property (process.binding)
// whose bare name is a common variable; called bare they are unverified too.
var memberLoaderNames = map[string]bool{"binding": true}

// globalObjects are names of the global object; a computed property of one
// (globalThis["ev" + "al"]) can reach any loader, so it is unverified.
var globalObjects = map[string]bool{"globalThis": true, "global": true, "window": true, "self": true, "this": true}

// loaderModules are built-in modules whose purpose is loading, evaluating
// or running code by computed name or path.
var loaderModules = map[string]bool{"vm": true, "module": true, "worker_threads": true, "child_process": true, "cluster": true}

// scanJS lexes src and returns every import form it contains.
func scanJS(src []byte) (jsScan, error) {
	toks, refs, ambiguous, err := lex(src)
	if err != nil {
		return jsScan{}, err
	}
	r := &recognizer{toks: toks}
	for _, line := range ambiguous {
		r.unverified(line, CodeAmbiguousSyntax, "regular expression or division")
	}
	for _, ref := range refs {
		r.out.imports = append(r.out.imports, jsImport{line: ref.line, form: "reference", specifier: ref.value, types: ref.types})
	}
	r.scan()
	return r.out, nil
}

type recognizer struct {
	toks []jsToken
	out  jsScan
	// braces mirrors the lexer's brace kinds, so a method named import in a
	// class body or object literal is not read as a dynamic import.
	braces []braceKind
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

// member reports a jsToken used as a property name: after "." or "?.".
func (r *recognizer) member(i int) bool {
	p := r.at(i - 1)
	return isPunct(p, ".") || isPunct(p, "?.")
}

func (r *recognizer) unverified(line int, code, form string) {
	r.out.unverified = append(r.out.unverified, jsUnverified{line: line, code: code, form: form})
}

func (r *recognizer) add(line int, form, specifier string) {
	r.out.imports = append(r.out.imports, jsImport{line: line, form: form, specifier: specifier})
}

func (r *recognizer) scan() {
	for i := 0; i < len(r.toks); i++ {
		t := r.toks[i]
		switch {
		case isPunct(t, "{"):
			r.braces = append(r.braces, t.brace)
		case t.kind == tokTemplateHead:
			r.braces = append(r.braces, braceTemplate)
		case isPunct(t, "}"), t.kind == tokTemplateTail:
			if len(r.braces) > 0 {
				r.braces = r.braces[:len(r.braces)-1]
			}
		case t.kind != tokIdent:
		case r.member(i):
			switch {
			case loaderNames[t.text] || memberLoaderNames[t.text]:
				r.unverified(t.line, CodeUnsupportedForm, "."+t.text)
			case t.text == "require":
				// module.require, require.main.require, x?.require,
				// prototype.require.call: every member route to a
				// require function is unverified.
				r.unverified(t.line, CodeUnsupportedForm, ".require")
			case t.text == "constructor" && !(isPunct(r.at(i+1), ".") && isIdent(r.at(i+2), "name")):
				// x.constructor reaches the Function and AsyncFunction
				// constructors (f.constructor("…")); only .constructor.name
				// is read without that risk
				r.unverified(t.line, CodeUnsupportedForm, ".constructor")
			}
		case r.propertyKey(i):
			// { require: … } or { import: … }
		case t.text == "import":
			i = r.importAt(i)
		case t.text == "export":
			i = r.exportAt(i)
		case t.text == "require":
			r.requireAt(i)
		case t.text == "module" && (isPunct(r.at(i+1), ".") || isPunct(r.at(i+1), "?.")):
			if !isIdent(r.at(i+2), "exports") && !isIdent(r.at(i+2), "require") { // .require is reported as a member
				r.unverified(t.line, CodeUnsupportedForm, "module."+r.at(i+2).text)
			}
		case t.text == "module" && isPunct(r.at(i+1), "["):
			r.unverified(t.line, CodeUnsupportedForm, "module[…]")
		case globalObjects[t.text] && (isPunct(r.at(i+1), "[") || isPunct(r.at(i+1), "?.") && isPunct(r.at(i+2), "[")):
			r.unverified(t.line, CodeUnsupportedForm, t.text+"[…]")
		case (t.text == "globalThis" || t.text == "global") && !isPunct(r.at(i+1), ".") && !isPunct(r.at(i+1), "?."):
			// the global object as a value (Reflect.get(globalThis, k))
			// reaches eval by a computed name
			r.unverified(t.line, CodeUnsupportedForm, t.text+" as a value")
		case memberLoaderNames[t.text] && isPunct(r.at(i+1), "("):
			r.unverified(t.line, CodeUnsupportedForm, t.text+"(…)")
		case t.text == "arguments":
			// at CommonJS module scope (or in an arrow function there) this
			// is the wrapper's (exports, require, module, …)
			r.unverified(t.line, CodeUnsupportedForm, "arguments")
		case t.text == "define" && isPunct(r.at(i+1), "("):
			r.unverified(t.line, CodeUnsupportedForm, "AMD define")
		case loaderNames[t.text]:
			r.unverified(t.line, CodeUnsupportedForm, t.text)
		}
	}
}

// propertyKey reports `{ name: …` or `, name: …` in an object literal or
// class body: a key, not a reference.
func (r *recognizer) propertyKey(i int) bool {
	prev := r.at(i - 1)
	return r.inObjectOrClass() && isPunct(r.at(i+1), ":") && (isPunct(prev, "{") || isPunct(prev, ","))
}

func (r *recognizer) inObjectOrClass() bool {
	if len(r.braces) == 0 {
		return false
	}
	kind := r.braces[len(r.braces)-1]
	return kind == braceExpr || kind == braceClass
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

// callArgument reads `( "literal" )` or `( "literal", … )` at open. It
// returns the literal and true, or false for a computed argument.
func (r *recognizer) callArgument(open int) (string, bool) {
	argument := r.at(open + 1)
	after := r.at(open + 2)
	if isStatic(argument) && (isPunct(after, ")") || isPunct(after, ",")) {
		return argument.value, true
	}
	return "", false
}

func (r *recognizer) importAt(i int) int {
	t := r.toks[i]
	next := r.at(i + 1)
	switch {
	case isPunct(next, "."):
		// import.meta; import.meta.resolve("x") names a module.
		if isIdent(r.at(i+2), "meta") && isPunct(r.at(i+3), ".") && isIdent(r.at(i+4), "resolve") && isPunct(r.at(i+5), "(") {
			if specifier, ok := r.callArgument(i + 5); ok {
				r.add(t.line, "import.meta.resolve", specifier)
			} else {
				r.unverified(t.line, CodeDynamicImport, "import.meta.resolve(expression)")
			}
		}
		return i
	case isPunct(next, "("):
		if specifier, ok := r.callArgument(i + 1); ok {
			r.add(t.line, "import()", specifier)
		} else {
			r.unverified(t.line, CodeDynamicImport, "import(expression)")
		}
		return i
	case isStatic(next):
		r.add(t.line, "import", next.value)
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
				if specifier, ok := r.callArgument(j + 2); ok {
					r.add(t.line, "import-equals", specifier)
				} else {
					r.unverified(t.line, CodeDynamicImport, "import = require(expression)")
				}
				return j + 2
			}
			return j
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
		r.add(t.line, "import", r.at(j+1).value)
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
		r.add(t.line, "export-from", r.at(j+1).value)
		return j + 1
	}
	r.unverified(t.line, CodeUnsupportedForm, "unrecognized export declaration")
	return j
}

func (r *recognizer) requireAt(i int) {
	t := r.toks[i]
	prev, next := r.at(i-1), r.at(i+1)
	switch {
	case isPunct(next, "("):
		if specifier, ok := r.callArgument(i + 1); ok {
			r.add(t.line, "require", specifier)
		} else {
			r.unverified(t.line, CodeDynamicImport, "require(expression)")
		}
	case isPunct(next, ".") && isIdent(r.at(i+2), "resolve") && isPunct(r.at(i+3), "("):
		if specifier, ok := r.callArgument(i + 3); ok {
			r.add(t.line, "require.resolve", specifier)
		} else {
			r.unverified(t.line, CodeDynamicImport, "require.resolve(expression)")
		}
	case isPunct(next, ".") || isPunct(next, "?.") || isPunct(next, "["):
		// require.main.require, require.cache, require?.(…), require[…]
		r.unverified(t.line, CodeUnsupportedForm, "require"+next.text+r.at(i+2).text)
	case isIdent(prev, "typeof"), isIdent(prev, "function"), isIdent(prev, "const"), isIdent(prev, "let"), isIdent(prev, "var"), isPunct(next, ":") && isIdent(r.at(i-2), "declare"):
		// typeof require, or a declaration of the name
	default:
		// require passed or stored as a value can load anything
		r.unverified(t.line, CodeUnsupportedForm, "require as a value")
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

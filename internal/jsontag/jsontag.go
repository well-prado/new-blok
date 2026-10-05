// Package jsontag reads the encoding/json struct tag options that decide
// which object key a field is written under and whether it is written, for
// the engine's field references and the accessor generator (#241).
package jsontag

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Options are the encoding effects of one json tag.
type Options struct {
	// Name is the tag's name part; empty means the Go field name applies.
	Name      string
	OmitEmpty bool
	OmitZero  bool
	String    bool
	// Embed and Format are encoding/json/v2 options Go 1.27's encoding/json
	// acts on: an unnamed embed field is inlined, and format fails encoding.
	Embed  bool
	Format bool
}

// Parse reads a json tag value (the text inside json:"…") when it is in
// plain form: a valid UTF-8 name part without quotes, backslashes or
// backticks (which encoding/json then uses verbatim as the key), and
// options that are each empty, a whole identifier, case:<identifier> or
// format:<value>. Go 1.27's encoding/json applies the leading identifier of
// a malformed option (",omitempty " still omits) and parses quoted forms its
// own way, so a tag outside plain form reports ok=false: callers must not
// predict its effect and should ask encoding/json instead.
func Parse(tag string) (options Options, ok bool) {
	name, rest, hasOptions := strings.Cut(tag, ",")
	if strings.ContainsAny(name, "'\"\\`") || !utf8.ValidString(name) {
		return Options{}, false
	}
	options.Name = name
	if !hasOptions {
		return options, true
	}
	for _, option := range strings.Split(rest, ",") {
		switch {
		case option == "":
			// A trailing or doubled comma: encoding/json reports it as
			// malformed yet applies the other options as written.
		case option == "omitempty":
			options.OmitEmpty = true
		case option == "omitzero":
			options.OmitZero = true
		case option == "string":
			options.String = true
		case option == "embed":
			options.Embed = true
		case strings.HasPrefix(option, "format:") && len(option) > len("format:"):
			options.Format = true
		case strings.HasPrefix(option, "case:") && identifier(option[len("case:"):]):
			// Decoding only.
		case identifier(option):
			// encoding/json ignores options it does not know, including
			// look-alikes such as omitEmpty.
		default:
			return Options{}, false
		}
	}
	return options, true
}

// identifier matches encoding/json/v2's option grammar: a letter or
// underscore, then letters, numbers or underscores (isLetterOrDigit).
func identifier(text string) bool {
	if text == "" {
		return false
	}
	for index, r := range text {
		if r == '_' || unicode.IsLetter(r) || index > 0 && unicode.IsNumber(r) {
			continue
		}
		return false
	}
	return true
}

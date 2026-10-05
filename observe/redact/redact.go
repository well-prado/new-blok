// Package redact is the framework's single redaction boundary for content
// that leaves a run: inspection projections, the development event stream,
// worker logs, telemetry log attributes, model-visible catalog listings and
// durable audit records (ADR 0021).
//
// It layers two things on the credential pattern shared through
// contract/observe: structured redaction (a sensitive key hides its whole
// value) and bounded decoding of encoded content (JSON inside a string,
// percent-encoding and base64), so a credential is found when it is wrapped
// once or twice. It is pattern matching, not a guarantee: content that is
// encrypted, compressed, hex-encoded, split across fields, embedded in prose
// without a recognisable marker, or larger than MaxDecodeBytes is not found.
// It imports only the standard library and contract/observe.
package redact

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/well-prado/new-blok/contract/observe"
)

const (
	// Marker replaces a redacted structured value.
	Marker = "[redacted]"
	// MessageMarker replaces a whole redacted free-form message. It is the
	// marker contract/observe.RedactLogMessage uses.
	MessageMarker = "[redacted: sensitive-looking log message]"
	// MaxDecodeBytes bounds the strings decoding is attempted on. A longer
	// string is matched as written only.
	MaxDecodeBytes = 16 << 10
	// MaxDecodeDepth bounds nested decodings (base64 of JSON of a
	// percent-encoded value is three).
	MaxDecodeDepth = 3
	// maxValueDepth bounds structural recursion; deeper values are replaced.
	maxValueDepth = 64
	// minBase64Bytes keeps short words and identifiers out of base64 decoding.
	minBase64Bytes = 12
)

var sensitiveKeyMarkers = []string{"password", "passwd", "pwd", "passphrase", "secret", "token", "authorization", "credential", "apikey", "privatekey", "cookie", "sessionid"}

// tokenCountSuffixes name counts and limits of model tokens, not tokens:
// "total_tokens", "maxTokens", "token_count", "tokenLimit".
var tokenCountSuffixes = []string{"tokens", "tokencount", "tokenlimit", "tokenbudget", "tokenusage"}

// Key reports whether a structured key names sensitive content. Case and the
// separators '_', '-' and '.' are ignored, so "api_key", "API-Key" and
// "client.secret" all match. A token count or limit ("usage.total_tokens",
// "max_tokens") is not a token and is not matched by "token" alone.
func Key(key string) bool {
	key = strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(key))
	for _, marker := range sensitiveKeyMarkers {
		if !strings.Contains(key, marker) {
			continue
		}
		if marker == "token" && tokenCount(key) {
			continue
		}
		return true
	}
	return false
}

func tokenCount(key string) bool {
	for _, suffix := range tokenCountSuffixes {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

// mode selects the predicate: broad for redacting projected copies, strict
// for refusing content (a credential value, not a sensitive word).
type mode bool

const (
	broad  mode = false
	strict mode = true
)

func (m mode) text(text string) bool {
	if m == strict {
		return observe.CredentialText(text)
	}
	return observe.SensitiveText(text)
}

// Sensitive reports whether text is credential-shaped as written or after up
// to MaxDecodeDepth bounded decodings. It is broad: use it to redact.
func Sensitive(text string) bool { return sensitive(text, MaxDecodeDepth, broad) }

// Credential reports whether text holds an actual credential value, plainly
// or after the same bounded decodings: a credential shape, or a generated-
// looking value after a sensitive key ("api_key=k3y..."), never a sensitive
// word in prose ("password: the new password", "max_tokens: 256"). Use it to
// refuse content at a boundary such as catalog registration.
func Credential(text string) bool { return sensitive(text, MaxDecodeDepth, strict) }

func sensitive(text string, depth int, m mode) bool {
	if m.text(text) || basicCredential(text) {
		return true
	}
	if depth == 0 || len(text) > MaxDecodeBytes {
		return false
	}
	trimmed := strings.TrimSpace(text)
	if len(trimmed) >= 2 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"') {
		if value, err := decode([]byte(trimmed)); err == nil && containsSensitive(value, depth-1, 0, m) {
			return true
		}
	}
	if strings.Contains(text, "%") {
		if decoded, ok := percentDecode(text); ok && sensitive(decoded, depth-1, m) {
			return true
		}
	}
	for _, token := range base64Candidates(trimmed) {
		if decoded, ok := decodeBase64(token); ok && sensitive(decoded, depth-1, m) {
			return true
		}
	}
	return false
}

// percentDecode reverses percent-encoding (and '+' as a space, as in a query
// string). It is implemented here so the package links no net package.
func percentDecode(text string) (string, bool) {
	var out strings.Builder
	changed := false
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case c == '%' && i+2 < len(text) && isHex(text[i+1]) && isHex(text[i+2]):
			out.WriteByte(unhex(text[i+1])<<4 | unhex(text[i+2]))
			i += 2
			changed = true
		case c == '+':
			out.WriteByte(' ')
			changed = true
		default:
			out.WriteByte(c)
		}
	}
	return out.String(), changed
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func unhex(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// basicCredential finds "Basic <base64 user:password>" outside a header
// name, which the shared pattern cannot tell apart from ordinary prose.
func basicCredential(text string) bool {
	fields := strings.Fields(text)
	for i := 0; i+1 < len(fields); i++ {
		if !strings.EqualFold(fields[i], "basic") {
			continue
		}
		if decoded, ok := decodeBase64(fields[i+1]); ok && strings.Contains(decoded, ":") {
			return true
		}
	}
	return false
}

// base64Candidates returns the whole text when it is one token, and every
// sufficiently long whitespace- or separator-delimited token otherwise, so a
// base64 credential inside a sentence or a query value is still decoded.
func base64Candidates(text string) []string {
	if len(text) < minBase64Bytes {
		return nil
	}
	tokens := strings.FieldsFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || r == '&' || r == '?' || r == ',' || r == ';' || r == '"' || r == '\''
	})
	var out []string
	for _, token := range tokens {
		if len(token) >= minBase64Bytes {
			out = append(out, token)
		}
		// A key=value pair: also decode the value, padding included. Base64
		// has '=' only as trailing padding, so a first '=' followed by
		// anything other than padding separates a key from its value.
		if eq := strings.IndexByte(token, '='); eq > 0 && strings.Trim(token[eq:], "=") != "" {
			if value := token[eq+1:]; len(value) >= minBase64Bytes {
				out = append(out, value)
			}
		}
	}
	return out
}

func decodeBase64(token string) (string, bool) {
	if len(token) < minBase64Bytes || len(token) > MaxDecodeBytes {
		return "", false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '+' || c == '/' || c == '-' || c == '_' || c == '=') {
			return "", false
		}
	}
	// The alphabet and padding select the one encoding that can apply.
	encoding := base64.RawStdEncoding
	if strings.ContainsAny(token, "-_") {
		encoding = base64.RawURLEncoding
	}
	if strings.HasSuffix(token, "=") {
		encoding = encoding.WithPadding(base64.StdPadding)
	}
	decoded, err := encoding.DecodeString(token)
	if err != nil || !printable(decoded) {
		return "", false
	}
	return string(decoded), true
}

// printable accepts decoded bytes that are text: valid UTF-8 and almost
// entirely printable. Random identifiers and binary data decode to neither.
func printable(data []byte) bool {
	if len(data) == 0 || !utf8.Valid(data) {
		return false
	}
	total, ok := 0, 0
	for _, r := range string(data) {
		total++
		if unicode.IsPrint(r) || r == '\n' || r == '\r' || r == '\t' {
			ok++
		}
	}
	return ok*10 >= total*9
}

func containsSensitive(value any, depth, level int, m mode) bool {
	if level > maxValueDepth {
		return true
	}
	switch item := value.(type) {
	case map[string]any:
		for key, child := range item {
			if sensitive(key, 0, m) || containsSensitive(child, depth, level+1, m) {
				return true
			}
			if Key(key) {
				// Broad: a sensitive key hides any value. Strict: only a
				// value that is itself credential-looking.
				text, isText := child.(string)
				if m == broad || isText && observe.LooksSecret(text) {
					return true
				}
			}
		}
	case []any:
		for _, child := range item {
			if containsSensitive(child, depth, level+1, m) {
				return true
			}
		}
	case string:
		return sensitive(item, depth, m)
	}
	return false
}

// HasSensitiveValue reports whether any string value inside a decoded JSON
// value is credential-shaped (broad). Keys are names here, not secrets: a
// schema property called "password" is legitimate, a default of
// "Bearer ..." is not.
func HasSensitiveValue(value any) bool { return hasValue(value, 0, broad) }

// HasCredentialValue is HasSensitiveValue with the strict Credential
// predicate, for refusing content.
func HasCredentialValue(value any) bool { return hasValue(value, 0, strict) }

func hasValue(value any, level int, m mode) bool {
	if level > maxValueDepth {
		return true
	}
	switch item := value.(type) {
	case map[string]any:
		for _, child := range item {
			if hasValue(child, level+1, m) {
				return true
			}
		}
	case []any:
		for _, child := range item {
			if hasValue(child, level+1, m) {
				return true
			}
		}
	case string:
		return sensitive(item, MaxDecodeDepth, m)
	}
	return false
}

// Value returns a redacted copy of a decoded JSON value: the value of a
// sensitive key, and any credential-shaped string (including an encoded
// one), become Marker. A string that holds JSON is replaced whole when the
// JSON inside it is sensitive; it is not partially rewritten.
func Value(value any) any { return redactValue(value, 0) }

func redactValue(value any, level int) any {
	if level > maxValueDepth {
		return Marker
	}
	switch item := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			if Key(key) {
				out[key] = Marker
			} else {
				out[key] = redactValue(child, level+1)
			}
		}
		return out
	case []any:
		out := make([]any, len(item))
		for index, child := range item {
			out[index] = redactValue(child, level+1)
		}
		return out
	case string:
		if Sensitive(item) {
			return Marker
		}
		return item
	default:
		return value
	}
}

// JSON redacts encoded JSON. Numbers keep their exact text.
func JSON(raw []byte) ([]byte, error) {
	value, err := decode(raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Value(value))
}

// Message returns MessageMarker for a credential-shaped free-form message
// (including an encoded one) and the message unchanged otherwise.
func Message(message string) string {
	if Sensitive(message) {
		return MessageMarker
	}
	return message
}

// String returns Marker for a credential-shaped value and the value
// unchanged otherwise.
func String(value string) string {
	if Sensitive(value) {
		return Marker
	}
	return value
}

func decode(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

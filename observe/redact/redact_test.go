package redact_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/observe/redact"
)

type fixture struct {
	Name     string          `json:"name"`
	Kind     string          `json:"kind"`
	Boundary bool            `json:"boundary"`
	Input    json.RawMessage `json:"input"`
	Expected json.RawMessage `json:"expected"`
	Redacted int             `json:"redacted"`
}

func loadFixtures(t *testing.T) []fixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []fixture `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Cases) < 20 {
		t.Fatalf("fixture inventory shrank to %d cases", len(file.Cases))
	}
	return file.Cases
}

// TestRedactionFixtures runs every predeclared case, including the encoded
// ones and the boundary cases the redactor is documented not to find.
func TestRedactionFixtures(t *testing.T) {
	var positives, negatives, boundaries int
	for _, item := range loadFixtures(t) {
		t.Run(item.Name, func(t *testing.T) {
			switch item.Kind {
			case "value":
				got, err := redact.JSON(item.Input)
				if err != nil {
					t.Fatal(err)
				}
				var gotValue, wantValue any
				if err := json.Unmarshal(got, &gotValue); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(item.Expected, &wantValue); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotValue, wantValue) {
					t.Fatalf("redacted %s\nwant %s", got, item.Expected)
				}
				if count := strings.Count(string(got), redact.Marker); count != item.Redacted {
					t.Fatalf("redacted values=%d want %d (%s)", count, item.Redacted, got)
				}
			case "message":
				var input, want string
				if err := json.Unmarshal(item.Input, &input); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(item.Expected, &want); err != nil {
					t.Fatal(err)
				}
				if got := redact.Message(input); got != want {
					t.Fatalf("message=%q want %q", got, want)
				}
			default:
				t.Fatalf("unknown fixture kind %q", item.Kind)
			}
		})
		switch {
		case item.Boundary:
			boundaries++
		case item.Redacted > 0:
			positives++
		default:
			negatives++
		}
	}
	if positives < 15 || negatives < 2 || boundaries < 3 {
		t.Fatalf("fixture balance positives=%d negatives=%d boundaries=%d", positives, negatives, boundaries)
	}
}

// TestDecodingIsBounded pins the documented limits: a string above
// MaxDecodeBytes is matched only as written, so an encoded credential inside
// it is not found, while the same credential written plainly still is.
func TestDecodingIsBounded(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("password=SYNTHETIC-bound-0001"))
	if !redact.Sensitive(encoded) {
		t.Fatal("small encoded credential not found")
	}
	padded := encoded + " " + strings.Repeat("a", redact.MaxDecodeBytes)
	if redact.Sensitive(padded) {
		t.Fatal("decoding ran beyond MaxDecodeBytes; update ADR 0021 if this is intended")
	}
	if !redact.Sensitive("password=SYNTHETIC-bound-0002 " + strings.Repeat("a", redact.MaxDecodeBytes)) {
		t.Fatal("plain credential in a large string not found")
	}
	deep := any("SYNTHETIC")
	for range 80 {
		deep = []any{deep}
	}
	if got := redact.Value(deep); strings.Contains(mustJSON(t, got), "SYNTHETIC") {
		t.Fatal("value deeper than the structural bound was kept")
	}
}

func TestKeyNormalisesSeparatorsAndCase(t *testing.T) {
	for _, key := range []string{"password", "API_KEY", "api-key", "client.secret", "X-Auth-Token", "privateKey", "Cookie"} {
		if !redact.Key(key) {
			t.Fatalf("%q not sensitive", key)
		}
	}
	for _, key := range []string{"sku", "quantity", "totalCents", "id"} {
		if redact.Key(key) {
			t.Fatalf("%q flagged", key)
		}
	}
}

func TestHasSensitiveValueIgnoresKeyNames(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"password": map[string]any{"type": "string"}}}
	if redact.HasSensitiveValue(schema) {
		t.Fatal("a property named password is not a secret")
	}
	schema["properties"].(map[string]any)["password"].(map[string]any)["default"] = "Bearer SYNTHETIC-default-0001"
	if !redact.HasSensitiveValue(schema) {
		t.Fatal("credential-shaped default not found")
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestCredentialChecksValuesUnderSensitiveKeys: the strict predicate refuses
// a generated-looking value under a sensitive key even when only JSON
// decoding reveals the key (an escaped key the text pattern cannot see),
// and does not refuse prose under the same key; the broad predicate
// redacts both.
func TestCredentialChecksValuesUnderSensitiveKeys(t *testing.T) {
	credential := `{"password":"SYNTHETIC0cred9xyz"}`
	prose := `{"password":"the new one"}`
	if !redact.Credential(credential) {
		t.Fatal("credential value under an escaped sensitive key not refused")
	}
	if redact.Credential(prose) {
		t.Fatal("prose under a sensitive key refused")
	}
	if !redact.Sensitive(credential) || !redact.Sensitive(prose) {
		t.Fatal("broad predicate must redact both")
	}
	if !redact.HasCredentialValue(map[string]any{"default": credential}) || redact.HasCredentialValue(map[string]any{"default": prose}) {
		t.Fatal("HasCredentialValue disagrees with Credential")
	}
}

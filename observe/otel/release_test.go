package otel_test

import (
	"os"
	"strings"
	"testing"
)

const rootModule = "github.com/well-prado/new-blok"

// TestModuleReleaseReadiness keeps the development wiring honest. While the
// root module is untagged, observe/otel requires it at the placeholder
// v0.0.0 and replaces it with ../.. — that only works inside this checkout;
// an external `go get` fails with "unknown revision v0.0.0". The two must
// therefore appear together, and a release (BLOK_RELEASE=1, step 3 of the
// ADR 0020 release checklist) must have neither: it requires a tagged root.
func TestModuleReleaseReadiness(t *testing.T) {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	mod := string(raw)
	replaced := strings.Contains(mod, "replace "+rootModule+" => ../..")
	placeholder := strings.Contains(mod, rootModule+" v0.0.0\n") || strings.Contains(mod, rootModule+" v0.0.0 ")
	if replaced != placeholder {
		t.Fatalf("go.mod has replace=%v but placeholder require=%v; they must change together", replaced, placeholder)
	}
	if os.Getenv("BLOK_RELEASE") == "1" && (replaced || placeholder) {
		t.Fatalf("release build: observe/otel still requires %s v0.0.0 through replace ../..; tag the root module, require that tag and drop the replace", rootModule)
	}
	if replaced {
		t.Logf("development wiring: %s v0.0.0 => ../.. (not consumable outside this repository)", rootModule)
	}
}
